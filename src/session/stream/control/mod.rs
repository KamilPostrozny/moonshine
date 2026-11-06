use std::net::SocketAddr;
use std::time::Duration;

use async_shutdown::ShutdownManager;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_enet::{Event, Host, HostConfig, Packet, PacketMode, PeerState};

use self::input::gamepad::GamepadConfig;
use self::{feedback::FeedbackCommand, input::InputHandler};
use crate::crypto::{decrypt, encrypt};
use crate::latency_protocol;
use crate::session::SessionContext;
use crate::session::SessionKeysReceiver;
use crate::session::compositor::{
	frame::{HdrMetadata, HdrModeState},
	input::CompositorInput,
};
use crate::session::manager::SessionShutdownReason;
use crate::session::stream::StartSignal;
use crate::session::stream::video::{FrameSent, RTP_CLOCK_RATE, VideoStreamHandle};

mod feedback;
pub(crate) mod input;

/// Configuration for the control stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ControlStreamConfig {
	/// Port to use for streaming control data.
	pub port: u16,

	/// Configuration for gamepad input remapping (e.g. hold-to-Home).
	pub gamepad: GamepadConfig,
}

impl Default for ControlStreamConfig {
	fn default() -> Self {
		Self {
			port: 47999,
			gamepad: GamepadConfig::default(),
		}
	}
}

const ENCRYPTION_TAG_LENGTH: usize = 16;
// Sequence number + tag + control message id.
const MINIMUM_ENCRYPTED_LENGTH: usize = 4 + ENCRYPTION_TAG_LENGTH + 4;

#[repr(u16)]
enum ControlMessageType {
	Encrypted = 0x0001,
	TerminationExtended = 0x0109,
	RumbleData = 0x010b,
	HdrMode = 0x010e,
	Ping = 0x0200,
	LossStats = 0x0201,
	FrameStats = 0x0204,
	InputData = 0x0206,
	RequestIdrFrame = 0x0302,
	InvalidateReferenceFrames = 0x0301,
	StartB = 0x0307,
	RumbleTriggers = 0x5500,
	SetMotionEvent = 0x5501,
	SetRgbLed = 0x5502,
	SetTriggerEffect = 0x5503,
	LatencyMessage = latency_protocol::MESSAGE_TYPE,
}

impl TryFrom<u16> for ControlMessageType {
	type Error = ();

	fn try_from(v: u16) -> Result<Self, Self::Error> {
		match v {
			x if x == Self::Encrypted as u16 => Ok(Self::Encrypted),
			x if x == Self::TerminationExtended as u16 => Ok(Self::TerminationExtended),
			x if x == Self::RumbleData as u16 => Ok(Self::RumbleData),
			x if x == Self::HdrMode as u16 => Ok(Self::HdrMode),
			x if x == Self::Ping as u16 => Ok(Self::Ping),
			x if x == Self::LossStats as u16 => Ok(Self::LossStats),
			x if x == Self::FrameStats as u16 => Ok(Self::FrameStats),
			x if x == Self::InputData as u16 => Ok(Self::InputData),
			x if x == Self::RequestIdrFrame as u16 => Ok(Self::RequestIdrFrame),
			x if x == Self::InvalidateReferenceFrames as u16 => Ok(Self::InvalidateReferenceFrames),
			x if x == Self::StartB as u16 => Ok(Self::StartB),
			x if x == Self::RumbleTriggers as u16 => Ok(Self::RumbleTriggers),
			x if x == Self::SetMotionEvent as u16 => Ok(Self::SetMotionEvent),
			x if x == Self::SetRgbLed as u16 => Ok(Self::SetRgbLed),
			x if x == Self::SetTriggerEffect as u16 => Ok(Self::SetTriggerEffect),
			x if x == Self::LatencyMessage as u16 => Ok(Self::LatencyMessage),
			_ => Err(()),
		}
	}
}

#[derive(Debug)]
enum ControlMessage<'a> {
	Encrypted(EncryptedControlMessage),
	TerminationExtended,
	RumbleData,
	HdrMode,
	Ping,
	LossStats,
	FrameStats,
	InputData(&'a [u8]),
	RequestIdrFrame,
	InvalidateReferenceFrames { first: u32, last: u32 },
	StartB,
	RumbleTriggers,
	SetMotionEvent,
	SetRgbLed,
	SetTriggerEffect,
	LatencyMessage(&'a [u8]), // Opaque latency protocol payload (fork extension, see `latency_protocol`).
}

impl<'a> ControlMessage<'a> {
	fn from_bytes(buffer: &'a [u8]) -> Result<Self, ()> {
		if buffer.len() < 4 {
			tracing::warn!(
				"Expected control message to have at least 4 bytes, got {}",
				buffer.len()
			);
			return Err(());
		}

		let length = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
		if length as usize != buffer.len() - 4 {
			tracing::warn!(
				"Received incorrect packet length: expecting {length} bytes, but buffer says it should be {} bytes.",
				buffer.len() - 4
			);
			return Err(());
		}

		match u16::from_le_bytes(buffer[..2].try_into().unwrap()).try_into()? {
			ControlMessageType::Encrypted => {
				if buffer.len() < MINIMUM_ENCRYPTED_LENGTH {
					tracing::warn!(
						"Expected encrypted control message of at least {MINIMUM_ENCRYPTED_LENGTH} bytes, got buffer of {} bytes.",
						buffer.len()
					);
					return Err(());
				}

				let length = u16::from_le_bytes(buffer[2..4].try_into().unwrap());
				if (length as usize) < MINIMUM_ENCRYPTED_LENGTH {
					tracing::warn!(
						"Expected encrypted control message of at least {MINIMUM_ENCRYPTED_LENGTH} bytes, got reported length of {length} bytes."
					);
					return Err(());
				}

				let sequence_number = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
				Ok(Self::Encrypted(EncryptedControlMessage {
					length,
					sequence_number,
					tag: buffer[8..8 + ENCRYPTION_TAG_LENGTH]
						.try_into()
						.map_err(|e| tracing::warn!("Failed to get tag from encrypted control message: {e}"))?,
					payload: buffer[8 + ENCRYPTION_TAG_LENGTH..].to_vec(),
				}))
			},
			ControlMessageType::Ping => Ok(Self::Ping),
			ControlMessageType::TerminationExtended => Ok(Self::TerminationExtended),
			ControlMessageType::RumbleData => Ok(Self::RumbleData),
			ControlMessageType::LossStats => Ok(Self::LossStats),
			ControlMessageType::FrameStats => Ok(Self::FrameStats),
			ControlMessageType::InputData => {
				// Length of the input event, excluding the length itself.
				let length = u32::from_be_bytes(buffer[4..8].try_into().unwrap());
				if length as usize != buffer.len() - 8 {
					tracing::warn!(
						"Failed to interpret input event message: expected {length} bytes, but buffer has {} bytes left.",
						buffer.len() - 8
					);
					return Err(());
				}

				Ok(Self::InputData(&buffer[8..]))
			},
			ControlMessageType::InvalidateReferenceFrames => {
				// Body (after the 4-byte type/length header), little-endian:
				//   firstFrameIndex(4), reserved1(4), lastFrameIndex(4), reserved2[3](12)
				// The client asks the host to stop referencing frames in the
				// range [first, last]. If the body is malformed, invalidate from
				// frame 0 so the encoder safely falls back to a full IDR.
				let (first, last) = if buffer.len() >= 16 {
					(
						u32::from_le_bytes(buffer[4..8].try_into().unwrap()),
						u32::from_le_bytes(buffer[12..16].try_into().unwrap()),
					)
				} else {
					(0, u32::MAX)
				};
				Ok(Self::InvalidateReferenceFrames { first, last })
			},
			ControlMessageType::RequestIdrFrame => Ok(Self::RequestIdrFrame),
			ControlMessageType::StartB => Ok(Self::StartB),
			ControlMessageType::HdrMode => Ok(Self::HdrMode),
			ControlMessageType::RumbleTriggers => Ok(Self::RumbleTriggers),
			ControlMessageType::SetMotionEvent => Ok(Self::SetMotionEvent),
			ControlMessageType::SetRgbLed => Ok(Self::SetRgbLed),
			ControlMessageType::SetTriggerEffect => Ok(Self::SetTriggerEffect),
			ControlMessageType::LatencyMessage => Ok(Self::LatencyMessage(&buffer[4..])),
		}
	}
}

#[derive(Debug)]
struct EncryptedControlMessage {
	length: u16,
	sequence_number: u32,
	tag: [u8; 16],
	payload: Vec<u8>,
}

impl EncryptedControlMessage {
	fn as_bytes(&self) -> Vec<u8> {
		let mut buffer = Vec::with_capacity(self.length as usize);

		buffer.extend((ControlMessageType::Encrypted as u16).to_le_bytes());
		buffer.extend(self.length.to_le_bytes());
		buffer.extend(self.sequence_number.to_le_bytes());
		buffer.extend(self.tag);
		buffer.extend(&self.payload);

		buffer
	}
}

fn encode_control(key: &[u8], sequence_number: u32, payload: &[u8]) -> Result<Vec<u8>, ()> {
	let mut initialization_vector = [0u8; 12];
	initialization_vector[0..4].copy_from_slice(&sequence_number.to_le_bytes());
	initialization_vector[10] = b'H';
	initialization_vector[11] = b'C';

	if key.len() != 16 {
		tracing::warn!("Key length has {} bytes, but expected {} bytes.", key.len(), 16);
		return Err(());
	}

	let mut tag = [0u8; 16];
	let payload = encrypt(payload, key, &initialization_vector, &mut tag)
		.map_err(|e| tracing::warn!("Failed to encrypt control data: {e}"))?;

	if payload.is_empty() {
		tracing::warn!("Failed to encrypt control data.");
		return Err(());
	}

	let message = EncryptedControlMessage {
		length: std::mem::size_of::<u32>() as u16 // Sequence number.
			 + ENCRYPTION_TAG_LENGTH as u16   // Tag.
			 + payload.len() as u16, // Payload.
		sequence_number,
		tag,
		payload,
	};

	Ok(message.as_bytes())
}

/// Decrypt the payload of an encrypted control message from the client.
fn decrypt_control(message: &EncryptedControlMessage, key: &[u8]) -> Result<Vec<u8>, aes_gcm::Error> {
	let mut initialization_vector = [0u8; 12];
	initialization_vector[0..4].copy_from_slice(&message.sequence_number.to_le_bytes());
	initialization_vector[10] = b'C';
	initialization_vector[11] = b'C';

	decrypt(&message.payload, key, &initialization_vector, &message.tag)
}

/// Context passed to the control stream from Session.
#[derive(Clone)]
pub(crate) struct ControlStreamContext {
	pub keys_rx: SessionKeysReceiver,
	pub hdr: bool,
	/// Whether the client's ANNOUNCE negotiated the latency protocol.
	pub latency_protocol: bool,
}

impl ControlStreamContext {
	/// Create a `ControlStreamContext` from a session context and the *effective*
	/// HDR flag reported by the compositor after launch.
	///
	/// `hdr_effective` may differ from `ctx.hdr` when the client requested HDR but
	/// the GPU does not support an HDR-capable DMA-BUF format (compositor falls back
	/// to SDR and reports `hdr = false` in `CompositorReady`).
	pub fn new(ctx: &SessionContext, hdr_effective: bool, latency_protocol: bool) -> Self {
		Self {
			keys_rx: ctx.keys.clone_rx().expect("session keys not initialized"),
			hdr: hdr_effective,
			latency_protocol,
		}
	}
}

pub(crate) struct ControlStream {
	stop: ShutdownManager<SessionShutdownReason>,
	input_handler: InputHandler,
	frame_clock: crate::session::compositor::frame_clock::CompositorFrameClock,
	host: Host,
}

impl ControlStream {
	pub fn new(
		config: ControlStreamConfig,
		address: String,
		compositor_input: CompositorInput,
		frame_clock: crate::session::compositor::frame_clock::CompositorFrameClock,
		stop_session_manager: ShutdownManager<SessionShutdownReason>,
	) -> Result<Self, ()> {
		let input_handler = InputHandler::new(compositor_input, stop_session_manager.clone(), config.gamepad.clone())?;

		let socket_address = SocketAddr::new(
			address
				.parse()
				.map_err(|e| tracing::warn!("Failed to parse address ({}): {e}", address))?,
			config.port,
		);

		let host_config = HostConfig {
			address: Some(socket_address),
			peer_count: 128,
			channel_limit: 0x30,
			..Default::default()
		};

		let host = Host::new(host_config).map_err(|e| tracing::error!("Failed to create enet host: {e}"))?;

		tracing::debug!("Listening for control messages on {:?}", socket_address);

		Ok(Self {
			stop: stop_session_manager,
			input_handler,
			frame_clock,
			host,
		})
	}

	pub fn start(
		self,
		stream_timeout: u64,
		context: ControlStreamContext,
		video_handle: VideoStreamHandle,
		audio_trigger: StartSignal,
		hdr_metadata_rx: watch::Receiver<HdrModeState>,
	) {
		let Self {
			stop: stop_session_manager,
			input_handler,
			frame_clock,
			host,
		} = self;

		tokio::spawn(async move {
			run_control_loop(
				stream_timeout,
				host,
				video_handle,
				audio_trigger,
				context,
				input_handler,
				frame_clock,
				stop_session_manager,
				hdr_metadata_rx,
			)
			.await;
		});
	}
}

/// Build the payload for an HDR mode control message (type 0x010e).
///
/// Format matches Sunshine's `control_hdr_mode_t`:
/// - u16 LE: message type (0x010e)
/// - u16 LE: payload length (1 + 30 = 31 bytes for metadata)
/// - u8: enabled (1 = HDR on, 0 = HDR off)
/// - SS_HDR_METADATA: 30 bytes (15 x u16 LE fields: display primaries,
///   white point, luminance, content light levels, padding). Populated
///   from the provided metadata, or zeroed when `metadata` is `None`.
fn build_hdr_mode_payload(enabled: bool, metadata: Option<&HdrMetadata>) -> Vec<u8> {
	let metadata_size = 30u16; // SS_HDR_METADATA: 15 x u16 fields
	let payload_len = 1 + metadata_size; // enabled byte + metadata
	let mut buf = Vec::with_capacity(4 + payload_len as usize);
	buf.extend((ControlMessageType::HdrMode as u16).to_le_bytes());
	buf.extend(payload_len.to_le_bytes());
	buf.push(if enabled { 1 } else { 0 });

	if let Some(m) = metadata {
		// Display primaries (RGB order).
		for &(x, y) in &m.display_primaries {
			buf.extend(x.to_le_bytes());
			buf.extend(y.to_le_bytes());
		}
		// White point.
		buf.extend(m.white_point.0.to_le_bytes());
		buf.extend(m.white_point.1.to_le_bytes());
		// Max display luminance (convert from 0.0001 cd/m² to nits).
		buf.extend(((m.max_luminance / 10000).min(u16::MAX as u32) as u16).to_le_bytes());
		// Min display luminance (0.0001 cd/m² maps to 1/10000th nit).
		buf.extend((m.min_luminance.min(u16::MAX as u32) as u16).to_le_bytes());
		// Content light levels (direct copy, already in nits).
		buf.extend(m.max_cll.to_le_bytes());
		buf.extend(m.max_fall.to_le_bytes());
		// maxFullFrameLuminance (not available from Wayland protocol).
		buf.extend(0u16.to_le_bytes());
		// Padding.
		buf.extend([0u8; 4]);
	} else {
		buf.extend(std::iter::repeat_n(0u8, metadata_size as usize));
	}

	buf
}

/// Build the payload for a termination extended control message.
///
/// The encrypted protocol uses V2 framing: `type(u16 LE) + length(u16 LE) + data`.
/// The client strips the length field after decryption (V2 → V1 conversion) and
/// then reads a 4-byte big-endian error code. Known NVST_DISCONN values are mapped
/// to client-side error constants (e.g. `0x80030023` → graceful termination).
fn build_termination_payload(error_code: u32) -> Vec<u8> {
	let payload_len = 4u16; // 4 bytes for error code.
	let mut buf = Vec::with_capacity(4 + payload_len as usize);
	buf.extend((ControlMessageType::TerminationExtended as u16).to_le_bytes());
	buf.extend(payload_len.to_le_bytes());
	buf.extend(error_code.to_be_bytes());
	buf
}

/// Send an encrypted control packet to the connected peer if it exists and is connected.
fn send_to_peer(
	host: &mut Host,
	peer_id: tokio_enet::PeerId,
	key: &[u8],
	sequence_number: u32,
	payload: &[u8],
	label: &str,
) {
	if let Ok(packet) = encode_control(key, sequence_number, payload)
		&& let Some(peer) = host.peer_mut(peer_id)
		&& peer.state() == PeerState::Connected
	{
		let _ = peer
			.send(0, Packet::new(packet.as_slice(), PacketMode::ReliableSequenced))
			.map_err(|e| tracing::warn!("Failed to send {label} to peer: {e}"));
	}
}

/// Build and send an HDR mode control message, then advance `sequence_number`.
fn send_hdr_state(
	host: &mut Host,
	peer_id: tokio_enet::PeerId,
	state: &HdrModeState,
	key: &[u8],
	sequence_number: &mut u32,
	label: &str,
) {
	let metadata = if state.enabled {
		Some(state.metadata.unwrap_or_else(HdrMetadata::fallback))
	} else {
		None
	};
	let payload = build_hdr_mode_payload(state.enabled, metadata.as_ref());
	send_to_peer(host, peer_id, key, *sequence_number, &payload, label);
	*sequence_number += 1;
	tracing::debug!("Sent HDR mode ({label}) to client: enabled={}", state.enabled);
}

/// Send a latency protocol message to the client, then advance `sequence_number`.
fn send_latency_message(
	host: &mut Host,
	peer_id: tokio_enet::PeerId,
	key: &[u8],
	sequence_number: &mut u32,
	message: &latency_protocol::Message,
) {
	let payload = latency_protocol::control_message(message);
	send_to_peer(
		host,
		peer_id,
		key,
		*sequence_number,
		&payload,
		"latency protocol message",
	);
	*sequence_number += 1;
}

/// The latency protocol's report of a frame the video sender wrote to the client.
fn frame_sent_message(sent: &FrameSent) -> latency_protocol::Message {
	latency_protocol::Message::FrameSent {
		frame_number: sent.frame_number,
		send_started_ns: latency_protocol::monotonic_ns(sent.started),
		send_ended_ns: latency_protocol::monotonic_ns(sent.ended),
	}
}

#[allow(clippy::too_many_arguments)]
async fn run_control_loop(
	stream_timeout: u64,
	mut host: Host,
	video_handle: VideoStreamHandle,
	audio_trigger: StartSignal,
	context: ControlStreamContext,
	input_handler: InputHandler,
	frame_clock: crate::session::compositor::frame_clock::CompositorFrameClock,
	stop_session_manager: ShutdownManager<SessionShutdownReason>,
	mut hdr_metadata_rx: watch::Receiver<HdrModeState>,
) {
	// Trigger session shutdown when the control stream stops.
	let _session_stop_token = stop_session_manager.trigger_shutdown_token(SessionShutdownReason::ControlStreamStopped);
	let _delay_stop = stop_session_manager.delay_shutdown_token();

	let mut stop_deadline = std::time::Instant::now() + std::time::Duration::from_secs(stream_timeout);

	// Create a channel over which we can receive feedback messages to send to the connected client.
	let (feedback_tx, mut feedback_rx) = mpsc::channel::<FeedbackCommand>(10);

	// Sequence number of feedback messages.
	let mut sequence_number = 0u32;
	let mut send_hdr_mode = false;
	let mut audio_triggered = false;
	// Track which peer slot the client is connected to.
	let mut connected_peer: Option<tokio_enet::PeerId> = None;
	// Whether the connected client has the capture epoch of the video RTP timestamps.
	let mut capture_epoch_sent = false;
	let mut latency_client_ready = false;
	let mut last_clock_report = None;
	let mut frame_sent = video_handle.frame_sent();

	while !stop_session_manager.is_shutdown_triggered() {
		// A clock-sync request this iteration received, with its host receive time, answered
		// after the `host.service()` match.
		let mut clock_sync_request = None;

		// Check if the timeout has passed.
		if std::time::Instant::now() > stop_deadline {
			tracing::info!(
				"Stopping because we haven't received a ping for {} seconds.",
				stream_timeout
			);
			break;
		}

		// Check for feedback messages.
		if let Ok(command) = feedback_rx.try_recv()
			&& let Some(peer_id) = connected_peer
		{
			tracing::debug!("Sending control feedback command: {command:?}");
			let payload = command.as_packet();
			let key = context.keys_rx.borrow().remote_input_key.clone();
			send_to_peer(&mut host, peer_id, &key, sequence_number, &payload, "feedback");
			sequence_number += 1;
		}

		match host
			.service(Duration::from_millis(10))
			.await
			.map_err(|e| tracing::error!("Failure in enet host: {e}"))
		{
			Ok(Some(Event::Connect { peer_id, .. })) => {
				connected_peer = Some(peer_id);
				capture_epoch_sent = false;
				latency_client_ready = false;
				last_clock_report = None;
				frame_clock.new_client();
			},
			Ok(Some(Event::Disconnect { peer_id, .. })) => {
				if connected_peer == Some(peer_id) {
					connected_peer = None;
				}
			},
			Ok(Some(Event::Receive { ref packet, .. })) => {
				// Before decrypting: a clock-sync request's host receive time.
				let received_at = std::time::Instant::now();
				let mut control_message = match ControlMessage::from_bytes(packet.data()) {
					Ok(control_message) => control_message,
					Err(()) => break,
				};
				tracing::trace!("Received control message: {control_message:?}");

				// First check for encrypted control messages and decrypt them.
				let decrypted;
				let encrypted = matches!(control_message, ControlMessage::Encrypted(_));
				if let ControlMessage::Encrypted(message) = control_message {
					let decrypted_result = decrypt_control(&message, &context.keys_rx.borrow().remote_input_key);

					decrypted = match decrypted_result {
						Ok(decrypted) => decrypted,
						Err(e) => {
							tracing::warn!("Failed to decrypt control message: {:?}", e);
							continue;
						},
					};

					control_message = match ControlMessage::from_bytes(&decrypted) {
						Ok(decrypted_message) => decrypted_message,
						Err(()) => continue,
					};

					tracing::trace!("Decrypted control message: {control_message:?}");
				}

				match control_message {
					ControlMessage::Encrypted(_) => {
						unreachable!("Encrypted control messages should be decrypted already.")
					},
					ControlMessage::RequestIdrFrame => {
						video_handle.request_idr_frame();
					},
					ControlMessage::InvalidateReferenceFrames { first, last } => {
						video_handle.invalidate_reference_frames(first, last);
					},
					ControlMessage::StartB => {
						video_handle.trigger();
						if !audio_triggered {
							audio_trigger.trigger();
							audio_triggered = true;
						}
						send_hdr_mode = context.hdr;
					},
					ControlMessage::Ping => {
						stop_deadline = std::time::Instant::now() + std::time::Duration::from_secs(stream_timeout);
					},
					ControlMessage::InputData(event) => {
						let _ = input_handler.handle_raw_input(event, feedback_tx.clone()).await;
					},
					ControlMessage::HdrMode => {
						tracing::info!("Received HdrMode toggle from client");
					},
					ControlMessage::LatencyMessage(payload) => {
						if !latency_protocol::accepts_message(context.latency_protocol, encrypted) {
							tracing::debug!(
								"Ignoring latency protocol message (negotiated: {}, encrypted: {encrypted}).",
								context.latency_protocol
							);
						} else {
							match latency_protocol::Message::decode(payload) {
								Some(request @ latency_protocol::Message::ClockSyncRequest { .. }) => {
									clock_sync_request = Some((request, latency_protocol::monotonic_ns(received_at)));
									latency_client_ready = true;
								},
								Some(correction @ latency_protocol::Message::Correction { .. }) => {
									frame_clock.apply(correction);
								},
								Some(message) => tracing::trace!("Received latency protocol message: {message:?}"),
								None => tracing::debug!(
									"Ignoring latency protocol message of {} bytes: unknown version or type.",
									payload.len()
								),
							}
						}
					},
					skipped_message => {
						tracing::trace!("Skipped control message: {skipped_message:?}");
					},
				};
			},
			Ok(None) => (),
			Err(_) => break,
		}

		// Answer a clock-sync request with the host's receive and send times.
		if let Some((request, host_received)) = clock_sync_request
			&& let Some(peer_id) = connected_peer
		{
			let key = context.keys_rx.borrow().remote_input_key.clone();
			let host_sent = latency_protocol::monotonic_ns(std::time::Instant::now());
			if let Some(reply) = latency_protocol::reply_to(request, host_received, host_sent) {
				send_latency_message(&mut host, peer_id, &key, &mut sequence_number, &reply);
				tracing::trace!("Answered clock-sync request: {reply:?}");
			}
		}

		// Announce the capture epoch of the video RTP timestamps once the first frame has one, so
		// the client can place each frame's capture on its own clock. It goes with a clock-sync
		// reply: the client asks only once it listens for latency messages, and drops earlier ones.
		if context.latency_protocol
			&& !capture_epoch_sent
			&& clock_sync_request.is_some()
			&& let Some(peer_id) = connected_peer
			&& let Some(epoch) = video_handle.rtp_epoch()
		{
			let key = context.keys_rx.borrow().remote_input_key.clone();
			let epoch_ns = latency_protocol::monotonic_ns(epoch);
			let message = latency_protocol::Message::CaptureEpoch {
				epoch_ns,
				rtp_clock_rate: RTP_CLOCK_RATE,
			};
			send_latency_message(&mut host, peer_id, &key, &mut sequence_number, &message);
			capture_epoch_sent = true;
			tracing::info!("Latency protocol: announced the capture epoch ({epoch_ns} ns, {RTP_CLOCK_RATE} Hz).");
		}

		// Wait until the client listens, then report this Stream's clock and
		// acknowledge only corrections actually installed by the compositor thread.
		let report = frame_clock.report();
		if latency_client_ready
			&& last_clock_report != Some(report)
			&& let Some(peer_id) = connected_peer
		{
			let key = context.keys_rx.borrow().remote_input_key.clone();
			send_latency_message(&mut host, peer_id, &key, &mut sequence_number, &report);
			tracing::info!("Latency protocol: frame clock {report:?}");
			last_clock_report = Some(report);
		}

		// Report every frame written to the client once it listens, so it can split each frame's
		// arrival between the host's send and the network. Drained every iteration either way.
		loop {
			let sent = match frame_sent.try_recv() {
				Ok(sent) => sent,
				Err(TryRecvError::Lagged(skipped)) => {
					tracing::debug!("Latency protocol: {skipped} frame-sent reports skipped.");
					continue;
				},
				Err(TryRecvError::Empty | TryRecvError::Closed) => break,
			};
			if latency_client_ready && let Some(peer_id) = connected_peer {
				let key = context.keys_rx.borrow().remote_input_key.clone();
				send_latency_message(
					&mut host,
					peer_id,
					&key,
					&mut sequence_number,
					&frame_sent_message(&sent),
				);
			}
		}

		// Send HDR mode notification after the host.service() match to avoid double mutable borrow.
		if send_hdr_mode {
			send_hdr_mode = false;
			if let Some(peer_id) = connected_peer {
				let state = hdr_metadata_rx.borrow_and_update().clone();
				let key = context.keys_rx.borrow().remote_input_key.clone();
				tracing::info!("Informing client: HDR session");
				send_hdr_state(&mut host, peer_id, &state, &key, &mut sequence_number, "initial");
			}
		}

		// Check for HDR metadata updates from the video pipeline.
		if context.hdr
			&& hdr_metadata_rx.has_changed().unwrap_or(false)
			&& let Some(peer_id) = connected_peer
		{
			let state = hdr_metadata_rx.borrow_and_update().clone();
			let key = context.keys_rx.borrow().remote_input_key.clone();
			send_hdr_state(
				&mut host,
				peer_id,
				&state,
				&key,
				&mut sequence_number,
				"metadata update",
			);
		}
	}

	tracing::debug!("Control stream stopped.");

	// Notify the client of graceful termination before closing the connection.
	// NVST_DISCONN_SERVER_TERMINATED_CLOSED (0x80030023) is recognized by the
	// client as a graceful shutdown so it does not display an error.
	let termination_payload = build_termination_payload(0x80030023);
	if let Some(peer_id) = connected_peer {
		let key = context.keys_rx.borrow().remote_input_key.clone();
		send_to_peer(
			&mut host,
			peer_id,
			&key,
			sequence_number,
			&termination_payload,
			"termination",
		);
	}
	let _ = host.flush().await;

	// Explicitly drop the ENet host before the delay shutdown token
	// to ensure the socket is released before wait_shutdown_complete
	// returns. Without this, the next session's control stream may
	// fail to bind to the same port.
	drop(host);
}

#[cfg(test)]
mod tests {
	use super::*;

	const KEY: [u8; 16] = [7; 16];

	/// A control message as moonlight-common-c frames it: type and length (little-endian), then the body.
	fn plain(message_type: u16, body: &[u8]) -> Vec<u8> {
		let mut buffer = Vec::new();
		buffer.extend(message_type.to_le_bytes());
		buffer.extend((body.len() as u16).to_le_bytes());
		buffer.extend(body);
		buffer
	}

	/// `plain` wrapped as the client encrypts it on the V2 control stream.
	fn client_encrypted(sequence_number: u32, inner: &[u8]) -> Vec<u8> {
		let mut initialization_vector = [0u8; 12];
		initialization_vector[0..4].copy_from_slice(&sequence_number.to_le_bytes());
		initialization_vector[10] = b'C';
		initialization_vector[11] = b'C';
		let mut tag = [0u8; 16];
		let payload = encrypt(inner, &KEY, &initialization_vector, &mut tag).unwrap();
		EncryptedControlMessage {
			length: (4 + ENCRYPTION_TAG_LENGTH + payload.len()) as u16,
			sequence_number,
			tag,
			payload,
		}
		.as_bytes()
	}

	#[test]
	fn parses_a_latency_message_with_its_payload() {
		let packet = plain(0x55f0, &[1, 2, 3]);
		match ControlMessage::from_bytes(&packet) {
			Ok(ControlMessage::LatencyMessage(payload)) => assert_eq!(payload, &[1, 2, 3]),
			other => panic!("expected a latency message, got {other:?}"),
		}
	}

	#[test]
	fn decrypts_a_latency_message_from_the_encrypted_control_stream() {
		let packet = client_encrypted(5, &plain(0x55f0, &[9, 8, 7, 6]));
		let Ok(ControlMessage::Encrypted(message)) = ControlMessage::from_bytes(&packet) else {
			panic!("expected an encrypted control message");
		};
		let decrypted = decrypt_control(&message, &KEY).unwrap();
		match ControlMessage::from_bytes(&decrypted) {
			Ok(ControlMessage::LatencyMessage(payload)) => assert_eq!(payload, &[9, 8, 7, 6]),
			other => panic!("expected a latency message, got {other:?}"),
		}
	}

	#[test]
	fn rejects_a_latency_message_under_the_wrong_key() {
		let packet = client_encrypted(5, &plain(0x55f0, &[9, 8, 7, 6]));
		let Ok(ControlMessage::Encrypted(message)) = ControlMessage::from_bytes(&packet) else {
			panic!("expected an encrypted control message");
		};
		assert!(decrypt_control(&message, &[8; 16]).is_err());
	}
}
