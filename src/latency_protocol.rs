//! Negotiation of moonlight-ios' latency protocol (fork extension).
//!
//! The latency protocol carries clock sync and frame-time corrections between a client and this
//! host as opaque payloads of one control-stream message type. The host advertises it in
//! `x-ss-general.featureFlags` of its DESCRIBE reply when built with it (the `latency-protocol`
//! feature), and a session speaks it only when the client sets the matching bit in
//! `x-ml-general.featureFlags` of its ANNOUNCE. Its messages count only on the encrypted control
//! stream. Other clients see a normal Moonshine: they ignore unknown host feature bits.
//!
//! The wire values match the KamilPostrozny moonlight-common-c fork (`LI_FF_LATENCY_PROTOCOL`,
//! `ML_FF_LATENCY_PROTOCOL`, message type `0x55f0`).
//!
//! The host answers clock-sync requests, announces the capture epoch of the video RTP
//! timestamps, so the client can place every frame's capture on its own clock, and reports when
//! it wrote each video frame to its socket, so the client can split a frame's arrival between the
//! host's send and the network.

use std::sync::LazyLock;
use std::time::Instant;

/// Whether this build speaks the latency protocol (the `latency-protocol` feature).
pub const BUILT: bool = cfg!(feature = "latency-protocol");

/// Host bit in `x-ss-general.featureFlags` (moonlight-common-c's `LI_FF_LATENCY_PROTOCOL`).
pub const HOST_FEATURE_FLAG: u32 = 0x4000_0000;

/// Client bit in `x-ml-general.featureFlags` (moonlight-common-c's `ML_FF_LATENCY_PROTOCOL`).
pub const CLIENT_FEATURE_FLAG: u32 = 0x4000_0000;

/// Control-stream message type carrying a latency protocol payload (Sunshine extension range).
pub const MESSAGE_TYPE: u16 = 0x55f0;

/// Add the latency protocol to the host feature flags of a DESCRIBE reply when `built`.
pub const fn advertise(feature_flags: u32, built: bool) -> u32 {
	if built {
		feature_flags | HOST_FEATURE_FLAG
	} else {
		feature_flags
	}
}

/// Whether a session speaks the latency protocol: the build speaks it and the client's
/// `x-ml-general.featureFlags` (absent for clients that send none) offered it.
pub const fn negotiated(client_feature_flags: Option<u32>, built: bool) -> bool {
	match client_feature_flags {
		Some(flags) => built && flags & CLIENT_FEATURE_FLAG != 0,
		None => false,
	}
}

/// Whether the control stream takes a latency protocol message: only in a negotiated session, and
/// only when the message arrived inside an encrypted control message.
pub const fn accepts_message(negotiated: bool, encrypted: bool) -> bool {
	negotiated && encrypted
}

/// The protocol version this host speaks: the first byte of every message.
pub const VERSION: u8 = 1;

/// One latency protocol message, the payload of a `MESSAGE_TYPE` control message.
///
/// Each starts with a version byte and a type byte, followed by fixed-width little-endian fields.
/// Phone times are nanoseconds on the client's clock, which the host only echoes. Host times are
/// nanoseconds of `CLOCK_MONOTONIC`, the clock that stamps captured frames ([`monotonic_ns`]). The
/// layout matches moonlight-ios' `StreamCore/LatencyMessage.swift`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
	/// Type 1, client → host: asks for the host's clock.
	ClockSyncRequest { sequence: u32, phone_sent: u64 },
	/// Type 2, host → client: answers a request with the host's receive and send times.
	ClockSyncReply {
		sequence: u32,
		phone_sent: u64,
		host_received: u64,
		host_sent: u64,
	},
	/// Type 3, client → host: a frame period and a relative phase shift (positive: later).
	Correction {
		sequence: u32,
		period_ns: u64,
		phase_shift_ns: i64,
	},
	/// Type 4, host → client: whether the host can steer the Game's frames, and the last
	/// correction it applied (0: none).
	SteerableReport { steerable: bool, last_correction: u32 },
	/// Type 5, host → client: the host time of video RTP timestamp 0, and the RTP clock rate.
	CaptureEpoch { epoch_ns: u64, rtp_clock_rate: u32 },
	/// Type 6, host → client: the host wrote a video frame's packets to its socket, from the start
	/// of the first write to the end of the last. `frame_number` is the frame index its packets carry.
	FrameSent {
		frame_number: u32,
		send_started_ns: u64,
		send_ended_ns: u64,
	},
}

impl Message {
	/// Decode a payload; `None` for another version, an unknown type or too few bytes. Bytes past
	/// the message's fields are ignored.
	pub fn decode(payload: &[u8]) -> Option<Self> {
		let mut reader = Reader(payload);
		if reader.u8()? != VERSION {
			return None;
		}
		match reader.u8()? {
			1 => Some(Self::ClockSyncRequest {
				sequence: reader.u32()?,
				phone_sent: reader.u64()?,
			}),
			2 => Some(Self::ClockSyncReply {
				sequence: reader.u32()?,
				phone_sent: reader.u64()?,
				host_received: reader.u64()?,
				host_sent: reader.u64()?,
			}),
			3 => Some(Self::Correction {
				sequence: reader.u32()?,
				period_ns: reader.u64()?,
				phase_shift_ns: reader.u64()? as i64,
			}),
			4 => Some(Self::SteerableReport {
				steerable: reader.u8()? != 0,
				last_correction: reader.u32()?,
			}),
			5 => Some(Self::CaptureEpoch {
				epoch_ns: reader.u64()?,
				rtp_clock_rate: reader.u32()?,
			}),
			6 => Some(Self::FrameSent {
				frame_number: reader.u32()?,
				send_started_ns: reader.u64()?,
				send_ended_ns: reader.u64()?,
			}),
			_ => None,
		}
	}

	/// Encode the message as its payload.
	pub fn encode(&self) -> Vec<u8> {
		let mut buffer = vec![VERSION];
		match *self {
			Self::ClockSyncRequest { sequence, phone_sent } => {
				buffer.push(1);
				buffer.extend(sequence.to_le_bytes());
				buffer.extend(phone_sent.to_le_bytes());
			},
			Self::ClockSyncReply {
				sequence,
				phone_sent,
				host_received,
				host_sent,
			} => {
				buffer.push(2);
				buffer.extend(sequence.to_le_bytes());
				buffer.extend(phone_sent.to_le_bytes());
				buffer.extend(host_received.to_le_bytes());
				buffer.extend(host_sent.to_le_bytes());
			},
			Self::Correction {
				sequence,
				period_ns,
				phase_shift_ns,
			} => {
				buffer.push(3);
				buffer.extend(sequence.to_le_bytes());
				buffer.extend(period_ns.to_le_bytes());
				buffer.extend(phase_shift_ns.to_le_bytes());
			},
			Self::SteerableReport {
				steerable,
				last_correction,
			} => {
				buffer.push(4);
				buffer.push(steerable as u8);
				buffer.extend(last_correction.to_le_bytes());
			},
			Self::CaptureEpoch {
				epoch_ns,
				rtp_clock_rate,
			} => {
				buffer.push(5);
				buffer.extend(epoch_ns.to_le_bytes());
				buffer.extend(rtp_clock_rate.to_le_bytes());
			},
			Self::FrameSent {
				frame_number,
				send_started_ns,
				send_ended_ns,
			} => {
				buffer.push(6);
				buffer.extend(frame_number.to_le_bytes());
				buffer.extend(send_started_ns.to_le_bytes());
				buffer.extend(send_ended_ns.to_le_bytes());
			},
		}
		buffer
	}
}

/// The reply to a clock-sync request received at `host_received` and answered at `host_sent`
/// (both [`monotonic_ns`]); `None` for every other message.
pub const fn reply_to(message: Message, host_received: u64, host_sent: u64) -> Option<Message> {
	match message {
		Message::ClockSyncRequest { sequence, phone_sent } => Some(Message::ClockSyncReply {
			sequence,
			phone_sent,
			host_received,
			host_sent,
		}),
		_ => None,
	}
}

/// A message framed as the control message the host encrypts and sends: `MESSAGE_TYPE` and the
/// payload length (little-endian), then the payload.
pub fn control_message(message: &Message) -> Vec<u8> {
	let payload = message.encode();
	let mut buffer = Vec::with_capacity(4 + payload.len());
	buffer.extend(MESSAGE_TYPE.to_le_bytes());
	buffer.extend((payload.len() as u16).to_le_bytes());
	buffer.extend(payload);
	buffer
}

/// One read of `Instant` and of `CLOCK_MONOTONIC`, back to back.
static MONOTONIC_ANCHOR: LazyLock<(Instant, u64)> = LazyLock::new(|| {
	let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
	// SAFETY: `now` is a valid timespec for the call to write.
	unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
	(Instant::now(), now.tv_sec as u64 * 1_000_000_000 + now.tv_nsec as u64)
});

/// `instant` in nanoseconds of `CLOCK_MONOTONIC`. On Linux `Instant` reads that clock, and
/// ferriscope stamps each captured frame with `Instant::now()` when the compositor commits it.
/// `Instant` hides its value, so this maps it through one anchor read of both.
pub fn monotonic_ns(instant: Instant) -> u64 {
	let (anchor, anchor_ns) = *MONOTONIC_ANCHOR;
	match instant.checked_duration_since(anchor) {
		Some(after) => anchor_ns + after.as_nanos() as u64,
		None => anchor_ns - anchor.duration_since(instant).as_nanos() as u64,
	}
}

/// Reads little-endian fields off the front of a payload.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
	fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
		let (field, rest) = self.0.split_first_chunk::<N>()?;
		self.0 = rest;
		Some(*field)
	}

	fn u8(&mut self) -> Option<u8> {
		self.take::<1>().map(|[byte]| byte)
	}

	fn u32(&mut self) -> Option<u32> {
		self.take().map(u32::from_le_bytes)
	}

	fn u64(&mut self) -> Option<u64> {
		self.take().map(u64::from_le_bytes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	#[test]
	fn wire_values_match_the_client_fork() {
		assert_eq!(HOST_FEATURE_FLAG, 0x4000_0000);
		assert_eq!(CLIENT_FEATURE_FLAG, 0x4000_0000);
		assert_eq!(MESSAGE_TYPE, 0x55f0);
	}

	#[test]
	fn advertises_only_when_built() {
		assert_eq!(advertise(0x03, true), 0x4000_0003);
		assert_eq!(advertise(0x03, false), 0x03);
	}

	#[test]
	fn negotiates_only_when_built_and_offered() {
		assert!(negotiated(Some(CLIENT_FEATURE_FLAG | 0x03), true));
		assert!(!negotiated(Some(CLIENT_FEATURE_FLAG | 0x03), false));
		assert!(!negotiated(Some(0x03), true));
		assert!(!negotiated(None, true));
	}

	#[test]
	fn accepts_messages_only_encrypted_in_a_negotiated_session() {
		assert!(accepts_message(true, true));
		assert!(!accepts_message(true, false));
		assert!(!accepts_message(false, true));
		assert!(!accepts_message(false, false));
	}

	// The byte layouts of moonlight-ios' `LatencyMessageTests` (`Tests/StreamCoreTests`), copied
	// byte for byte: version 1, the type, then each field little-endian.

	const SYNC_REQUEST: Message = Message::ClockSyncRequest {
		sequence: 7,
		phone_sent: 0x0102_0304_0506_0708,
	};
	const SYNC_REQUEST_BYTES: &[u8] = &[
		0x01, 0x01, //
		0x07, 0x00, 0x00, 0x00, //
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
	];

	const SYNC_REPLY: Message = Message::ClockSyncReply {
		sequence: 0x1122_3344,
		phone_sent: 0x0102_0304_0506_0708,
		host_received: 0x0000_0012_3456_789A,
		host_sent: 0x0000_0012_3456_ABCD,
	};
	const SYNC_REPLY_BYTES: &[u8] = &[
		0x01, 0x02, //
		0x44, 0x33, 0x22, 0x11, //
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, //
		0x9A, 0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, //
		0xCD, 0xAB, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00,
	];

	// A period of 8.333333 ms, and the next frames 250 µs sooner.
	const CORRECTION: Message = Message::Correction {
		sequence: 3,
		period_ns: 8_333_333,
		phase_shift_ns: -250_000,
	};
	const CORRECTION_BYTES: &[u8] = &[
		0x01, 0x03, //
		0x03, 0x00, 0x00, 0x00, //
		0x15, 0x28, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, //
		0x70, 0x2F, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	];

	const STEERABLE_REPORT: Message = Message::SteerableReport {
		steerable: true,
		last_correction: 0x0102,
	};
	const STEERABLE_REPORT_BYTES: &[u8] = &[
		0x01, 0x04, //
		0x01, //
		0x02, 0x01, 0x00, 0x00,
	];

	const CAPTURE_EPOCH: Message = Message::CaptureEpoch {
		epoch_ns: 86_400_000_000_123,
		rtp_clock_rate: 90_000,
	};
	const CAPTURE_EPOCH_BYTES: &[u8] = &[
		0x01, 0x05, //
		0x7B, 0x00, 0x4F, 0x91, 0x94, 0x4E, 0x00, 0x00, //
		0x90, 0x5F, 0x01, 0x00,
	];

	// Frame 258 left the host from 86 400.004 s to 210 µs later.
	const FRAME_SENT: Message = Message::FrameSent {
		frame_number: 258,
		send_started_ns: 86_400_004_000_000,
		send_ended_ns: 86_400_004_210_000,
	};
	const FRAME_SENT_BYTES: &[u8] = &[
		0x01, 0x06, //
		0x02, 0x01, 0x00, 0x00, //
		0x00, 0x09, 0x8C, 0x91, 0x94, 0x4E, 0x00, 0x00, //
		0x50, 0x3D, 0x8F, 0x91, 0x94, 0x4E, 0x00, 0x00,
	];

	const FIXTURES: [(Message, &[u8]); 6] = [
		(SYNC_REQUEST, SYNC_REQUEST_BYTES),
		(SYNC_REPLY, SYNC_REPLY_BYTES),
		(CORRECTION, CORRECTION_BYTES),
		(STEERABLE_REPORT, STEERABLE_REPORT_BYTES),
		(CAPTURE_EPOCH, CAPTURE_EPOCH_BYTES),
		(FRAME_SENT, FRAME_SENT_BYTES),
	];

	#[test]
	fn each_message_has_the_clients_byte_layout() {
		for (message, bytes) in FIXTURES {
			assert_eq!(message.encode(), bytes, "{message:?}");
			assert_eq!(Message::decode(bytes), Some(message));
		}
	}

	#[test]
	fn other_versions_unknown_types_and_short_payloads_are_ignored() {
		let mut other_version = SYNC_REQUEST_BYTES.to_vec();
		other_version[0] = 0x02;
		assert_eq!(Message::decode(&other_version), None);
		other_version[0] = 0x00;
		assert_eq!(Message::decode(&other_version), None);
		for unknown in [0x00, 0x07, 0xFF] {
			let mut bytes = SYNC_REQUEST_BYTES.to_vec();
			bytes[1] = unknown;
			assert_eq!(Message::decode(&bytes), None);
		}
		assert_eq!(Message::decode(&[]), None);
		assert_eq!(Message::decode(&[0x01]), None);
		for (_, bytes) in FIXTURES {
			assert_eq!(Message::decode(&bytes[..bytes.len() - 1]), None);
		}
	}

	#[test]
	fn bytes_past_a_messages_fields_are_ignored() {
		for (message, bytes) in FIXTURES {
			let mut longer = bytes.to_vec();
			longer.extend([0xEE, 0xEE]);
			assert_eq!(Message::decode(&longer), Some(message));
		}
	}

	#[test]
	fn any_non_zero_steerable_byte_means_steerable() {
		let mut bytes = STEERABLE_REPORT_BYTES.to_vec();
		bytes[2] = 0x00;
		assert_eq!(
			Message::decode(&bytes),
			Some(Message::SteerableReport {
				steerable: false,
				last_correction: 0x0102
			})
		);
		bytes[2] = 0x80;
		assert_eq!(Message::decode(&bytes), Some(STEERABLE_REPORT));
	}

	#[test]
	fn a_clock_sync_request_is_answered_with_the_hosts_times() {
		let reply = reply_to(SYNC_REQUEST, 0x0000_0012_3456_789A, 0x0000_0012_3456_ABCD);
		assert_eq!(
			reply,
			Some(Message::ClockSyncReply {
				sequence: 7,
				phone_sent: 0x0102_0304_0506_0708,
				host_received: 0x0000_0012_3456_789A,
				host_sent: 0x0000_0012_3456_ABCD,
			})
		);
		for other in [SYNC_REPLY, CORRECTION, STEERABLE_REPORT, CAPTURE_EPOCH, FRAME_SENT] {
			assert_eq!(reply_to(other, 1, 2), None);
		}
	}

	#[test]
	fn a_message_goes_out_as_a_control_message_of_its_type() {
		let mut expected = vec![0xF0, 0x55, CAPTURE_EPOCH_BYTES.len() as u8, 0x00];
		expected.extend(CAPTURE_EPOCH_BYTES);
		assert_eq!(control_message(&CAPTURE_EPOCH), expected);
	}

	#[test]
	fn instants_map_onto_clock_monotonic_nanoseconds() {
		let read_monotonic = || {
			let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
			assert_eq!(unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) }, 0);
			now.tv_sec as u64 * 1_000_000_000 + now.tv_nsec as u64
		};
		let before = read_monotonic();
		let instant = Instant::now();
		let after = read_monotonic();
		let mapped = monotonic_ns(instant);
		// Instant and the anchor are two reads of the same clock; allow a little slack for them.
		assert!(
			mapped + 100_000 >= before && mapped <= after + 100_000,
			"{before} {mapped} {after}"
		);
		assert_eq!(monotonic_ns(instant + Duration::from_millis(5)) - mapped, 5_000_000);
		assert_eq!(mapped - monotonic_ns(instant - Duration::from_millis(5)), 5_000_000);
	}
}
