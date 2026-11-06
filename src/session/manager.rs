use std::sync::Arc;
use std::time::Duration;

use async_shutdown::ShutdownManager;
use tokio::sync::{Mutex, broadcast, watch};

use crate::ShutdownReason;
use crate::session::FrameStats;
use crate::session::InitializedSession;
use crate::session::SessionContext;
use crate::session::SessionKeyData;
use crate::session::SessionKeys;
use crate::session::SessionKeysSender;
use crate::session::SessionState;
use crate::session::application::GAME_STOP_TIMEOUT;
use crate::session::compositor::CompositorConfig;
use crate::session::stream::StartSignal;
use crate::session::stream::audio::AudioStreamConfig;
use crate::session::stream::audio::AudioStreamContext;
use crate::session::stream::control::ControlStreamConfig;
use crate::session::stream::video::VideoStreamConfig;
use crate::session::stream::video::VideoStreamContext;

/// How long a stopped session gets to finish: its compositor stays until the Game's unit has
/// stopped, which takes up to `GAME_STOP_TIMEOUT` (a Steam's own shutdown), then the compositor
/// and the streams wind down.
const SESSION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(GAME_STOP_TIMEOUT.as_secs() + 10);
const _: () = assert!(SESSION_SHUTDOWN_TIMEOUT.as_secs() > GAME_STOP_TIMEOUT.as_secs());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionShutdownReason {
	/// Session manager is shutting down.
	ManagerShutdown,
	/// The session was stopped by the user.
	UserStopped,
	/// The launched application exited.
	ApplicationStopped,
	/// Video packet handler stopped unexpectedly.
	VideoPacketHandlerStopped,
	/// Video encoder stopped unexpectedly.
	VideoEncoderStopped,
	/// Audio packet handler stopped unexpectedly.
	AudioPacketHandlerStopped,
	/// PulseAudio server stopped unexpectedly.
	PulseServerStopped,
	/// Audio encoder stopped unexpectedly.
	AudioEncoderStopped,
	/// Control stream stopped unexpectedly.
	ControlStreamStopped,
	/// Input handler stopped unexpectedly.
	InputHandlerStopped,
	/// Compositor stopped unexpectedly.
	CompositorStopped,
}

struct SessionManagerInner {
	/// Configuration for the compositor.
	compositor_config: CompositorConfig,

	/// Configuration for the video stream.
	video_config: VideoStreamConfig,

	/// Configuration for the audio stream.
	audio_config: AudioStreamConfig,

	/// Configuration for the control stream.
	control_config: ControlStreamConfig,

	/// Address to bind streams to.
	address: String,

	/// Time in seconds since last ping after which the stream closes.
	stream_timeout: u64,

	/// Whether to inhibit system sleep while a session is active.
	inhibit_sleep: bool,

	/// The currently active session, if any.
	session: Option<SessionState>,

	/// Shutdown manager for the active session, used to trigger session shutdown upon request.
	stop: ShutdownManager<SessionShutdownReason>,

	/// Shutdown manager of the last session taken out of the manager. Until it completes, that
	/// session's compositor is still up, waiting for its Game's unit to stop: a new session waits
	/// for it, since its Game would take the same unit.
	stopping: Option<ShutdownManager<SessionShutdownReason>>,

	/// Sender for session keys, used to update keys from the webserver in different subsystems.
	///
	/// Subsystems that get updated are: video encoder, audio encoder and input handler.
	keys_tx: Option<SessionKeysSender>,

	/// Stream contexts received via RTSP ANNOUNCE, consumed by `start_session`.
	video_stream_context: Option<VideoStreamContext>,
	audio_stream_context: Option<AudioStreamContext>,

	/// Whether the ANNOUNCE negotiated the latency protocol, consumed by `start_session`.
	latency_protocol: bool,

	/// Broadcast sender for per-frame encoding statistics.
	stats_tx: tokio::sync::broadcast::Sender<FrameStats>,

	/// Watchdog task for monitoring unexpected session shutdowns.
	stop_watcher: Option<tokio::task::JoinHandle<()>>,

	/// Starts the video pipeline (used by bench / external callers).
	video_start: Option<StartSignal>,

	/// Starts the audio pipeline (used by bench / external callers).
	audio_start: Option<StartSignal>,

	/// Shutdown manager for the entire application.
	shutdown: ShutdownManager<ShutdownReason>,

	/// Trigger token for the session manager's own shutdown trigger.
	///
	/// Used to trigger an application shutdown if the session manager stops unexpectedly.
	_trigger_token: async_shutdown::TriggerShutdownToken<ShutdownReason>,

	/// Delay token for the session manager's own shutdown trigger.
	///
	/// Used to delay shutdown until the session manager has cleaned up.
	_delay_token: async_shutdown::DelayShutdownToken<ShutdownReason>,
}

impl SessionManagerInner {
	/// Resets the manager's session state and hands back the session, which the caller drops
	/// after releasing the lock.
	#[must_use = "drop the session after releasing the lock"]
	fn reset_session(&mut self) -> Option<SessionState> {
		if let Some(handle) = self.stop_watcher.take() {
			handle.abort();
		}
		self.keys_tx = None;
		self.video_stream_context = None;
		self.audio_stream_context = None;
		self.latency_protocol = false;
		self.video_start = None;
		self.audio_start = None;
		let stop = std::mem::replace(&mut self.stop, ShutdownManager::new());
		let session = self.session.take();
		if session.is_some() {
			self.stopping = Some(stop);
		}
		session
	}
}

impl Drop for SessionManagerInner {
	fn drop(&mut self) {
		if let Some(handle) = self.stop_watcher.take() {
			handle.abort();
		}
		if self.session.is_some() {
			tracing::debug!("Stopping active session before shutdown.");
			// Drop the session first, as `stop_session` does: that starts the Game unit's stop,
			// which the compositor waits for before the session's shutdown can complete.
			self.session = None;
			let _ = self.stop.trigger_shutdown(SessionShutdownReason::ManagerShutdown);
			// Wait until shutdown completed.
			if let Ok(handle) = tokio::runtime::Handle::try_current() {
				handle.block_on(self.stop.wait_shutdown_complete());
			}
		}
	}
}

#[derive(Clone)]
pub struct SessionManager {
	inner: Arc<Mutex<SessionManagerInner>>,
	stats_tx: broadcast::Sender<FrameStats>,
}

impl SessionManager {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		compositor_config: CompositorConfig,
		video_config: VideoStreamConfig,
		audio_config: AudioStreamConfig,
		control_config: ControlStreamConfig,
		address: String,
		stream_timeout: u64,
		inhibit_sleep: bool,
		shutdown: ShutdownManager<ShutdownReason>,
	) -> Result<Self, ()> {
		let trigger_token = shutdown.trigger_shutdown_token(ShutdownReason::SessionManagerShutdown);
		let delay_token = shutdown.delay_shutdown_token().map_err(|e| {
			tracing::error!("Failed to create delay shutdown token: {e:?}");
		})?;

		let inner = SessionManagerInner {
			compositor_config,
			video_config,
			audio_config,
			control_config,
			address,
			stream_timeout,
			inhibit_sleep,
			session: None,
			stop: ShutdownManager::new(),
			stopping: None,
			keys_tx: None,
			video_stream_context: None,
			audio_stream_context: None,
			latency_protocol: false,
			stats_tx: tokio::sync::broadcast::channel(256).0,
			stop_watcher: None,
			video_start: None,
			audio_start: None,
			shutdown: shutdown.clone(),
			_trigger_token: trigger_token,
			_delay_token: delay_token,
		};

		let stats_tx = inner.stats_tx.clone();
		let inner = Arc::new(Mutex::new(inner));

		Ok(Self { inner, stats_tx })
	}

	/// Returns a receiver for per-frame encoding statistics.
	///
	/// Call **before** `initialize_session()` to receive stats from the start.
	/// Multiple receivers can be created — each receives a copy of every message.
	pub fn bench_stats_receiver(&self) -> tokio::sync::broadcast::Receiver<FrameStats> {
		self.stats_tx.subscribe()
	}

	/// Trigger the video and audio pipelines to start encoding.
	///
	/// In the normal flow, this is triggered by the control stream when the
	/// client sends `StartB`. Call this from external callers (e.g. bench binary)
	/// that have no Moonlight client. Must be called after `start_session()`.
	pub async fn trigger_streams_start(&self) {
		let inner = self.inner.lock().await;
		for start in [&inner.video_start, &inner.audio_start].into_iter().flatten() {
			start.trigger();
		}
	}

	/// Set the video and audio stream contexts after receiving RTSP ANNOUNCE, and whether it
	/// negotiated the latency protocol.
	pub async fn set_stream_context(
		&self,
		video_stream_context: VideoStreamContext,
		audio_stream_context: AudioStreamContext,
		latency_protocol: bool,
	) -> Result<(), ()> {
		let mut guard = self.inner.lock().await;
		match guard.session.as_ref() {
			Some(SessionState::Launched(_)) => {
				tracing::debug!("Stream contexts received via RTSP ANNOUNCE.");
				guard.video_stream_context = Some(video_stream_context);
				guard.audio_stream_context = Some(audio_stream_context);
				guard.latency_protocol = latency_protocol;
				Ok(())
			},
			Some(SessionState::Initialized(_)) => {
				tracing::warn!("SetStreamContext rejected: session not yet launched (Initialized state)");
				Err(())
			},
			Some(SessionState::Active(_)) => {
				// Client is resuming an already-running session (reconnect). The video,
				// audio, and control streams are still running and re-learn the client's
				// address from its PINGs (with refreshed keys via `/resume`), so there is
				// nothing to rebuild — accept the re-ANNOUNCE without storing new contexts.
				//
				// The latency protocol stays as the session's first ANNOUNCE negotiated it.
				// That is safe both ways: the Host sends latency messages only in reply to a
				// client's clock-sync request, which a client that didn't offer the protocol
				// never sends; and a client that offers it to a session that didn't negotiate
				// it gets no replies, so its frame clock is never steered.
				tracing::info!("Resuming active session: accepting RTSP ANNOUNCE from reconnecting client.");
				Ok(())
			},
			None => {
				tracing::warn!("SetStreamContext rejected: no active session");
				Err(())
			},
		}
	}

	/// Get the current session context if there is an active session; otherwise return `None`.
	pub async fn get_session_context(&self) -> Result<Option<SessionContext>, ()> {
		let guard = self.inner.lock().await;
		Ok(guard.session.as_ref().map(|s| s.context().clone()))
	}

	/// Initialize a new session with the provided context.
	///
	/// The session is not launched until `launch_session` is called.
	pub async fn initialize_session(&self, mut context: SessionContext) -> Result<(), ()> {
		let previous = self.inner.lock().await.stopping.clone();
		wait_for_previous_session(previous, SESSION_SHUTDOWN_TIMEOUT).await?;

		let mut guard = self.inner.lock().await;
		if guard
			.stopping
			.as_ref()
			.is_some_and(ShutdownManager::is_shutdown_completed)
		{
			guard.stopping = None;
		}

		if guard.session.is_some() || guard.keys_tx.is_some() {
			tracing::warn!("Session already initialized, rejecting InitializeSession command.");
			return Err(());
		}

		// Extract the raw keys from context and create the watch channel.
		let session_keys = match context.keys {
			SessionKeys::Keys(data) => data,
			SessionKeys::Rx(_) => {
				tracing::error!("Session keys already initialized as a watch receiver");
				return Err(());
			},
		};
		let (tx, rx) = watch::channel(session_keys);
		context.keys = SessionKeys::Rx(rx);

		let compositor_config = guard.compositor_config.clone();
		let video_config = guard.video_config.clone();
		let audio_config = guard.audio_config.clone();
		let control_config = guard.control_config.clone();
		let address = guard.address.clone();
		let stop = guard.stop.clone();
		let stats_tx = guard.stats_tx.clone();
		let session = InitializedSession::new(
			compositor_config,
			video_config,
			audio_config,
			control_config,
			address,
			context,
			stop,
			stats_tx,
		)
		.await?;
		guard.session = Some(SessionState::Initialized(session));

		spawn_session_watchdog(&self.inner, &mut guard);
		tracing::info!("Session initialized successfully, waiting to be launched.");

		// Set the keys sender here so that it is not set if session initialization failed.
		guard.keys_tx = Some(tx);
		Ok(())
	}

	/// Launch the session by starting the compositor and application, but don't start streams until RTSP ANNOUNCE is received.
	pub async fn launch_session(&self) -> Result<(), ()> {
		let session = {
			let mut guard = self.inner.lock().await;
			match guard.session.take() {
				Some(SessionState::Initialized(session)) => session,
				Some(SessionState::Launched(launched)) => {
					guard.session = Some(SessionState::Launched(launched));
					tracing::warn!("LaunchSession rejected: session already launched");
					return Err(());
				},
				Some(SessionState::Active(active)) => {
					guard.session = Some(SessionState::Active(active));
					tracing::warn!("LaunchSession rejected: session already active");
					return Err(());
				},
				None => {
					tracing::warn!("LaunchSession rejected: no active session");
					return Err(());
				},
			}
		};

		tracing::info!("Launching session (starting compositor and app).");
		match session.launch().await {
			Ok(launched) => {
				let mut guard = self.inner.lock().await;
				guard.session = Some(SessionState::Launched(launched));
				tracing::info!("Session launched successfully, waiting for RTSP ANNOUNCE.");
				Ok(())
			},
			Err(()) => {
				let mut guard = self.inner.lock().await;
				let _ = guard.reset_session();
				tracing::error!("Failed to launch session, waiting for new session.");
				Err(())
			},
		}
	}

	/// Start the video and audio streams.
	///
	/// Returns `Ok(())` only after all three streams (video, audio, control) are
	/// successfully constructed. Returns `Err(())` if any stream fails to initialize.
	pub async fn start_session(&self) -> Result<(), ()> {
		let (launched, video_stream_context, audio_stream_context, latency_protocol, stop) = {
			let mut guard = self.inner.lock().await;
			let video_stream_context = guard.video_stream_context.take();
			let audio_stream_context = guard.audio_stream_context.take();
			let latency_protocol = std::mem::take(&mut guard.latency_protocol);
			match guard.session.take() {
				Some(SessionState::Launched(launched)) => (
					launched,
					video_stream_context,
					audio_stream_context,
					latency_protocol,
					guard.stop.clone(),
				),
				Some(SessionState::Initialized(session)) => {
					guard.session = Some(SessionState::Initialized(session));
					tracing::warn!("StartSession rejected: session not yet launched");
					return Err(());
				},
				Some(SessionState::Active(active)) => {
					// Resume (reconnect): the streams are already running, so PLAY is a
					// no-op — the client picks up the existing streams once it PINGs.
					// The reconnecting client is a fresh Moonlight session that expects
					// frame numbers to start at 1, so reset the video frame counters and
					// force an IDR; otherwise it sees the running counter as a huge frame
					// gap and reports a poor connection. The reset fires on the client's
					// first video PING, once its new address is known.
					active.reset_video_stream();
					guard.session = Some(SessionState::Active(active));
					tracing::info!(
						"Resuming active session: resetting video frame counter and treating PLAY as no-op."
					);
					return Ok(());
				},
				None => {
					tracing::warn!("StartSession rejected: no active session");
					return Err(());
				},
			}
		};

		let video_stream_context = video_stream_context.ok_or_else(|| {
			tracing::error!("VideoStreamContext not set");
		})?;
		let audio_stream_context = audio_stream_context.ok_or_else(|| {
			tracing::error!("AudioStreamContext not set");
		})?;

		tracing::info!("Starting session streams.");
		let mut guard = self.inner.lock().await;
		let video_config = guard.video_config.clone();
		let stream_timeout = guard.stream_timeout;
		match launched
			.start(
				video_config,
				stream_timeout,
				video_stream_context,
				audio_stream_context,
				latency_protocol,
				stop,
				guard.inhibit_sleep,
			)
			.await
		{
			Ok((active, video_start, audio_start)) => {
				guard.session = Some(SessionState::Active(active));
				guard.video_start = Some(video_start);
				guard.audio_start = Some(audio_start);
				Ok(())
			},
			Err(()) => {
				let _ = guard.reset_session();
				tracing::error!("Failed to start session streams.");
				Err(())
			},
		}
	}

	/// Stop the session and return to Uninitialized state.
	pub async fn stop_session(&self) -> Result<(), ()> {
		let (session, stop, shutdown) = {
			let mut guard = self.inner.lock().await;
			if guard.session.is_none() {
				return Ok(());
			}
			let stop = guard.stop.clone();
			let shutdown = guard.shutdown.clone();
			(guard.reset_session(), stop, shutdown)
		};
		// Hold the application's shutdown until the Game has stopped (see the watchdog).
		let delay = shutdown
			.delay_shutdown_token()
			.inspect_err(|e| tracing::warn!("Failed to delay shutdown for the session's stop: {e:?}"))
			.ok();

		// Drop the session first, outside the lock: that starts the Game unit's stop (which takes
		// a Steam's whole shutdown) on a thread of its own, and the compositor stays until it has
		// stopped. Then trigger shutdown of the compositor & streams.
		drop(session);
		let _ = stop.trigger_shutdown(SessionShutdownReason::UserStopped);

		let result = wait_for_session_shutdown(&stop, &shutdown, SESSION_SHUTDOWN_TIMEOUT).await;
		drop(delay);
		result?;
		tracing::info!("Session stopped by user, waiting for new session.");
		Ok(())
	}

	/// Update the session keys for the active session.
	pub(crate) async fn update_keys(&self, keys: SessionKeyData) -> Result<(), ()> {
		let guard = self.inner.lock().await;

		if guard.stop.is_shutdown_triggered() {
			tracing::warn!("Session is shutting down; rejecting resume key update.");
			return Err(());
		}

		if !matches!(guard.session.as_ref(), Some(SessionState::Active(_))) {
			tracing::warn!("No active streaming session to update keys for.");
			return Err(());
		}

		if let Some(keys_tx) = &guard.keys_tx {
			keys_tx.send_replace(keys);
		} else {
			tracing::warn!("Active streaming session has no key sender; rejecting resume.");
			return Err(());
		}

		Ok(())
	}
}

/// Spawn a watchdog task to monitor the session for unexpected shutdowns.
fn spawn_session_watchdog(inner: &Arc<Mutex<SessionManagerInner>>, guard: &mut SessionManagerInner) {
	if guard.stop_watcher.is_some() {
		tracing::error!("Session watchdog already running, not spawning another.");
		return;
	}

	let inner = inner.clone();
	let stop = guard.stop.clone();
	let shutdown = guard.shutdown.clone();
	// Hold the application's shutdown open until the session has fully stopped:
	// the process exits once it completes, and exiting while the session's
	// threads are still tearing down (the encoder's Vulkan device, the
	// compositor's GL renderer) crashes in NVIDIA's exit handlers.
	let delay = shutdown
		.delay_shutdown_token()
		.inspect_err(|e| tracing::warn!("Failed to delay shutdown for the session: {e:?}"))
		.ok();
	let handle = tokio::spawn(async move {
		tokio::select! {
			reason = stop.wait_shutdown_triggered() => {
				if reason == SessionShutdownReason::UserStopped {
					tracing::info!("Session shutdown requested by user.");
				} else {
					tracing::warn!("Session stopped unexpectedly (reason: {reason:?}), waiting for new session.");
				}
			},
			_ = shutdown.wait_shutdown_triggered() => {
				tracing::debug!("Global shutdown triggered, stopping active session.");
				let _ = stop.trigger_shutdown(SessionShutdownReason::ManagerShutdown);
			},
		}

		// First drop the session, outside the lock, so that the application exits as soon as
		// possible. `reset_session` aborts the watchdog's handle, and this task is the
		// watchdog: detach the handle first, or the abort would cancel the wait
		// below.
		let session = {
			let mut guard = inner.lock().await;
			drop(guard.stop_watcher.take());
			guard.reset_session()
		};
		drop(session);

		// Then wait for the session to shut down.
		if tokio::time::timeout(SESSION_SHUTDOWN_TIMEOUT, stop.wait_shutdown_complete())
			.await
			.is_err()
		{
			tracing::error!(
				"Session shutdown timed out after {}s.",
				SESSION_SHUTDOWN_TIMEOUT.as_secs()
			);
		}
		drop(delay);
	});
	guard.stop_watcher = Some(handle);
}

/// Wait for the session to shut down within the given timeout.
///
/// If the session does not shut down in time, triggers a global application
/// shutdown to prevent orphaned tasks and resource leaks.
async fn wait_for_session_shutdown(
	stop: &ShutdownManager<SessionShutdownReason>,
	shutdown: &ShutdownManager<ShutdownReason>,
	timeout: Duration,
) -> Result<(), ()> {
	match tokio::time::timeout(timeout, stop.wait_shutdown_complete()).await {
		Ok(_) => Ok(()),
		Err(_) => {
			tracing::error!(
				"Session shutdown timed out after {}s — triggering application shutdown.",
				timeout.as_secs()
			);
			let _ = shutdown.trigger_shutdown(ShutdownReason::SessionManagerShutdown);
			Err(())
		},
	}
}

/// Waits, up to `timeout`, for the session taken out before (`previous`) to finish stopping:
/// until then its compositor is up, waiting for its Game's unit to stop (a Steam's own shutdown),
/// and a new session's Game would take the same unit.
async fn wait_for_previous_session(
	previous: Option<ShutdownManager<SessionShutdownReason>>,
	timeout: Duration,
) -> Result<(), ()> {
	let Some(previous) = previous else {
		return Ok(());
	};
	if !previous.is_shutdown_completed() {
		tracing::info!("Waiting for the previous session to stop before starting a new one.");
	}
	match tokio::time::timeout(timeout, previous.wait_shutdown_complete()).await {
		Ok(_) => Ok(()),
		Err(_) => {
			tracing::warn!(
				"The previous session is still stopping after {}s, rejecting InitializeSession command.",
				timeout.as_secs()
			);
			Err(())
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A stopped session whose compositor still waits for its Game's unit to stop.
	fn stopping_session() -> (
		ShutdownManager<SessionShutdownReason>,
		async_shutdown::DelayShutdownToken<SessionShutdownReason>,
	) {
		let stop = ShutdownManager::new();
		let compositor = stop.delay_shutdown_token().unwrap();
		stop.trigger_shutdown(SessionShutdownReason::UserStopped).unwrap();
		(stop, compositor)
	}

	#[tokio::test]
	async fn a_new_session_waits_until_the_previous_one_has_stopped() {
		let (previous, compositor) = stopping_session();
		let wait = tokio::spawn(wait_for_previous_session(Some(previous), Duration::from_secs(5)));
		tokio::task::yield_now().await;
		assert!(!wait.is_finished(), "the previous Game is still stopping");

		drop(compositor);
		assert_eq!(wait.await.unwrap(), Ok(()));
	}

	#[tokio::test]
	async fn a_previous_session_not_yet_told_to_stop_is_waited_for_too() {
		let previous = ShutdownManager::new();
		let wait = tokio::spawn(wait_for_previous_session(
			Some(previous.clone()),
			Duration::from_secs(5),
		));
		tokio::task::yield_now().await;
		assert!(!wait.is_finished(), "taken out, but its shutdown isn't triggered yet");

		previous.trigger_shutdown(SessionShutdownReason::UserStopped).unwrap();
		assert_eq!(wait.await.unwrap(), Ok(()));
	}

	#[tokio::test]
	async fn a_new_session_is_refused_while_the_previous_one_is_still_stopping() {
		let (previous, _compositor) = stopping_session();
		assert_eq!(
			wait_for_previous_session(Some(previous), Duration::from_millis(50)).await,
			Err(())
		);
	}

	#[tokio::test]
	async fn with_no_previous_session_a_new_one_starts_at_once() {
		assert_eq!(wait_for_previous_session(None, Duration::ZERO).await, Ok(()));
	}
}
