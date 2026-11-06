// TODO: Remove this when a proper error type is implemented for all functions that return `Result<(), ()>`.
#![allow(clippy::result_unit_err)]

pub mod app_scanner;
pub mod clients;
pub mod config;
pub(crate) mod crypto;
pub mod discovery;
pub mod healthcheck;
pub mod latency_protocol;
pub mod rtsp;
pub mod session;
pub(crate) mod state;
pub mod tls;
pub mod webserver;

/// Reasons for initiating a global shutdown.
///
/// Used as the type parameter for `ShutdownManager<ShutdownReason>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownReason {
	/// Application quit signal (Ctrl+C or SIGTERM).
	AppQuit = 1,
	/// HTTP webserver is shutting down.
	HttpShutdown = 2,
	/// HTTPS webserver is shutting down.
	HttpsShutdown = 3,
	/// RTSP server is shutting down.
	RtspShutdown = 4,
	/// Session manager guard token (trigger_shutdown_token, not a shutdown trigger).
	SessionManagerShutdown = 5,
}

impl ShutdownReason {
	/// The process's exit status: 0 for a requested quit (SIGINT or SIGTERM), so a service
	/// manager records a stop as a stop, not a failure; the reason's number otherwise.
	pub fn exit_code(self) -> i32 {
		match self {
			Self::AppQuit => 0,
			other => other as i32,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::ShutdownReason;

	#[test]
	fn a_requested_quit_exits_0_and_the_other_reasons_fail() {
		assert_eq!(ShutdownReason::AppQuit.exit_code(), 0);
		for reason in [
			ShutdownReason::HttpShutdown,
			ShutdownReason::HttpsShutdown,
			ShutdownReason::RtspShutdown,
			ShutdownReason::SessionManagerShutdown,
		] {
			assert_ne!(reason.exit_code(), 0, "{reason:?}");
		}
	}
}
