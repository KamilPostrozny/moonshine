//! The session's ferriscope frame clock, connected only after compositor readiness.

use std::{
	sync::{Arc, OnceLock},
	time::Duration,
};

use ferriscope::{FrameClockCorrection, FrameClockSender};

use crate::latency_protocol::Message;

#[derive(Clone, Default)]
pub(crate) struct CompositorFrameClock(Arc<OnceLock<FrameClockSender>>);

impl CompositorFrameClock {
	pub(super) fn connect(&self, sender: FrameClockSender) {
		let _ = self.0.set(sender);
	}

	/// All Moonshine video is captured from the session's ferriscope compositor.
	/// Its frame callbacks and FIFO releases pace both native and Xwayland clients,
	/// including Steam. There is no desktop-capture path to advertise as steerable.
	pub(crate) fn report(&self) -> Message {
		report(self.0.get().map(FrameClockSender::last_applied))
	}

	pub(crate) fn apply(&self, message: Message) {
		if let Some((sequence, correction)) = correction(message)
			&& let Some(sender) = self.0.get()
			&& let Err(error) = sender.send(sequence, correction)
		{
			tracing::warn!("Frame clock correction {sequence} refused: {error}");
		}
	}
}

fn report(applied: Option<u32>) -> Message {
	Message::SteerableReport {
		steerable: applied.is_some(),
		last_correction: applied.unwrap_or(0),
	}
}

fn correction(message: Message) -> Option<(u32, FrameClockCorrection)> {
	match message {
		Message::Correction {
			sequence,
			period_ns,
			phase_shift_ns,
		} => Some((
			sequence,
			FrameClockCorrection {
				period: Duration::from_nanos(period_ns),
				phase_shift_ns,
			},
		)),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_session_without_a_compositor_clock_reports_not_steerable() {
		let clock = CompositorFrameClock::default();
		clock.apply(Message::Correction {
			sequence: 7,
			period_ns: 8_333_333,
			phase_shift_ns: -250_000,
		});
		assert_eq!(
			clock.report(),
			Message::SteerableReport {
				steerable: false,
				last_correction: 0
			}
		);
	}

	#[test]
	fn a_connected_clock_reports_only_the_sequence_installed_by_the_compositor() {
		assert_eq!(
			report(Some(0)),
			Message::SteerableReport {
				steerable: true,
				last_correction: 0
			}
		);
		assert_eq!(
			report(Some(7)),
			Message::SteerableReport {
				steerable: true,
				last_correction: 7
			}
		);
	}

	#[test]
	fn a_decoded_relative_correction_keeps_its_units_and_sign() {
		let bytes = [
			1, 3, 7, 0, 0, 0, 0x55, 0x28, 0x7f, 0, 0, 0, 0, 0, 0x70, 0x2f, 0xfc, 0xff, 0xff, 0xff, 0xff, 0xff,
		];
		let (sequence, clock) = correction(Message::decode(&bytes).unwrap()).unwrap();
		assert_eq!(sequence, 7);
		assert_eq!(clock.period, Duration::from_nanos(8_333_397));
		assert_eq!(clock.phase_shift_ns, -250_000);
		assert!(
			correction(Message::ClockSyncRequest {
				sequence: 1,
				phone_sent: 0
			})
			.is_none()
		);
	}
}
