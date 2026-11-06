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
	///
	/// `steerable` means the compositor's frame clock is connected to this session,
	/// not that its thread is alive: a stalled compositor still reports steerable,
	/// and the client sees it as corrections that are never acknowledged. A steerable
	/// report carries the compositor's nominal period, which the client sizes its
	/// corrections from (the compositor keeps its launch rate across a resume).
	pub(crate) fn report(&self) -> Message {
		report(
			self.0
				.get()
				.map(|sender| (sender.last_applied(), sender.nominal_period())),
		)
	}

	/// A new client connected: its correction sequences start over, so neither the
	/// last client's acknowledgement nor its queued corrections may reach it.
	pub(crate) fn new_client(&self) {
		if let Some(sender) = self.0.get() {
			sender.new_consumer();
		}
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

/// The report for a clock that installed `applied` last at its `nominal` period, or for none.
fn report(clock: Option<(u32, Duration)>) -> Message {
	Message::SteerableReport {
		steerable: clock.is_some(),
		last_correction: clock.map_or(0, |(applied, _)| applied),
		nominal_period_ns: clock.map(|(_, nominal)| nominal.as_nanos() as u64),
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
				last_correction: 0,
				nominal_period_ns: None,
			}
		);
	}

	#[test]
	fn a_connected_clock_reports_the_sequence_installed_and_the_compositors_nominal_period() {
		assert_eq!(
			report(Some((0, Duration::from_nanos(8_333_333)))),
			Message::SteerableReport {
				steerable: true,
				last_correction: 0,
				nominal_period_ns: Some(8_333_333),
			}
		);
		assert_eq!(
			report(Some((7, Duration::from_nanos(16_666_667)))),
			Message::SteerableReport {
				steerable: true,
				last_correction: 7,
				nominal_period_ns: Some(16_666_667),
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
