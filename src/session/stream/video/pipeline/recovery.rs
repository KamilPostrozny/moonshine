//! The client's recovery requests (stream resets, keyframe requests and
//! reference frame invalidations) and the encoded frame each one applies to.

use tokio::sync::broadcast::{self, error::TryRecvError};

/// The encoder's side of a recovery request.
pub(super) trait RecoveryEncoder {
	/// Make the next frame a keyframe.
	fn request_idr(&mut self);
	/// Stop predicting from frames at or after `first_lost_display_order`.
	fn invalidate_reference_frames(&mut self, first_lost_display_order: u64);
}

impl RecoveryEncoder for pixelforge::Encoder {
	fn request_idr(&mut self) {
		pixelforge::Encoder::request_idr(self);
	}

	fn invalidate_reference_frames(&mut self, first_lost_display_order: u64) {
		pixelforge::Encoder::invalidate_reference_frames(self, first_lost_display_order);
	}
}

/// What a wait for the next frame found queued.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Drained {
	/// A stream reset's epoch. The packet consumer restarts its frame numbering
	/// there, before the next frame.
	pub reset: Option<u32>,
	/// Whether a keyframe was asked for (a reset asks for one too).
	pub keyframe: bool,
}

/// Applies the client's recovery requests to the encoder.
///
/// Requests are drained after each wait for a frame, so one that arrives
/// during the wait applies to the frame the wait returns. Each loss gets one
/// recovery frame: an invalidation that repeats the one still waiting for its
/// recovery frame, or that reports only frames older than the last keyframe
/// or recovery frame, is ignored.
pub(super) struct RecoveryRequests {
	resets: broadcast::Receiver<u32>,
	keyframes: broadcast::Receiver<()>,
	invalidations: broadcast::Receiver<(u32, u32)>,
	/// The display order (pixelforge's `pts`) of the next submitted frame.
	next_display_order: u64,
	/// The display order of the current epoch's first frame. The packet
	/// consumer numbers the frames it sends from 1 within an epoch, and the
	/// display order advances once per submitted frame, so client frame `n` is
	/// display order `epoch_start + n - 1`. A rare readback drop can skew this
	/// by a frame, which at worst over-invalidates into a (safe) IDR.
	epoch_start: u64,
	/// A keyframe was asked for and no frame has been submitted since.
	keyframe_pending: bool,
	/// The first lost frame of an applied invalidation whose recovery frame
	/// hasn't been submitted yet.
	invalidated_from: Option<u64>,
	/// The last frame submitted as a keyframe or a recovery frame. A loss
	/// reported only before it is recovered already.
	recovered_at: Option<u64>,
}

impl RecoveryRequests {
	pub fn new(
		resets: broadcast::Receiver<u32>,
		keyframes: broadcast::Receiver<()>,
		invalidations: broadcast::Receiver<(u32, u32)>,
	) -> Self {
		Self {
			resets,
			keyframes,
			invalidations,
			next_display_order: 0,
			epoch_start: 0,
			keyframe_pending: false,
			invalidated_from: None,
			recovered_at: None,
		}
	}

	/// Waits for the next frame with `wait`, then applies every request queued
	/// until it returned to `encoder`.
	pub fn wait_for_frame<T>(&mut self, encoder: &mut impl RecoveryEncoder, wait: impl FnOnce() -> T) -> (T, Drained) {
		let frame = wait();
		(frame, self.drain(encoder))
	}

	/// Records a submitted frame. Returns whether it is a recovery frame: the
	/// first after an invalidation, which the client must see marked as one to
	/// stop dropping frames.
	pub fn submitted(&mut self) -> bool {
		let display_order = self.next_display_order;
		self.next_display_order += 1;
		let recovery = self.invalidated_from.take().is_some();
		let keyframe = std::mem::take(&mut self.keyframe_pending);
		if recovery || keyframe {
			self.recovered_at = Some(display_order);
		}
		recovery
	}

	fn drain(&mut self, encoder: &mut impl RecoveryEncoder) -> Drained {
		let mut drained = Drained::default();

		// Moonlight starts every (re)connected session with its frame counter at 1
		// and waits for an IDR. Because we keep the pipeline running across a resume,
		// `frame_number` keeps climbing, so a reconnecting client would see the first
		// frame's index as a huge jump from its expected 1 — counted as ~100% frame
		// loss within its sampling window, which immediately trips CONN_STATUS_POOR
		// ("Your network connection isn't performing well"). Reset the frame and RTP
		// sequence counters and force an IDR so the resumed client sees a clean stream
		// starting from frame 1.
		loop {
			match self.resets.try_recv() {
				Ok(epoch) => drained.reset = Some(epoch),
				// The newest epoch is still queued; the next try_recv returns it.
				Err(TryRecvError::Lagged(_)) => continue,
				Err(TryRecvError::Closed | TryRecvError::Empty) => break,
			}
		}
		if drained.reset.is_some() {
			tracing::info!("Resetting video frame counter for resumed client and forcing IDR.");
			// The next submitted frame is the consumer's frame 1 of the new epoch.
			self.epoch_start = self.next_display_order;
			drained.keyframe = true;
		}

		loop {
			match self.keyframes.try_recv() {
				Ok(()) => {
					tracing::debug!("IDR frame requested");
					drained.keyframe = true;
				},
				Err(TryRecvError::Lagged(n)) => {
					tracing::debug!("IDR frame channel lagged by {n} messages.");
					drained.keyframe = true;
				},
				Err(TryRecvError::Closed | TryRecvError::Empty) => break,
			}
		}

		// Each invalidation takes effect on the next submitted frame, which then
		// predicts from a surviving reference instead of forcing an IDR (the
		// encoder still falls back to an IDR when no reference survives).
		loop {
			match self.invalidations.try_recv() {
				Ok((first, last)) => self.invalidate(encoder, first, last),
				Err(TryRecvError::Lagged(n)) => {
					// Missed some loss reports; force an IDR to be safe.
					tracing::debug!("Invalidation channel lagged by {n} messages; forcing IDR.");
					drained.keyframe = true;
				},
				Err(TryRecvError::Closed | TryRecvError::Empty) => break,
			}
		}

		if drained.keyframe {
			encoder.request_idr();
			self.keyframe_pending = true;
		}
		drained
	}

	/// Applies an invalidation of client frames `first` to `last`, unless a
	/// frame already submitted or about to be recovers them. The client asks
	/// again for every frame it drops while it waits for the recovery frame,
	/// with the same `first`; it asks with a `last` at or after the recovery
	/// frame only if it lost that too.
	fn invalidate(&mut self, encoder: &mut impl RecoveryEncoder, first: u32, last: u32) {
		let first_lost = self.display_order(first);
		let last_lost = self.display_order(last.max(first));
		if let Some(recovered_at) = self.recovered_at
			&& last_lost < recovered_at
		{
			tracing::debug!(
				"Ignoring invalidation of client frames {first}-{last}: display order {recovered_at} already recovers them"
			);
			return;
		}
		if self.invalidated_from.is_some_and(|from| from <= first_lost) {
			tracing::debug!("Ignoring repeated invalidation from client frame {first}");
			return;
		}
		tracing::debug!(
			"Reference frame invalidation requested from client frame {first} (display order {first_lost})"
		);
		encoder.invalidate_reference_frames(first_lost);
		self.invalidated_from = Some(first_lost);
	}

	fn display_order(&self, client_frame: u32) -> u64 {
		self.epoch_start + (u64::from(client_frame.max(1)) - 1)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	enum Applied {
		Keyframe,
		Invalidation(u64),
	}

	#[derive(Default)]
	struct Encoder(Vec<Applied>);

	impl RecoveryEncoder for Encoder {
		fn request_idr(&mut self) {
			self.0.push(Applied::Keyframe);
		}

		fn invalidate_reference_frames(&mut self, first_lost_display_order: u64) {
			self.0.push(Applied::Invalidation(first_lost_display_order));
		}
	}

	struct Client {
		resets: broadcast::Sender<u32>,
		keyframes: broadcast::Sender<()>,
		invalidations: broadcast::Sender<(u32, u32)>,
	}

	impl Client {
		fn invalidate(&self, first: u32, last: u32) {
			self.invalidations.send((first, last)).unwrap();
		}
	}

	/// The encoding loop's view: requests, the encoder they reach, and the
	/// client sending them.
	struct Loop {
		requests: RecoveryRequests,
		encoder: Encoder,
		client: Client,
	}

	impl Loop {
		fn new() -> Self {
			let (resets, resets_rx) = broadcast::channel(8);
			let (keyframes, keyframes_rx) = broadcast::channel(8);
			let (invalidations, invalidations_rx) = broadcast::channel(8);
			Self {
				requests: RecoveryRequests::new(resets_rx, keyframes_rx, invalidations_rx),
				encoder: Encoder::default(),
				client: Client {
					resets,
					keyframes,
					invalidations,
				},
			}
		}

		/// Waits for a frame, the client sending what `during` sends meanwhile,
		/// then submits the frame. Returns whether it went out as a recovery frame.
		fn frame(&mut self, during: impl FnOnce(&Client)) -> bool {
			let client = &self.client;
			self.requests.wait_for_frame(&mut self.encoder, || during(client));
			self.requests.submitted()
		}

		/// Submits client frames until `n` was the last.
		fn frames_up_to(&mut self, n: u32) {
			while self.requests.next_display_order < u64::from(n) {
				self.frame(|_| {});
			}
		}

		fn applied(&mut self) -> Vec<Applied> {
			std::mem::take(&mut self.encoder.0)
		}
	}

	#[test]
	fn an_invalidation_queued_during_the_frame_wait_applies_to_that_frame() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(2);

		// Client frame 2 (display order 1) is lost while the loop waits for frame 3.
		let recovery = pipeline.frame(|client| client.invalidate(2, 2));

		assert!(recovery, "frame 3 is the recovery frame");
		assert_eq!(pipeline.applied(), [Applied::Invalidation(1)]);
	}

	#[test]
	fn a_keyframe_or_reset_queued_during_the_frame_wait_applies_to_that_frame() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(2);

		let (_, drained) = pipeline.requests.wait_for_frame(&mut pipeline.encoder, || {
			pipeline.client.keyframes.send(()).unwrap();
		});
		assert_eq!(
			drained,
			Drained {
				reset: None,
				keyframe: true
			}
		);
		assert_eq!(pipeline.applied(), [Applied::Keyframe]);

		let (_, drained) = pipeline.requests.wait_for_frame(&mut pipeline.encoder, || {
			pipeline.client.resets.send(7).unwrap();
		});
		assert_eq!(
			drained,
			Drained {
				reset: Some(7),
				keyframe: true
			}
		);
		assert_eq!(pipeline.applied(), [Applied::Keyframe]);
	}

	#[test]
	fn a_repeated_invalidation_gets_no_second_recovery_frame() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(3);

		// The client asks twice for the same loss before the recovery frame...
		let recovery = pipeline.frame(|client| {
			client.invalidate(2, 3);
			client.invalidate(2, 3);
		});
		assert!(recovery, "frame 4 is the recovery frame");
		assert_eq!(pipeline.applied(), [Applied::Invalidation(1)]);

		// ...and again for a frame sent before the recovery frame reached it.
		let recovery = pipeline.frame(|client| client.invalidate(2, 3));
		assert!(!recovery, "frame 5 predicts from the recovery frame");
		assert_eq!(pipeline.applied(), []);
	}

	#[test]
	fn a_lost_recovery_frame_gets_another_one() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(3);
		assert!(pipeline.frame(|client| client.invalidate(2, 3)));
		pipeline.applied();

		// Frame 4, the recovery frame, is lost too: the client still waits from frame 2.
		let recovery = pipeline.frame(|client| client.invalidate(2, 4));

		assert!(recovery, "frame 5 recovers again");
		assert_eq!(pipeline.applied(), [Applied::Invalidation(1)]);
	}

	#[test]
	fn an_invalidation_older_than_the_last_keyframe_is_ignored() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(3);
		// Frame 4 is a keyframe.
		pipeline.frame(|client| {
			client.keyframes.send(()).unwrap();
		});
		pipeline.applied();

		// A loss of frames 2-3, reported after the keyframe went out.
		let recovery = pipeline.frame(|client| client.invalidate(2, 3));
		assert!(!recovery);
		assert_eq!(pipeline.applied(), []);

		// A loss that includes the keyframe still applies.
		let recovery = pipeline.frame(|client| client.invalidate(4, 5));
		assert!(recovery);
		assert_eq!(pipeline.applied(), [Applied::Invalidation(3)]);
	}

	#[test]
	fn client_frames_count_from_the_last_reset() {
		let mut pipeline = Loop::new();
		pipeline.frames_up_to(5);
		// The resumed client's frame 1 is display order 5.
		pipeline.frame(|client| {
			client.resets.send(1).unwrap();
		});
		pipeline.frame(|_| {});
		pipeline.applied();

		pipeline.frame(|client| client.invalidate(2, 2));

		assert_eq!(pipeline.applied(), [Applied::Invalidation(6)]);
	}
}
