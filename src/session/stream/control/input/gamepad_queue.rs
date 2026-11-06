//! The gamepad thread's queue of events from the control loop.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc};

use super::InputEvent;
use crate::session::stream::control::FeedbackCommand;

/// A gamepad event, and where a gamepad it creates sends its feedback.
pub(super) type GamepadCommand = (InputEvent, mpsc::Sender<FeedbackCommand>);

/// A queue from the control loop to the gamepad thread that never makes the control loop wait,
/// so a gamepad being created or a starved gamepad thread can't hold up keyboard, mouse and
/// acknowledgements. Updates, motion samples and battery reports carry a pad's whole state: when
/// the queue is full, a newer one replaces every pending one of its kind for its pad (the latest
/// state wins). Arrivals, touchpad and haptics events keep their place, and are refused only when
/// the queue is full of them.
pub(super) fn gamepad_queue(capacity: usize) -> (GamepadSender, GamepadReceiver) {
	let shared = Arc::new(Shared {
		state: Mutex::new(State {
			queue: VecDeque::with_capacity(capacity),
			closed: false,
		}),
		ready: Notify::new(),
		capacity,
	});
	(GamepadSender(shared.clone()), GamepadReceiver(shared))
}

struct Shared {
	state: Mutex<State>,
	ready: Notify,
	capacity: usize,
}

struct State {
	queue: VecDeque<GamepadCommand>,
	/// The sender is gone.
	closed: bool,
}

/// The queue is full of events that carry no gamepad state.
#[derive(Debug)]
pub(super) struct Full;

pub(super) struct GamepadSender(Arc<Shared>);

impl GamepadSender {
	/// Queue `event` without waiting.
	pub fn try_send(&self, event: InputEvent, feedback: mpsc::Sender<FeedbackCommand>) -> Result<(), Full> {
		{
			let mut state = self.0.state.lock().unwrap();
			if state.queue.len() >= self.0.capacity {
				keep_latest_states(&mut state.queue, state_of(&event));
			}
			if state.queue.len() >= self.0.capacity {
				return Err(Full);
			}
			state.queue.push_back((event, feedback));
		}
		self.0.ready.notify_one();
		Ok(())
	}
}

/// The gamepad state an event carries whole, which a newer event of the same kind replaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GamepadState {
	/// Buttons, sticks and triggers of a pad.
	Update {
		pad: u16,
	},
	/// One motion sensor of a pad.
	Motion {
		pad: u8,
		sensor: u8,
	},
	Battery {
		pad: u8,
	},
}

fn state_of(event: &InputEvent) -> Option<GamepadState> {
	match event {
		InputEvent::GamepadUpdate(update) => Some(GamepadState::Update { pad: update.index }),
		InputEvent::GamepadMotion(motion) => Some(GamepadState::Motion {
			pad: motion.index,
			sensor: motion.sensor(),
		}),
		InputEvent::GamepadBattery(battery) => Some(GamepadState::Battery { pad: battery.index }),
		_ => None,
	}
}

/// Drop every queued state that a later one (or `incoming`, about to be queued) replaces.
fn keep_latest_states(queue: &mut VecDeque<GamepadCommand>, incoming: Option<GamepadState>) {
	let mut newer: Vec<GamepadState> = incoming.into_iter().collect();
	let keep: Vec<bool> = queue
		.iter()
		.rev()
		.map(|(event, _)| match state_of(event) {
			Some(state) if newer.contains(&state) => false,
			Some(state) => {
				newer.push(state);
				true
			},
			None => true,
		})
		.collect();
	let mut keep = keep.into_iter().rev();
	queue.retain(|_| keep.next().unwrap_or(true));
}

impl Drop for GamepadSender {
	fn drop(&mut self) {
		self.0.state.lock().unwrap().closed = true;
		self.0.ready.notify_one();
	}
}

pub(super) struct GamepadReceiver(Arc<Shared>);

impl GamepadReceiver {
	/// The next event, oldest first; `None` once the sender is gone and the queue is empty.
	pub async fn recv(&mut self) -> Option<GamepadCommand> {
		loop {
			{
				let mut state = self.0.state.lock().unwrap();
				if let Some(command) = state.queue.pop_front() {
					return Some(command);
				}
				if state.closed {
					return None;
				}
			}
			self.0.ready.notified().await;
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn event(event_type: u32, body: &[u8]) -> InputEvent {
		let mut bytes = event_type.to_le_bytes().to_vec();
		bytes.extend(body);
		InputEvent::from_bytes(&bytes).unwrap()
	}

	/// Pad `index`'s whole state, with `buttons` pressed and every pad up to it active.
	fn update(index: u16, buttons: u16) -> InputEvent {
		let mut body = [0u8; 26];
		body[2..4].copy_from_slice(&index.to_le_bytes());
		body[4..6].copy_from_slice(&(u16::MAX >> (15 - index)).to_le_bytes());
		body[8..10].copy_from_slice(&buttons.to_le_bytes());
		event(0x0000_000C, &body)
	}

	/// Pad `index` arrives as an Xbox pad.
	fn arrival(index: u8) -> InputEvent {
		event(0x5500_0004, &[index, 0x01, 0, 0, 0, 0, 0, 0])
	}

	/// A touchpad event on pad `index`.
	fn touch(index: u8) -> InputEvent {
		let mut body = [0u8; 20];
		body[0] = index;
		event(0x5500_0005, &body)
	}

	/// Each queued event, oldest first, described.
	async fn drained(mut receiver: GamepadReceiver) -> Vec<String> {
		let mut events = Vec::new();
		while let Some((event, _)) = receiver.recv().await {
			events.push(match event {
				InputEvent::GamepadUpdate(update) => format!("pad {} buttons {}", update.index, update.button_flags()),
				InputEvent::GamepadInfo(info) => format!("pad {} arrives", info.index),
				InputEvent::GamepadTouch(touch) => format!("pad {} touch", touch.index),
				other => format!("{other:?}"),
			});
		}
		events
	}

	#[tokio::test]
	async fn a_full_queue_takes_a_newer_state_without_waiting_and_the_latest_state_wins() {
		let (sender, receiver) = gamepad_queue(4);
		let (feedback, _feedback_rx) = mpsc::channel(1);
		for buttons in 1..=10 {
			assert!(sender.try_send(update(0, buttons), feedback.clone()).is_ok());
		}
		drop(sender);
		let events = drained(receiver).await;
		assert!(events.len() <= 4, "{events:?}");
		assert_eq!(events.last().map(String::as_str), Some("pad 0 buttons 10"));
	}

	#[tokio::test]
	async fn a_full_queue_keeps_arrivals_and_other_pads_states_in_order() {
		let (sender, receiver) = gamepad_queue(4);
		let (feedback, _feedback_rx) = mpsc::channel(1);
		assert!(sender.try_send(arrival(1), feedback.clone()).is_ok());
		assert!(sender.try_send(update(1, 5), feedback.clone()).is_ok());
		for buttons in 1..=10 {
			assert!(sender.try_send(update(0, buttons), feedback.clone()).is_ok());
		}
		drop(sender);
		let events = drained(receiver).await;
		assert_eq!(events[..2], ["pad 1 arrives", "pad 1 buttons 5"], "{events:?}");
		assert_eq!(events.last().map(String::as_str), Some("pad 0 buttons 10"));
	}

	#[tokio::test]
	async fn a_queue_full_of_touches_refuses_more_without_waiting() {
		let (sender, receiver) = gamepad_queue(2);
		let (feedback, _feedback_rx) = mpsc::channel(1);
		assert!(sender.try_send(touch(0), feedback.clone()).is_ok());
		assert!(sender.try_send(touch(0), feedback.clone()).is_ok());
		assert!(sender.try_send(touch(0), feedback.clone()).is_err());
		assert!(sender.try_send(update(0, 1), feedback.clone()).is_err());
		drop(sender);
		assert_eq!(drained(receiver).await, ["pad 0 touch", "pad 0 touch"]);
	}
}
