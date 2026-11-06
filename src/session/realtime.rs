//! Real-time scheduling for the Stream's own threads (moonlight-ios fork).
//!
//! A Game that saturates the CPU otherwise delays the threads every video frame and every audio
//! packet passes through, which run at the default policy: `SCHED_RR` puts them ahead of every
//! normal thread whatever its nice. Each of them runs for well under a frame per frame, and the
//! kernel's real-time throttling (`sched_rt_runtime_us`) and the unit's `RLIMIT_RTTIME`, if set, bound them.
//! Measured on a Host under a CPU and GPU load, promoting the video threads cut the frame latency's
//! p95; promoting every thread of the process (the tokio workers too) didn't, and nice -11 alone
//! didn't either. With only the video threads promoted, the audio threads starved under the same
//! load (20.9 phone underruns a minute).
//!
//! Each thread is registered by its thread ID when it is spawned, never found by name: a thread's
//! name is inherited by the threads it starts, and a thread left over from an earlier Stream can
//! carry it too. The policy is set with `SCHED_RESET_ON_FORK`, so the threads and processes a
//! registered thread starts (Xwayland, smithay's and the Vulkan driver's threads) start at the
//! default policy.
//!
//! The registered threads, each once per Stream:
//! - `ferriscope-compositor` (comm `ferriscope-comp`): renders and delivers every frame.
//! - `video-pipeline`: capture, colour conversion and encode submit.
//! - `pixelforge-encode-readback` (comm `pixelforge-enco`): waits for each encode and reads it back.
//! - `video-send`: awaits each encoded frame, strips the filler, adds FEC, packetizes and sends.
//! - `pulse-server`: the Game's audio clock and mixer.
//! - `audio-encode`: Opus, FEC and encryption of each audio packet.
//!
//! The control loop and the gamepad thread are not registered.
//!
//! Needs `RLIMIT_RTPRIO` of at least the priority (systemd's user manager passes on the limit of
//! the user's login, 99 for the `audio` group on Arch and CachyOS); without it a thread keeps its
//! policy and a warning is logged.

/// The Stream threads' `SCHED_RR` priority: above none of the system's own real-time threads
/// (PipeWire uses 88, kernel IRQ threads 50).
pub const STREAM_PRIORITY: i32 = 10;

/// The calling thread's Linux thread ID.
pub fn current_thread_id() -> libc::pid_t {
	unsafe { libc::gettid() }
}

/// Registers the Stream thread `tid`, called `name` in the log, when it is spawned: puts that one
/// thread on `SCHED_RR` at [`STREAM_PRIORITY`] with `SCHED_RESET_ON_FORK`, and logs the outcome.
pub fn register(name: &str, tid: libc::pid_t) -> std::io::Result<()> {
	let param = libc::sched_param {
		sched_priority: STREAM_PRIORITY,
	};
	// The thread ID as the pid: sched_setscheduler acts on that one thread.
	let result = if unsafe { libc::sched_setscheduler(tid, libc::SCHED_RR | libc::SCHED_RESET_ON_FORK, &param) } == 0 {
		Ok(())
	} else {
		Err(std::io::Error::last_os_error())
	};
	match &result {
		Ok(()) => tracing::info!(tid, priority = STREAM_PRIORITY, "Stream thread {name} on SCHED_RR."),
		Err(e) => tracing::warn!(
			tid,
			"Failed to put Stream thread {name} on SCHED_RR {STREAM_PRIORITY}, it keeps its policy: {e}"
		),
	}
	result
}

/// Registers the calling thread, which the Stream just spawned.
pub fn register_current(name: &str) -> std::io::Result<()> {
	register(name, current_thread_id())
}

/// Registers a Stream thread a dependency spawned and reported by its ID (ferriscope's compositor,
/// pixelforge's readback). One that reported none can't be registered, which is logged.
pub fn register_reported(name: &str, tid: Option<u32>) -> std::io::Result<()> {
	match tid.map(libc::pid_t::try_from) {
		Some(Ok(tid)) => register(name, tid),
		_ => {
			tracing::warn!("Stream thread {name} reported no thread ID, it keeps its policy.");
			Err(std::io::Error::other("no thread ID"))
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::mpsc;

	fn policy(tid: libc::pid_t) -> libc::c_int {
		unsafe { libc::sched_getscheduler(tid) }
	}

	/// Whether this process may put a thread on `SCHED_RR` at [`STREAM_PRIORITY`]: an
	/// `RLIMIT_RTPRIO` that high, and a test runner that didn't start the threads on `SCHED_IDLE`
	/// (leaving that also depends on `RLIMIT_NICE`).
	fn may_promote() -> bool {
		let mut limit = libc::rlimit {
			rlim_cur: 0,
			rlim_max: 0,
		};
		let read = unsafe { libc::getrlimit(libc::RLIMIT_RTPRIO, &mut limit) } == 0;
		read && limit.rlim_cur >= STREAM_PRIORITY as libc::rlim_t && policy(current_thread_id()) != libc::SCHED_IDLE
	}

	/// A named thread that runs `body`, reports its thread ID and what `body` returned, and parks
	/// until the returned sender drops.
	fn named_thread<T: Send + 'static>(
		name: &str,
		body: impl FnOnce() -> T + Send + 'static,
	) -> (libc::pid_t, T, mpsc::Sender<()>, std::thread::JoinHandle<()>) {
		let (report_tx, report_rx) = mpsc::channel();
		let (stop_tx, stop_rx) = mpsc::channel::<()>();
		let handle = std::thread::Builder::new()
			.name(name.to_string())
			.spawn(move || {
				report_tx.send((current_thread_id(), body())).unwrap();
				let _ = stop_rx.recv();
			})
			.unwrap();
		let (tid, report) = report_rx.recv().unwrap();
		(tid, report, stop_tx, handle)
	}

	#[test]
	fn promotes_the_registered_thread_only() {
		// Two threads with one name: only the one registered by its ID is promoted.
		let (registered, (), stop_registered, registered_handle) = named_thread("rt-stream", || ());
		let (namesake, (), stop_namesake, namesake_handle) = named_thread("rt-stream", || ());
		// What the test runner gave them (SCHED_IDLE under some shells).
		let before = policy(namesake);
		assert_eq!(policy(registered), before);

		match register("rt-stream", registered) {
			Ok(()) => assert_eq!(policy(registered), libc::SCHED_RR | libc::SCHED_RESET_ON_FORK),
			// No RLIMIT_RTPRIO here: the thread keeps its policy.
			Err(e) => {
				assert!(!may_promote(), "promotion failed although permitted: {e}");
				assert_eq!(e.raw_os_error(), Some(libc::EPERM));
				assert_eq!(policy(registered), before);
			},
		}
		assert_eq!(policy(namesake), before);

		drop((stop_registered, stop_namesake));
		registered_handle.join().unwrap();
		namesake_handle.join().unwrap();
	}

	#[test]
	fn threads_a_registered_thread_starts_are_not_promoted() {
		let (parent, (promoted, child_policy), stop_parent, parent_handle) = named_thread("rt-parent", || {
			let promoted = register_current("rt-parent").is_ok();
			// A thread the registered one starts afterwards, like Xwayland or a driver thread.
			let child_policy = std::thread::spawn(|| policy(current_thread_id())).join().unwrap();
			(promoted, child_policy)
		});

		if promoted {
			assert_eq!(policy(parent), libc::SCHED_RR | libc::SCHED_RESET_ON_FORK);
			assert_eq!(child_policy, libc::SCHED_OTHER);
		} else {
			assert!(!may_promote(), "promotion failed although permitted");
			// Unpromoted, the child simply inherits the parent's policy.
			assert_eq!(child_policy, policy(parent));
		}

		drop(stop_parent);
		parent_handle.join().unwrap();
	}

	#[test]
	fn a_thread_reported_without_an_id_is_not_registered() {
		assert!(register_reported("rt-unreported", None).is_err());
	}
}
