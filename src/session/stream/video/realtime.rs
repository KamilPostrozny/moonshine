//! Real-time scheduling for the threads every video frame passes through (moonlight-ios fork).
//!
//! A Game that saturates the CPU otherwise delays the compositor, the video pipeline and the
//! encoder's readback, which all run at the default policy: `SCHED_RR` puts them ahead of every
//! normal thread whatever its nice. Each of them runs for well under a frame per frame, and the
//! kernel's real-time throttling (`sched_rt_runtime_us`) still bounds them. Measured on a Host
//! under a CPU and GPU load, promoting these three cut the frame latency's p95; promoting every
//! thread of the process (the tokio workers too) didn't, and nice -11 alone didn't either.
//!
//! Needs `RLIMIT_RTPRIO` of at least the priority (systemd's user manager passes on the limit of
//! the user's login, 99 for the `audio` group on Arch and CachyOS); without it the threads keep
//! their policy and a warning is logged.

/// Name prefixes of the per-frame threads, as Linux keeps them (15 bytes): ferriscope's
/// compositor thread and the thread it starts, the video pipeline (capture, convert, encode,
/// packetize) and pixelforge's encode readback.
pub const STREAM_THREADS: &[&str] = &["ferriscope-comp", "video-pipeline", "pixelforge-enco"];

/// Their `SCHED_RR` priority: above none of the system's own real-time threads (PipeWire uses 88,
/// kernel IRQ threads 50).
pub const STREAM_PRIORITY: i32 = 10;

/// Moves the per-frame threads that exist now to `SCHED_RR`, and logs the outcome. The video
/// pipeline calls it once its encoder (and with it the readback thread) exists.
pub fn promote_stream_threads() {
	let outcome = promote_threads(STREAM_THREADS, STREAM_PRIORITY);
	let promoted: Vec<&str> = outcome
		.iter()
		.filter(|(_, result)| result.is_ok())
		.map(|(name, _)| name.as_str())
		.collect();
	if promoted.is_empty() {
		tracing::warn!(priority = STREAM_PRIORITY, "No Stream thread on SCHED_RR.");
	} else {
		tracing::info!(
			priority = STREAM_PRIORITY,
			?promoted,
			"{} Stream threads on SCHED_RR.",
			promoted.len()
		);
	}
	for (name, result) in &outcome {
		if let Err(e) = result {
			tracing::warn!("Failed to put thread {name} on SCHED_RR {STREAM_PRIORITY}: {e}");
		}
	}
}

/// Moves this process's threads whose name starts with one of `prefixes` to `SCHED_RR` at
/// `priority`, and returns each matched thread's name with the outcome.
pub fn promote_threads(prefixes: &[&str], priority: i32) -> Vec<(String, std::io::Result<()>)> {
	let tasks = match std::fs::read_dir("/proc/self/task") {
		Ok(tasks) => tasks,
		Err(e) => return vec![("/proc/self/task".to_string(), Err(e))],
	};
	let param = libc::sched_param {
		sched_priority: priority,
	};
	let mut outcome = Vec::new();
	for task in tasks.flatten() {
		let Some(tid) = task
			.file_name()
			.to_str()
			.and_then(|tid| tid.parse::<libc::pid_t>().ok())
		else {
			continue;
		};
		// A thread that has exited since the listing has no comm any more.
		let Ok(comm) = std::fs::read_to_string(task.path().join("comm")) else {
			continue;
		};
		let name = comm.trim_end_matches('\n');
		if !prefixes.iter().any(|prefix| name.starts_with(prefix)) {
			continue;
		}
		// The thread ID as the pid: sched_setscheduler acts on that one thread.
		let result = if unsafe { libc::sched_setscheduler(tid, libc::SCHED_RR, &param) } == 0 {
			Ok(())
		} else {
			Err(std::io::Error::last_os_error())
		};
		outcome.push((name.to_string(), result));
	}
	outcome
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::mpsc;

	fn policy(tid: libc::pid_t) -> libc::c_int {
		unsafe { libc::sched_getscheduler(tid) }
	}

	/// A named thread that reports its thread ID and parks until `stop` drops.
	fn named_thread(name: &str) -> (libc::pid_t, mpsc::Sender<()>, std::thread::JoinHandle<()>) {
		let (tid_tx, tid_rx) = mpsc::channel();
		let (stop_tx, stop_rx) = mpsc::channel::<()>();
		let handle = std::thread::Builder::new()
			.name(name.to_string())
			.spawn(move || {
				tid_tx.send(unsafe { libc::gettid() }).unwrap();
				let _ = stop_rx.recv();
			})
			.unwrap();
		(tid_rx.recv().unwrap(), stop_tx, handle)
	}

	#[test]
	fn promotes_only_threads_with_a_matching_name() {
		// Linux keeps 15 bytes of a thread's name: match by prefix.
		let (matched, stop_matched, matched_handle) = named_thread("rt-promote-me-please");
		let (other, stop_other, other_handle) = named_thread("rt-leave-me");
		// What the test runner gave them (SCHED_IDLE under some shells).
		let before = policy(other);
		assert_eq!(policy(matched), before);

		let outcome = promote_threads(&["rt-promote-me"], 10);

		assert_eq!(outcome.len(), 1, "{outcome:?}");
		assert_eq!(outcome[0].0, "rt-promote-me-p");
		match &outcome[0].1 {
			Ok(()) => assert_eq!(policy(matched), libc::SCHED_RR),
			// No RLIMIT_RTPRIO here: the thread keeps its policy.
			Err(e) => {
				assert_eq!(e.raw_os_error(), Some(libc::EPERM));
				assert_eq!(policy(matched), before);
			},
		}
		assert_eq!(policy(other), before);

		drop((stop_matched, stop_other));
		matched_handle.join().unwrap();
		other_handle.join().unwrap();
	}
}
