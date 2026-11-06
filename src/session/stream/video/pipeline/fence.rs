//! Waiting for the compositor's acquire fence before reading a frame, and for
//! GPU work before releasing what it reads.

use std::os::fd::RawFd;
use std::time::{Duration, Instant};

use ash::vk;
use pixelforge::TimelinePoint;

/// How long the pipeline waits for the compositor's acquire fence. A frame whose
/// render has not finished after this is a stuck GPU, not a slow frame.
pub(super) const FENCE_TIMEOUT: Duration = Duration::from_secs(1);

/// Wait until a sync file fd signals (becomes readable).
///
/// Returns `Err` if it did not signal within `timeout` or `poll(2)` failed.
pub(super) fn wait_sync_file(fd: RawFd, timeout: Duration) -> Result<(), String> {
	let deadline = Instant::now() + timeout;
	loop {
		let remaining = deadline.saturating_duration_since(Instant::now());
		let mut pollfd = libc::pollfd {
			fd,
			events: libc::POLLIN,
			revents: 0,
		};
		let millis = remaining.as_millis().min(i32::MAX as u128) as libc::c_int;
		// SAFETY: `pollfd` is a valid, initialised struct and the count is 1.
		let result = unsafe { libc::poll(&mut pollfd, 1, millis) };
		match result {
			1.. => return Ok(()),
			0 => return Err(format!("acquire fence did not signal within {timeout:?}")),
			_ => {
				let error = std::io::Error::last_os_error();
				if error.kind() != std::io::ErrorKind::Interrupted {
					return Err(format!("poll on acquire fence failed: {error}"));
				}
			},
		}
	}
}

/// Wait until the GPU reaches `point`, for at most `timeout`.
pub(super) fn wait_timeline_point(
	device: &ash::Device,
	point: TimelinePoint,
	timeout: Duration,
) -> Result<(), vk::Result> {
	let semaphores = [point.semaphore];
	let values = [point.value];
	let info = vk::SemaphoreWaitInfo::default().semaphores(&semaphores).values(&values);
	// SAFETY: pixelforge keeps the point's timeline semaphore alive as long as its context.
	unsafe { device.wait_semaphores(&info, timeout.as_nanos().min(u128::from(u64::MAX)) as u64) }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn wait_sync_file_returns_once_readable() {
		// A pipe with data is "signalled"; an empty one is not.
		let mut fds = [0 as RawFd; 2];
		assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
		let (read, write) = (fds[0], fds[1]);
		assert!(wait_sync_file(read, Duration::from_millis(20)).is_err());
		assert_eq!(unsafe { libc::write(write, [1u8].as_ptr().cast(), 1) }, 1);
		assert!(wait_sync_file(read, Duration::from_millis(20)).is_ok());
		unsafe {
			libc::close(read);
			libc::close(write);
		}
	}
}
