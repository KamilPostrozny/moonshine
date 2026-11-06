//! What ended stream sessions leave behind in this process
//! (`moonshine-bench --sessions`).
//!
//! A session's compositor starts an Xwayland child, listens on a Wayland socket,
//! takes Wayland and X11 display locks, puts its per-frame threads on a
//! real-time policy and allocates VRAM. Once the session has ended none of that
//! may remain: Moonshine runs for days, so anything a session leaves grows with
//! every Stream until a restart.

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	fs,
	path::Path,
};

/// What this process (and, for VRAM, its children) holds at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
	/// Live or unreaped `Xwayland` children, by pid.
	pub xwayland: BTreeSet<u32>,
	/// Listening Unix sockets named like a Wayland or X11 display.
	pub display_sockets: BTreeSet<String>,
	/// Display lock files this process holds open or wrote its pid into.
	pub display_locks: BTreeSet<String>,
	/// Threads on a real-time policy (`SCHED_FIFO`, `SCHED_RR`,
	/// `SCHED_DEADLINE`), by thread id, with their names.
	pub realtime_threads: BTreeMap<u32, String>,
	/// VRAM resident for the DRM clients of this process and its children
	/// (a leaked Xwayland holds its own), in bytes.
	pub vram_bytes: u64,
}

impl Snapshot {
	/// Read this process's state from `/proc`.
	pub fn take() -> Self {
		let pid = std::process::id();
		let children = children_of(pid);
		let mut snapshot = Snapshot {
			xwayland: children
				.iter()
				.filter(|(_, name)| *name == "Xwayland")
				.map(|(child, _)| *child)
				.collect(),
			realtime_threads: realtime_threads(),
			..Snapshot::default()
		};

		let mut socket_inodes = HashSet::new();
		for (_, target) in open_files("self") {
			if let Some(inode) = target.strip_prefix("socket:[").and_then(|rest| rest.strip_suffix(']')) {
				if let Ok(inode) = inode.parse::<u64>() {
					socket_inodes.insert(inode);
				}
			} else if is_display_lock(&target) {
				snapshot.display_locks.insert(target);
			}
		}

		// Duplicated descriptors share one DRM client.
		let mut vram_by_client = HashMap::new();
		let processes = std::iter::once("self".to_string()).chain(children.keys().map(u32::to_string));
		for process in processes {
			for (fd, target) in open_files(&process) {
				if target.starts_with("/dev/dri/")
					&& let Some((client, bytes)) = fs::read_to_string(format!("/proc/{process}/fdinfo/{fd}"))
						.ok()
						.and_then(|fdinfo| drm_client_vram(&fdinfo))
				{
					vram_by_client.insert(client, bytes);
				}
			}
		}
		snapshot.vram_bytes = vram_by_client.values().sum();

		if let Ok(table) = fs::read_to_string("/proc/self/net/unix") {
			snapshot.display_sockets = listening_unix_sockets(&table)
				.into_iter()
				.filter(|(inode, path)| socket_inodes.contains(inode) && is_display_socket(path))
				.map(|(_, path)| path)
				.collect();
		}

		// smithay writes its pid into the X11 lock it holds.
		if let Ok(entries) = fs::read_dir("/tmp") {
			for path in entries.filter_map(|entry| Some(entry.ok()?.path())) {
				let name = path.to_string_lossy().into_owned();
				if is_display_lock(&name)
					&& fs::read_to_string(&path).is_ok_and(|owner| owner.trim().parse::<u32>() == Ok(pid))
				{
					snapshot.display_locks.insert(name);
				}
			}
		}
		snapshot
	}

	/// What `self` holds that `baseline` did not, one line each. VRAM counts
	/// once it has grown by more than `vram_slack` bytes.
	pub fn leftovers_since(&self, baseline: &Snapshot, vram_slack: u64) -> Vec<String> {
		let mut leftovers = Vec::new();
		for pid in self.xwayland.difference(&baseline.xwayland) {
			leftovers.push(format!("Xwayland child {pid}"));
		}
		for socket in self.display_sockets.difference(&baseline.display_sockets) {
			leftovers.push(format!("listening socket {socket}"));
		}
		for lock in self.display_locks.difference(&baseline.display_locks) {
			leftovers.push(format!("display lock {lock}"));
		}
		for (tid, name) in &self.realtime_threads {
			if !baseline.realtime_threads.contains_key(tid) {
				leftovers.push(format!("real-time thread {tid} ({name})"));
			}
		}
		if self.vram_bytes > baseline.vram_bytes.saturating_add(vram_slack) {
			leftovers.push(format!(
				"VRAM {} KiB, baseline {} KiB",
				self.vram_bytes / 1024,
				baseline.vram_bytes / 1024
			));
		}
		leftovers
	}
}

/// The open descriptors of `/proc/<process>` and what they point at.
fn open_files(process: &str) -> Vec<(String, String)> {
	let Ok(entries) = fs::read_dir(format!("/proc/{process}/fd")) else {
		return Vec::new();
	};
	entries
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let target = fs::read_link(entry.path()).ok()?;
			Some((
				entry.file_name().to_string_lossy().into_owned(),
				target.to_string_lossy().into_owned(),
			))
		})
		.collect()
}

/// The children of `parent`, zombies included, with their names.
fn children_of(parent: u32) -> BTreeMap<u32, String> {
	let Ok(entries) = fs::read_dir("/proc") else {
		return BTreeMap::new();
	};
	entries
		.filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<u32>().ok())
		.filter_map(|pid| {
			let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
			let (name, fields) = stat_fields(&stat)?;
			let ppid = fields.get(PPID)?.parse::<u32>().ok()?;
			(ppid == parent).then(|| (pid, name.to_string()))
		})
		.collect()
}

/// This process's threads on a real-time policy.
fn realtime_threads() -> BTreeMap<u32, String> {
	let Ok(tasks) = fs::read_dir("/proc/self/task") else {
		return BTreeMap::new();
	};
	tasks
		.filter_map(|task| {
			let task = task.ok()?;
			let tid = task.file_name().to_str()?.parse::<u32>().ok()?;
			let stat = fs::read_to_string(task.path().join("stat")).ok()?;
			let (name, policy) = name_and_policy(&stat)?;
			is_realtime(policy).then(|| (tid, name.to_string()))
		})
		.collect()
}

/// Index of the parent pid among the fields after the name (field 4).
const PPID: usize = 1;
/// Index of the scheduling policy among the fields after the name (field 41).
const POLICY: usize = 38;

/// The name in a `/proc/<pid>/stat` line and the fields after it, from the
/// state (field 3) on. The name may itself hold spaces and parentheses.
fn stat_fields(stat: &str) -> Option<(&str, Vec<&str>)> {
	let (open, close) = (stat.find('(')?, stat.rfind(')')?);
	Some((
		stat.get(open + 1..close)?,
		stat.get(close + 1..)?.split_whitespace().collect(),
	))
}

/// A thread's name and scheduling policy from its `stat` line.
fn name_and_policy(stat: &str) -> Option<(&str, u32)> {
	let (name, fields) = stat_fields(stat)?;
	Some((name, fields.get(POLICY)?.parse().ok()?))
}

/// `SCHED_FIFO`, `SCHED_RR` and `SCHED_DEADLINE`.
fn is_realtime(policy: u32) -> bool {
	matches!(policy, 1 | 2 | 6)
}

/// A DRM descriptor's client id and resident VRAM in bytes, from its
/// `/proc/<pid>/fdinfo/<fd>` (DRM usage stats).
fn drm_client_vram(fdinfo: &str) -> Option<(u64, u64)> {
	let field = |key: &str| {
		fdinfo.lines().find_map(|line| {
			let (name, value) = line.split_once(':')?;
			(name.trim() == key).then(|| value.trim())
		})
	};
	let client = field("drm-client-id")?.parse().ok()?;
	// `drm-memory-vram` is the older name of `drm-resident-vram`.
	let vram = field("drm-resident-vram").or_else(|| field("drm-memory-vram"))?;
	Some((client, memory_bytes(vram)?))
}

/// A DRM usage-stats amount: a number with an optional `KiB`/`MiB`/`GiB` unit.
fn memory_bytes(value: &str) -> Option<u64> {
	let mut parts = value.split_whitespace();
	let amount: u64 = parts.next()?.parse().ok()?;
	let scale = match parts.next() {
		None => 1,
		Some("KiB") => 1 << 10,
		Some("MiB") => 1 << 20,
		Some("GiB") => 1 << 30,
		Some(_) => return None,
	};
	amount.checked_mul(scale)
}

/// Listening sockets in a `/proc/net/unix` table: inode and path (abstract
/// names start with `@`).
fn listening_unix_sockets(table: &str) -> Vec<(u64, String)> {
	/// `__SO_ACCEPTCON` in the flags column.
	const LISTENING: u32 = 0x0001_0000;
	table
		.lines()
		.skip(1)
		.filter_map(|line| {
			// Num RefCount Protocol Flags Type St Inode [Path]
			let fields: Vec<&str> = line.split_whitespace().collect();
			let flags = u32::from_str_radix(fields.get(3)?, 16).ok()?;
			let inode = fields.get(6)?.parse().ok()?;
			let path = fields.get(7)?;
			(flags & LISTENING != 0).then(|| (inode, path.to_string()))
		})
		.collect()
}

/// A Wayland display socket (`wayland-N`) or an X11 one (`.X11-unix/XN`).
fn is_display_socket(path: &str) -> bool {
	let name = file_name(path);
	numbered(name, "wayland-", "") || (path.contains(".X11-unix/") && numbered(name, "X", ""))
}

/// A Wayland display lock (`wayland-N.lock`) or an X11 one (`/tmp/.XN-lock`).
fn is_display_lock(path: &str) -> bool {
	let name = file_name(path);
	numbered(name, "wayland-", ".lock") || numbered(name, ".X", "-lock")
}

fn file_name(path: &str) -> &str {
	Path::new(path.trim_start_matches('@'))
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("")
}

/// `name` is `prefix`, a display number, then `suffix`.
fn numbered(name: &str, prefix: &str, suffix: &str) -> bool {
	name.strip_prefix(prefix)
		.and_then(|rest| rest.strip_suffix(suffix))
		.is_some_and(|number| !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A thread's `stat` line with `name` and `policy` (other fields zero).
	fn stat_line(tid: u32, name: &str, policy: u32) -> String {
		let mut fields = vec!["0".to_string(); 50];
		fields[0] = "S".to_string();
		fields[POLICY] = policy.to_string();
		format!("{tid} ({name}) {}", fields.join(" "))
	}

	#[test]
	fn the_policy_is_field_41_of_a_real_stat_line() {
		// A shell under SCHED_IDLE (5).
		let stat = "1116090 (omp) S 24574 1116090 24574 34816 1116090 4194304 6216147 309072085 369621 \
		            1159576 33907 7860 1108999 93443 36 16 110 0 11291671 166308773888 241675 \
		            18446744073709551615 18445056 94228176 140724979156496 0 0 0 0 16781312 1744848635 0 0 0 \
		            17 0 0 5 0 0 0 94232272 94363424 752254976 140724979161470 140724979161499 \
		            140724979161499 140724979163115 0";
		assert_eq!(name_and_policy(stat), Some(("omp", 5)));
		assert!(!is_realtime(5));
	}

	#[test]
	fn round_robin_fifo_and_deadline_threads_are_real_time() {
		assert_eq!(
			name_and_policy(&stat_line(7, "Shm dropping th", 2)),
			Some(("Shm dropping th", 2))
		);
		assert_eq!(
			name_and_policy(&stat_line(8, "a (weird) name", 1)),
			Some(("a (weird) name", 1))
		);
		assert!([1, 2, 6].into_iter().all(is_realtime));
		assert!(![0, 3, 5].into_iter().any(is_realtime));
	}

	#[test]
	fn vram_is_read_with_its_unit_from_the_resident_count() {
		let fdinfo = "pos:\t0\nflags:\t02100002\ndrm-driver:\tamdgpu\ndrm-client-id:\t8640\n\
		              drm-total-vram:\t1667012 KiB\ndrm-resident-vram:\t32 MiB\ndrm-memory-vram:\t1 KiB\n";
		assert_eq!(drm_client_vram(fdinfo), Some((8640, 32 << 20)));
		// Older kernels only have the deprecated name; a bare number is bytes.
		assert_eq!(
			drm_client_vram("drm-client-id:\t3\ndrm-memory-vram:\t4096\n"),
			Some((3, 4096))
		);
		assert_eq!(drm_client_vram("pos:\t0\nflags:\t02\n"), None, "not a DRM descriptor");
		assert_eq!(memory_bytes("2 GiB"), Some(2 << 30));
		assert_eq!(memory_bytes("2 TB"), None);
	}

	#[test]
	fn listening_display_sockets_are_told_from_others() {
		let table = "Num       RefCount Protocol Flags    Type St Inode Path\n\
		             0000000000000000: 00000002 00000000 00010000 0001 01 101 /run/user/1000/wayland-3\n\
		             0000000000000000: 00000003 00000000 00000000 0001 03 102 /run/user/1000/wayland-3\n\
		             0000000000000000: 00000002 00000000 00010000 0001 01 103 @/tmp/.X11-unix/X4\n\
		             0000000000000000: 00000002 00000000 00010000 0001 01 104 /run/user/1000/bus\n\
		             0000000000000000: 00000002 00000000 00010000 0001 01 105\n";
		let listening = listening_unix_sockets(table);
		assert_eq!(
			listening,
			vec![
				(101, "/run/user/1000/wayland-3".to_string()),
				(103, "@/tmp/.X11-unix/X4".to_string()),
				(104, "/run/user/1000/bus".to_string()),
			]
		);
		let display: Vec<_> = listening.iter().filter(|(_, path)| is_display_socket(path)).collect();
		assert_eq!(display.len(), 2);
		assert!(!is_display_socket("/run/user/1000/wayland-3.lock"));
		assert!(!is_display_socket("/run/user/1000/wayland-"));
	}

	#[test]
	fn display_locks_are_named_by_their_display_number() {
		assert!(is_display_lock("/run/user/1000/wayland-12.lock"));
		assert!(is_display_lock("/tmp/.X3-lock"));
		assert!(!is_display_lock("/tmp/.X3-lock.bak"));
		assert!(!is_display_lock("/run/user/1000/wayland-1"));
		assert!(!is_display_lock("/tmp/.Xa-lock"));
	}

	#[test]
	fn leftovers_name_what_the_baseline_lacked() {
		let baseline = Snapshot {
			realtime_threads: BTreeMap::from([(10, "pipewire".to_string())]),
			vram_bytes: 64 << 20,
			..Snapshot::default()
		};
		assert!(baseline.leftovers_since(&baseline, 0).is_empty());

		let after = Snapshot {
			xwayland: BTreeSet::from([4321]),
			display_sockets: BTreeSet::from(["/run/user/1000/wayland-2".to_string()]),
			display_locks: BTreeSet::from(["/tmp/.X5-lock".to_string()]),
			realtime_threads: BTreeMap::from([(10, "pipewire".to_string()), (11, "ferriscope-comp".to_string())]),
			vram_bytes: (64 << 20) + (40 << 20),
		};
		assert_eq!(
			after.leftovers_since(&baseline, 8 << 20),
			vec![
				"Xwayland child 4321".to_string(),
				"listening socket /run/user/1000/wayland-2".to_string(),
				"display lock /tmp/.X5-lock".to_string(),
				"real-time thread 11 (ferriscope-comp)".to_string(),
				"VRAM 106496 KiB, baseline 65536 KiB".to_string(),
			]
		);

		let within_slack = Snapshot {
			vram_bytes: (64 << 20) + (8 << 20),
			..baseline.clone()
		};
		assert!(within_slack.leftovers_since(&baseline, 8 << 20).is_empty());
	}
}
