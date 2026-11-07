use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Duration;

use async_shutdown::ShutdownManager;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use zbus::proxy::SignalStream;
use zbus::{Connection, MatchRule, MessageStream, Proxy};
use zvariant::OwnedObjectPath;

pub fn default_launch_timeout() -> u64 {
	2
}

/// Configuration for a single application that can be launched in a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationConfig {
	/// Title of the application.
	pub title: String,

	/// Path to a boxart image.
	pub boxart: Option<PathBuf>,

	/// The command to run.
	pub command: Vec<String>,

	/// Commands to run before launching the application.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub pre_command: Vec<Vec<String>>,

	/// Commands to run after the streaming session ends.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub post_command: Vec<Vec<String>>,

	/// systemd StandardOutput value. If not set, defaults to "null".
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdout: Option<String>,

	/// systemd StandardError value. If not set, defaults to "null".
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stderr: Option<String>,

	/// Seconds to wait for the application to reach an active state after launch.
	#[serde(default = "default_launch_timeout")]
	pub launch_timeout_secs: u64,

	/// Steam integration, equivalent to gamescope's `-e`: Steam chooses which
	/// window is shown. Turn it on for Steam itself (Big Picture), so its
	/// overlay and focus contract work; leave it off (the default) for anything
	/// not launched through Steam, as with it on only Steam and Steam game
	/// windows can take focus. Games from the Steam scanner have it on.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub steam_integration: bool,

	/// Extra environment variables for the application. They override the
	/// variables Moonshine sets, such as `DISPLAY`.
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: BTreeMap<String, String>,
}

impl Default for ApplicationConfig {
	fn default() -> Self {
		Self {
			title: String::new(),
			boxart: None,
			command: Vec::new(),
			pre_command: Vec::new(),
			post_command: Vec::new(),
			stdout: None,
			stderr: None,
			launch_timeout_secs: default_launch_timeout(),
			steam_integration: false,
			env: BTreeMap::new(),
		}
	}
}

impl ApplicationConfig {
	pub fn id(&self) -> i32 {
		let mut hasher = DefaultHasher::new();
		self.title.hash(&mut hasher);
		// Clients only accept non-negative application IDs.
		(hasher.finish() as i32) & i32::MAX
	}
}

use crate::session::manager::SessionShutdownReason;

const SYSTEMD_BUS: &str = "org.freedesktop.systemd1";
const SYSTEMD_PATH: &str = "/org/freedesktop/systemd1";
const SYSTEMD_MANAGER: &str = "org.freedesktop.systemd1.Manager";

const PROPERTIES_INTERFACE: &str = "org.freedesktop.DBus.Properties";
const PROPERTIES_CHANGED: &str = "PropertiesChanged";
const UNIT_INTERFACE: &str = "org.freedesktop.systemd1.Unit";
const ACTIVE_STATE_PROPERTY: &str = "ActiveState";

/// The Game unit's stop timeout (`TimeoutStopUSec`), which systemd gives each stop step
/// (`ExecStop`, then `SIGTERM` to what is left) before the next.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

/// A Steam application's stop timeout: Steam's own shutdown writes cloud saves and its config
/// before it exits, which takes longer than a Game's exit.
const STEAM_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long Moonshine waits for a stop job: longer than any Game unit's stop timeout, so the
/// compositor, which stays until the Game's unit has stopped, outlives a Steam still shutting down.
const STOP_JOB_TIMEOUT: Duration = Duration::from_secs(STEAM_STOP_TIMEOUT.as_secs() + 5);
const _: () = assert!(STOP_JOB_TIMEOUT.as_secs() > STEAM_STOP_TIMEOUT.as_secs());
const _: () = assert!(STOP_JOB_TIMEOUT.as_secs() > STOP_TIMEOUT.as_secs());

const UNIT_REMOVED_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest a dropped `Application` takes to stop its unit: the stop job, then the unit's
/// removal. The compositor stays that long after a session's stop.
pub(crate) const GAME_STOP_TIMEOUT: Duration =
	Duration::from_secs(STOP_JOB_TIMEOUT.as_secs() + UNIT_REMOVED_TIMEOUT.as_secs());

/// Variables of the desktop session that the user manager passes to every unit it starts: the
/// desktop compositor's displays and IPC sockets, and the desktop's identity. The Game's unit
/// unsets the ones Moonshine doesn't set itself, so a Stream Game can't reach the desktop's
/// compositor (unsetting them in Moonshine's own environment doesn't reach the unit). The session
/// bus stays: the Stream shares the user's, and clients find it at `$XDG_RUNTIME_DIR/bus` anyway.
const DESKTOP_ENVIRONMENT: &[&str] = &[
	"WAYLAND_DISPLAY",
	"DISPLAY",
	"XAUTHORITY",
	"NIRI_SOCKET",
	"SWAYSOCK",
	"I3SOCK",
	"HYPRLAND_INSTANCE_SIGNATURE",
	"XDG_CURRENT_DESKTOP",
	"XDG_SESSION_DESKTOP",
	"DESKTOP_STARTUP_ID",
	"XDG_ACTIVATION_TOKEN",
];

/// Start-job wait (includes `ExecStartPre`), decoupled from `launch_timeout_secs` so a `pre_command` isn't cut off.
const START_JOB_TIMEOUT: Duration = Duration::from_secs(90);

/// systemd only emits JobRemoved/UnitRemoved/PropertiesChanged bus signals to
/// connections that have called org.freedesktop.systemd1.Manager.Subscribe().
/// Without it, waiting for those signals only works when some *other* client
/// on the user bus happens to hold a subscription. Call this once for every
/// new session bus connection, before waiting on any systemd signals.
async fn subscribe_to_systemd_signals(conn: &Connection) -> Result<(), ()> {
	conn.call_method(Some(SYSTEMD_BUS), SYSTEMD_PATH, Some(SYSTEMD_MANAGER), "Subscribe", &())
		.await
		.map_err(|e| tracing::error!("Failed to subscribe to systemd signals: {e}"))?;
	Ok(())
}

#[derive(Clone)]
pub(crate) struct LaunchOptions<'a> {
	pub unit_name: &'a str,
	pub program: &'a str,
	pub args: &'a [String],
	pub envs: &'a [String],
	pub timeout: Duration,
	pub pre_commands: &'a Vec<Vec<String>>,
	pub post_commands: &'a Vec<Vec<String>>,
	pub stdout_value: &'a Option<String>,
	pub stderr_value: &'a Option<String>,
	/// A Steam application (`steam_integration`): stopped with Steam's own shutdown and
	/// `STEAM_STOP_TIMEOUT`.
	pub steam: bool,
}

/// Runtime context required to launch an application.
pub(crate) struct ApplicationContext {
	/// systemd transient unit name (e.g. `"moonshine-session.service"`).
	pub unit_name: String,
	/// Path to the PulseAudio socket created by the audio stream.
	pub pulse_socket_path: PathBuf,
	/// X11 display number reported by XWayland (e.g. `0` → `":0"`).
	pub xdisplay: u32,
	/// Wayland socket name reported by the compositor.
	pub wayland_display: String,
	/// The compositor's environment for the application (see
	/// `CompositorReady::env`).
	pub compositor_env: Vec<(String, String)>,
	/// Environment variables to pass on.
	pub extra_env: HashMap<String, String>,
}

pub(crate) struct Application {
	unit_name: String,
	config: ApplicationConfig,
	exit_monitor: Option<JoinHandle<()>>,
	/// Dropped only once the unit has stopped, after `drop`: its receiver (the compositor) then
	/// knows the Game no longer needs its display.
	unit_stopped: Option<oneshot::Sender<()>>,
}

impl Application {
	/// `unit_stopped` is dropped once the application's unit has stopped, or with the error if the
	/// launch fails.
	pub async fn spawn(
		config: ApplicationConfig,
		context: ApplicationContext,
		stop: ShutdownManager<SessionShutdownReason>,
		unit_stopped: oneshot::Sender<()>,
	) -> Result<Self, ()> {
		let Some(program) = config.command.first() else {
			tracing::error!("Application command is empty.");
			return Err(());
		};
		let args = &config.command[1..];
		let envs = make_envs(&context, &config.env)?;

		tracing::info!(program, ?args, "Launching application.");

		// Connect to the user session bus.
		let conn = Connection::session()
			.await
			.map_err(|e| tracing::error!("Failed to connect to session bus: {e}"))?;
		subscribe_to_systemd_signals(&conn).await?;

		// Stop any leftover unit from a previous session.
		let _ = stop_unit(&conn, &context.unit_name).await;

		// Launch the application as a transient systemd service unit.
		let options = LaunchOptions {
			unit_name: &context.unit_name,
			program,
			args,
			envs: &envs,
			timeout: Duration::from_secs(config.launch_timeout_secs),
			pre_commands: &config.pre_command,
			post_commands: &config.post_command,
			stdout_value: &config.stdout,
			stderr_value: &config.stderr,
			steam: config.steam_integration,
		};

		let unit_path = match start_transient_service(&conn, &options).await {
			Ok(unit_path) => unit_path,
			Err(_) => {
				// Best effort cleanup on launch failure.
				stop_unit(&conn, &context.unit_name).await.ok();
				return Err(());
			},
		};
		let exit_monitor = spawn_unit_exit_monitor(conn.clone(), context.unit_name.clone(), unit_path, stop);

		Ok(Self {
			unit_name: context.unit_name,
			config,
			exit_monitor: Some(exit_monitor),
			unit_stopped: Some(unit_stopped),
		})
	}
}

impl Drop for Application {
	fn drop(&mut self) {
		tracing::info!("Application '{}' is exiting.", self.config.title);
		if let Some(handle) = self.exit_monitor.take() {
			handle.abort();
		}

		let Some(unit_stopped) = self.unit_stopped.take() else {
			return;
		};
		// Unfortunately we have no `drop_async` yet, so the stop gets a runtime of its own. It runs
		// on its own thread: a Steam's shutdown takes up to `STEAM_STOP_TIMEOUT`, and the session
		// manager drops sessions on its async workers.
		let unit_name = self.unit_name.clone();
		stop_in_background(unit_stopped, move || match tokio::runtime::Runtime::new() {
			Ok(rt) => {
				rt.block_on(stop_unit_owned(unit_name)).ok();
			},
			Err(e) => tracing::error!("Failed to build a runtime to stop the Game's unit: {e}"),
		});
	}
}

/// Runs `stop` on a thread of its own and drops `unit_stopped` once it has returned. Returns at
/// once.
fn stop_in_background(unit_stopped: oneshot::Sender<()>, stop: impl FnOnce() + Send + 'static) {
	let spawned = std::thread::Builder::new()
		.name("game-stop".to_string())
		.spawn(move || {
			stop();
			drop(unit_stopped);
		});
	// The closure, `unit_stopped` with it, is dropped: the compositor stops now, and the next
	// launch stops the leftover unit before it starts its own.
	if let Err(e) = spawned {
		tracing::error!("Failed to start a thread to stop the Game's unit: {e}");
	}
}

/// Build environment variables for the application based on the context (e.g. display, PulseAudio socket).
fn make_envs(context: &ApplicationContext, config_env: &BTreeMap<String, String>) -> Result<Vec<String>, ()> {
	let pulse_runtime_path = context
		.pulse_socket_path
		.parent()
		.ok_or_else(|| tracing::error!("Failed to get parent directory of PulseAudio socket."))?
		.to_string_lossy();
	// Build environment variables as "KEY=value" strings for systemd.
	let mut envs: Vec<String> = vec![
		format!("PULSE_SERVER=unix:{}", context.pulse_socket_path.display()),
		format!("PULSE_RUNTIME_PATH={pulse_runtime_path}"),
		// Native PipeWire clients (SDL's default backend, pipewire-alsa, pipewire-jack) ignore
		// PULSE_SERVER. They look for `$PIPEWIRE_RUNTIME_DIR/$PIPEWIRE_REMOTE`, which doesn't
		// exist next to Moonshine's Pulse socket, so they can't reach the desktop's PipeWire:
		// SDL then falls back to Pulse, and audio reaches only the Stream.
		format!("PIPEWIRE_RUNTIME_DIR={pulse_runtime_path}"),
		"PIPEWIRE_REMOTE=pipewire-0".to_string(),
		format!("DISPLAY=:{}", context.xdisplay),
		format!("WAYLAND_DISPLAY={}", context.wayland_display),
		// Force Proton to use winepulse.drv instead of winepipewire.drv,
		// so it respects PULSE_SERVER and routes audio through Moonshine.
		"PROTON_USE_PIPEWIRE=0".to_string(),
	];

	// Forced sync: a GL Game with VSync off never waits for a frame callback, so ferriscope's
	// steered tick can't pace it and it renders unbounded. Mesa's `vblank_mode=3` gives its GL a
	// swap interval of at least 1 whatever the Game sets (Wayland EGL and GLX through Xwayland);
	// NVIDIA's proprietary GL reads `__GL_SYNC_TO_VBLANK`. Pushed before the compositor's and the
	// application's variables, so the application's own values (and Steam launch options, which
	// Steam applies later) win. The opt-out is ferriscope-wsi's `FERRISCOPE_WSI_HOLD` (`fifo`,
	// `off`, `0` or `false`, parsed by `ferriscope::WsiHold` as the layer parses it) in the
	// application's `env`: `off` is the one switch that turns off both the layer's hold and forced
	// sync. Any other value is rejected and the application isn't launched.
	if forces_sync(config_env)? {
		envs.push("vblank_mode=3".to_string());
		envs.push("__GL_SYNC_TO_VBLANK=1".to_string());
	}

	// ferriscope's environment: the gamescope identity Steam uses for its
	// external-overlay mode, the ferriscope-wsi layer, and the HDR and
	// capability variables it actually backs.
	for (key, value) in &context.compositor_env {
		envs.push(format!("{key}={value}"));
	}

	for (key, value) in &context.extra_env {
		envs.push(format!("{key}={value}"));
	}

	// The application's own variables come last, so they win.
	for (key, value) in config_env {
		envs.push(format!("{key}={value}"));
	}

	Ok(last_assignments(envs))
}

/// `envs` with only the last assignment of each name, in order: systemd's StartTransientUnit
/// rejects an `Environment=` block that assigns a name twice ("Invalid environment block"), so a
/// variable Moonshine forces and the application sets again would stop the Game from starting.
fn last_assignments(mut envs: Vec<String>) -> Vec<String> {
	let name = |assignment: &str| {
		assignment
			.split_once('=')
			.map_or(assignment, |(key, _)| key)
			.to_string()
	};
	let mut later = HashSet::new();
	let mut keep: Vec<bool> = envs.iter().rev().map(|entry| later.insert(name(entry))).collect();
	keep.reverse();
	let mut keep = keep.into_iter();
	envs.retain(|_| keep.next().unwrap_or(true));
	envs
}

/// Whether a Game gets forced sync: not when its environment opts out of ferriscope-wsi's hold,
/// read with the layer's own parser. An unrecognised value is an error, never "hold everything".
fn forces_sync(config_env: &BTreeMap<String, String>) -> Result<bool, ()> {
	let hold = config_env.get(ferriscope::WsiHold::VARIABLE).map(String::as_str);
	let hold = ferriscope::WsiHold::parse(hold)
		.map_err(|rejected| tracing::error!("Application env {rejected}; not launching the application."))?;
	Ok(hold == ferriscope::WsiHold::Every)
}

/// Wait for a `JobRemoved` signal matching the given job path, accepting only `"done"` as success.
async fn wait_for_job_signal(
	job_stream: &mut SignalStream<'_>,
	job_path: &OwnedObjectPath,
	timeout: Duration,
	label: &str,
) -> Result<(), ()> {
	let result = tokio::time::timeout(timeout, async {
		while let Some(message) = job_stream.next().await {
			let body: (u32, OwnedObjectPath, String, String) = message
				.body()
				.deserialize()
				.map_err(|e| tracing::error!("Failed to deserialize JobRemoved signal: {e}"))?;
			let (_, ref path, ref unit, ref result) = body;

			if path != job_path {
				continue;
			}

			return match result.as_str() {
				"done" => Ok(()),
				other => {
					tracing::warn!(result = other, unit = unit, "{label} job finished unsuccessfully.");
					Err(())
				},
			};
		}
		Err(())
	})
	.await;

	match result {
		Ok(Ok(())) => Ok(()),
		Ok(Err(())) => {
			tracing::warn!(label, "Received failure result for {label} job.");
			Err(())
		},
		Err(_) => {
			tracing::warn!(timeout_secs = timeout.as_secs(), "Timed out waiting for {label} job.");
			Err(())
		},
	}
}

/// Stop a unit via the user session bus.
///
/// Waits for the stop job to complete and the unit to be removed, with a timeout.
async fn stop_unit(conn: &Connection, unit_name: &str) -> Result<(), ()> {
	// Subscribe to both JobRemoved and UnitRemoved before calling StopUnit to avoid races.
	let proxy = Proxy::new(conn, SYSTEMD_BUS, SYSTEMD_PATH, SYSTEMD_MANAGER)
		.await
		.map_err(|e| tracing::error!("Failed to create systemd proxy: {e}"))?;
	let mut job_removed_stream = proxy
		.receive_signal("JobRemoved")
		.await
		.map_err(|e| tracing::error!("Failed to subscribe to JobRemoved signals: {e}"))?;
	let mut unit_removed_stream = proxy
		.receive_signal("UnitRemoved")
		.await
		.map_err(|e| tracing::error!("Failed to subscribe to UnitRemoved signals: {e}"))?;

	// Call StopUnit, which queues a stop job but does not wait for it to complete.
	let reply = conn
		.call_method(
			Some(SYSTEMD_BUS),
			SYSTEMD_PATH,
			Some(SYSTEMD_MANAGER),
			"StopUnit",
			&(unit_name, "replace"),
		)
		.await
		.map_err(|e| match e {
			zbus::Error::MethodError(ref err_name, ..)
				if err_name.as_str() == "org.freedesktop.systemd1.NoSuchUnit" =>
			{
				tracing::debug!("Unit was already stopped.");
			},
			e => {
				tracing::error!("Failed to get unit: {e}");
			},
		})?;

	let job_path = reply
		.body()
		.deserialize()
		.map_err(|e| tracing::warn!("Failed to deserialize StopUnit reply for {unit_name}: {e}"))?;

	// Wait for JobRemoved with result "done" — the stop job completed.
	wait_for_job_signal(&mut job_removed_stream, &job_path, STOP_JOB_TIMEOUT, "Stop").await?;

	// Stop job succeeded — wait for the unit to be collected and removed.
	let unit_result = tokio::time::timeout(UNIT_REMOVED_TIMEOUT, async {
		while let Some(message) = unit_removed_stream.next().await {
			let (id, _path): (String, OwnedObjectPath) = message
				.body()
				.deserialize()
				.map_err(|e| tracing::error!("Failed to deserialize UnitRemoved signal: {e}"))?;

			if id == unit_name {
				return Ok(());
			}
		}
		Err(())
	})
	.await;

	match unit_result {
		Ok(Ok(())) => {
			tracing::debug!("Leftover unit {unit_name} stopped and unloaded.");
			Ok(())
		},
		Ok(Err(())) => Err(()),
		Err(_) => {
			tracing::warn!(
				timeout_secs = UNIT_REMOVED_TIMEOUT.as_secs(),
				unit = unit_name,
				"Timed out waiting for leftover unit to be removed."
			);
			Err(())
		},
	}
}

/// Stop a unit with a new session bus connection.
async fn stop_unit_owned(unit_name: String) -> Result<(), ()> {
	let conn = Connection::session().await.map_err(|e| {
		tracing::error!("Failed to connect to session bus: {e}");
	})?;
	subscribe_to_systemd_signals(&conn).await?;
	stop_unit(&conn, &unit_name).await
}

fn spawn_unit_exit_monitor(
	conn: Connection,
	unit_name: String,
	unit_path: OwnedObjectPath,
	stop: ShutdownManager<SessionShutdownReason>,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		tokio::select! {
			state = wait_for_unit_terminal_state(&conn, &unit_name, &unit_path) => {
				match state {
					Ok(state) => {
						tracing::info!(unit = unit_name, state, "Application unit exited; stopping session.");
						let _ = stop.trigger_shutdown(SessionShutdownReason::ApplicationStopped);
					},
					Err(()) => {
						tracing::warn!(unit = unit_name, "Application unit monitor stopped unexpectedly.");
					},
				}
			},
			_ = stop.wait_shutdown_triggered() => {},
		}
	})
}

async fn wait_for_unit_terminal_state(
	conn: &Connection,
	unit_name: &str,
	unit_path: &OwnedObjectPath,
) -> Result<String, ()> {
	let proxy = Proxy::new(conn, SYSTEMD_BUS, SYSTEMD_PATH, SYSTEMD_MANAGER)
		.await
		.map_err(|e| tracing::error!("Failed to create systemd proxy: {e}"))?;
	let mut unit_removed_stream = proxy
		.receive_signal("UnitRemoved")
		.await
		.map_err(|e| tracing::error!("Failed to subscribe to UnitRemoved signals: {e}"))?;

	let rule = MatchRule::builder()
		.msg_type(zbus::message::Type::Signal)
		.sender(SYSTEMD_BUS)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.path(unit_path)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.interface(PROPERTIES_INTERFACE)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.member(PROPERTIES_CHANGED)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.build();
	let mut state_stream = MessageStream::for_match_rule(rule, conn, None)
		.await
		.map_err(|e| tracing::error!("Failed to create state stream: {e}"))?;

	match current_unit_state(conn, unit_path).await {
		Ok(state) => {
			if let Some(state) = terminal_state(state.as_str()) {
				return Ok(state.to_string());
			}
		},
		Err(()) => return Ok("removed".to_string()),
	}

	loop {
		tokio::select! {
			message = state_stream.next() => {
				let Some(Ok(signal)) = message else {
					return Err(());
				};
				let body = signal.body();
				let (iface, changed, _): (String, HashMap<String, zvariant::Value<'_>>, Vec<String>) =
					body.deserialize()
						.map_err(|e| tracing::error!("Failed to deserialize PropertiesChanged signal: {e}"))?;
				if iface != UNIT_INTERFACE {
					continue;
				}
				if let Some(zvariant::Value::Str(state)) = changed.get(ACTIVE_STATE_PROPERTY)
					&& let Some(state) = terminal_state(state.as_str()) {
						return Ok(state.to_string());
					}
			},
			message = unit_removed_stream.next() => {
				let Some(message) = message else {
					return Err(());
				};
				let (id, _path): (String, OwnedObjectPath) = message
					.body()
					.deserialize()
					.map_err(|e| tracing::error!("Failed to deserialize UnitRemoved signal: {e}"))?;
				if id == unit_name {
					// Reloading systemd emits UnitRemoved even for running units.
					match current_unit_state(conn, unit_path).await {
						Ok(state) => {
							if let Some(state) = terminal_state(state.as_str()) {
								return Ok(state.to_string());
							}
						},
						Err(()) => return Ok("removed".to_string()),
					}
				}
			},
		}
	}
}

async fn current_unit_state(conn: &Connection, unit_path: &OwnedObjectPath) -> Result<String, ()> {
	let reply = conn
		.call_method(
			Some(SYSTEMD_BUS),
			unit_path,
			Some(PROPERTIES_INTERFACE),
			"Get",
			&(UNIT_INTERFACE, ACTIVE_STATE_PROPERTY),
		)
		.await
		.map_err(|e| tracing::error!("Failed to get unit state: {e}"))?;

	let body = reply.body();
	let (variant,): (zvariant::Value<'_>,) = body.deserialize().map_err(|e| {
		tracing::error!("Failed to deserialize unit state: {e}");
	})?;
	match variant {
		zvariant::Value::Str(state) => Ok(state.to_string()),
		_ => Ok("unknown".to_string()),
	}
}

fn terminal_state(state: &str) -> Option<&'static str> {
	match state {
		"inactive" => Some("inactive"),
		"failed" => Some("failed"),
		_ => None,
	}
}

/// Split a systemd `StandardOutput`/`StandardError` setting into the enum value
/// and, for path-based settings, the companion path property.
///
/// Unit files accept `StandardOutput=file:/path`, but over D-Bus
/// (StartTransientUnit) the path must go in a separate property:
/// `StandardOutput=file` + `StandardOutputFile=/path`. Sending the combined
/// `file:/path` string is rejected with "Invalid StandardOutput setting".
/// Mirrors systemd's `bus_append_standard_inputs()` in bus-unit-util.c.
fn split_standard_io(value: &Option<String>) -> (String, Option<(&'static str, String)>) {
	let Some(v) = value.as_deref() else {
		return ("null".to_string(), None);
	};
	for (prefix, prop) in [
		("file:", "File"),
		("append:", "FileToAppend"),
		("truncate:", "FileToTruncate"),
		("fd:", "FileDescriptorName"),
	] {
		if let Some(path) = v.strip_prefix(prefix) {
			return (v[..prefix.len() - 1].to_string(), Some((prop, path.to_string())));
		}
	}
	(v.to_string(), None)
}

/// One systemd exec command: (absolute path, argv, ignore failure).
type ExecEntry = (String, Vec<String>, bool);

/// The Game unit's commands, resolved to absolute paths.
struct UnitCommands {
	pre: Vec<ExecEntry>,
	main: ExecEntry,
	/// `ExecStop`: Steam's own shutdown, for a Steam application.
	stop: Option<ExecEntry>,
	post: Vec<ExecEntry>,
}

/// A Steam application's `ExecStop`, run by `sh` with Steam as `$1`: Steam's own shutdown
/// (`steam -shutdown`), so it writes cloud saves and its config, then a wait for the unit's main
/// process (`$MAINPID`, the Steam the unit started), because systemd sends `SIGTERM` to whatever is
/// left as soon as `ExecStop` returns. With the main process already gone it sends nothing:
/// `steam -shutdown` with no Steam running starts one. systemd expands only `${NAME}` inside a
/// word, so `$MAINPID` and `$1` reach `sh` as written.
const STEAM_SHUTDOWN_SCRIPT: &str = r#"[ -n "$MAINPID" ] && kill -0 "$MAINPID" 2>/dev/null || exit 0
"$1" -shutdown
while kill -0 "$MAINPID" 2>/dev/null; do sleep 0.2 || exit 1; done"#;

fn steam_shutdown_entry(shell: String, steam: String) -> ExecEntry {
	let argv = vec![
		shell.clone(),
		"-c".to_string(),
		STEAM_SHUTDOWN_SCRIPT.to_string(),
		"steam-shutdown".to_string(),
		steam,
	];
	(shell, argv, false)
}

/// Resolve `sh` and `steam` for a Steam application's `ExecStop`. Without them it stops with
/// `SIGTERM` after the stop timeout, as any Game.
fn resolve_steam_shutdown() -> Option<ExecEntry> {
	let resolve = |program: &str| {
		which::which(program)
			.ok()
			.and_then(|path| path.to_str().map(str::to_string))
	};
	match (resolve("sh"), resolve("steam")) {
		(Some(shell), Some(steam)) => Some(steam_shutdown_entry(shell, steam)),
		_ => {
			tracing::warn!("No sh or steam in PATH: the Steam application will stop with SIGTERM.");
			None
		},
	}
}

/// The desktop's variables (`DESKTOP_ENVIRONMENT`) that `envs` doesn't assign. Only those are
/// unset: systemd applies `UnsetEnvironment=` after `Environment=`, so it would also remove the
/// Stream's own `DISPLAY` and `WAYLAND_DISPLAY`.
fn desktop_variables_to_unset(envs: &[String]) -> Vec<String> {
	DESKTOP_ENVIRONMENT
		.iter()
		.filter(|name| {
			!envs
				.iter()
				.any(|assignment| assignment.split_once('=').is_some_and(|(key, _)| key == **name))
		})
		.map(|name| name.to_string())
		.collect()
}

/// The properties of the Game's transient unit.
fn unit_properties<'a>(
	options: &'a LaunchOptions<'a>,
	commands: &UnitCommands,
) -> Result<Vec<(String, zvariant::Value<'a>)>, ()> {
	// Split StandardOutput/StandardError into the enum value plus an optional
	// companion path property. Unit-file syntax (`StandardOutput=file:/path`) is
	// not accepted over D-Bus; the path must be a separate property
	// (`StandardOutput=file` + `StandardOutputFile=/path`).
	let (stdout_setting, stdout_path) = split_standard_io(options.stdout_value);
	let (stderr_setting, stderr_path) = split_standard_io(options.stderr_value);
	let stop_timeout = if options.steam {
		STEAM_STOP_TIMEOUT
	} else {
		STOP_TIMEOUT
	};

	// Properties: a(sv) — array of (property_name: s, value: v)
	// zvariant::Value has D-Bus type 'v' (variant), so Vec<(String, Value)> serialises as a(sv).
	//
	// IMPORTANT: do NOT use zvariant::Array::from(Vec<Value>) — it always produces `av`
	// (array of variant). Build typed arrays with Array::new(signature) + append() instead.
	let mut properties: Vec<(String, zvariant::Value<'a>)> = vec![
		("Type".to_string(), zvariant::Value::Str("exec".into())),
		("Slice".to_string(), zvariant::Value::Str("moonshine.slice".into())),
		// Environment: as
		("Environment".to_string(), zvariant::Value::from(options.envs.to_vec())),
		// UnsetEnvironment: as
		(
			"UnsetEnvironment".to_string(),
			zvariant::Value::from(desktop_variables_to_unset(options.envs)),
		),
		// ExecStart: a(sasb)
		(
			"ExecStart".to_string(),
			build_exec_array(std::slice::from_ref(&commands.main))?,
		),
		(
			"TimeoutStopUSec".to_string(),
			zvariant::Value::U64(stop_timeout.as_micros() as u64),
		),
		(
			"CollectMode".to_string(),
			zvariant::Value::Str("inactive-or-failed".into()),
		),
		// StandardOutput/StandardError: bare enum value
		(
			"StandardOutput".to_string(),
			zvariant::Value::Str(stdout_setting.into()),
		),
		("StandardError".to_string(), zvariant::Value::Str(stderr_setting.into())),
	];

	// Path-based outputs carry their path in a companion property.
	if let Some((prop, path)) = stdout_path {
		properties.push((format!("StandardOutput{prop}"), zvariant::Value::Str(path.into())));
	}
	if let Some((prop, path)) = stderr_path {
		properties.push((format!("StandardError{prop}"), zvariant::Value::Str(path.into())));
	}

	// Only include ExecStartPre/ExecStop/ExecStopPost when non-empty: an empty a(sasb) array still
	// needs a valid element signature, and omitting absent properties is cleaner.
	if !commands.pre.is_empty() {
		properties.push(("ExecStartPre".to_string(), build_exec_array(&commands.pre)?));
	}
	if let Some(stop) = &commands.stop {
		properties.push(("ExecStop".to_string(), build_exec_array(std::slice::from_ref(stop))?));
	}
	if !commands.post.is_empty() {
		properties.push(("ExecStopPost".to_string(), build_exec_array(&commands.post)?));
	}

	Ok(properties)
}

/// Launch the application as a transient systemd service unit via D-Bus.
async fn start_transient_service(conn: &Connection, options: &LaunchOptions<'_>) -> Result<OwnedObjectPath, ()> {
	// Resolve exec entries in a blocking task — `which::which` does filesystem lookups.
	let commands = tokio::task::spawn_blocking({
		let pre_commands = options.pre_commands.clone();
		let post_commands = options.post_commands.clone();
		let main_program = options.program.to_string();
		let args = options.args.to_vec();
		let steam = options.steam;
		move || -> Result<_, ()> {
			let program_for_error = main_program.clone();
			Ok(UnitCommands {
				pre: build_exec_entries(&pre_commands),
				main: build_exec_entry(main_program, args).ok_or_else(move || {
					tracing::error!("Main program '{}' not found in PATH.", program_for_error);
				})?,
				stop: if steam { resolve_steam_shutdown() } else { None },
				post: build_exec_entries(&post_commands),
			})
		}
	})
	.await
	.map_err(|e| tracing::error!("spawn_blocking panicked: {e}"))??;

	tracing::debug!(
		pre_entries = ?commands.pre,
		main_entry = ?commands.main,
		stop_entry = ?commands.stop,
		post_entries = ?commands.post,
		"Building transient service"
	);

	let properties = unit_properties(options, &commands)?;

	// Aux units: empty a(sa(sv))
	let aux: Vec<(&str, Vec<(&str, zvariant::Value)>)> = Vec::new();

	// Subscribe to JobRemoved before calling StartTransientUnit to avoid a race condition.
	let proxy = Proxy::new(conn, SYSTEMD_BUS, SYSTEMD_PATH, SYSTEMD_MANAGER)
		.await
		.map_err(|e| tracing::error!("Failed to create systemd proxy: {e}"))?;
	let mut job_stream = proxy
		.receive_signal("JobRemoved")
		.await
		.map_err(|e| tracing::error!("Failed to subscribe to JobRemoved signals: {e}"))?;

	// Call StartTransientUnit.
	let reply = conn
		.call_method(
			Some(SYSTEMD_BUS),
			SYSTEMD_PATH,
			Some(SYSTEMD_MANAGER),
			"StartTransientUnit",
			&(options.unit_name, "replace", &properties, &aux),
		)
		.await
		.map_err(|e| tracing::warn!("Failed to start transient service: {e}"))?;

	let (job_path,): (OwnedObjectPath,) = reply
		.body()
		.deserialize()
		.map_err(|e| tracing::warn!("Failed to deserialize StartTransientUnit reply: {e}"))?;

	// Wait for the launch job to complete.
	wait_for_job_signal(&mut job_stream, &job_path, START_JOB_TIMEOUT, "Application launch").await?;

	// Get the unit object path — now that the job is done, the unit should exist.
	let unit_path = conn
		.call_method(
			Some(SYSTEMD_BUS),
			SYSTEMD_PATH,
			Some(SYSTEMD_MANAGER),
			"GetUnit",
			&options.unit_name,
		)
		.await
		.map_err(|e| tracing::error!("Failed to get unit object path: {e}"))?;
	let (path,): (OwnedObjectPath,) = unit_path
		.body()
		.deserialize()
		.map_err(|e| tracing::error!("Failed to deserialize unit object path: {e}"))?;

	// Check current state before subscribing — catches apps that exit immediately.
	let state = current_unit_state(conn, &path).await?;
	if terminal_state(&state).is_some() {
		tracing::warn!(state = state, "Application exited immediately after launch.");
		return Err(());
	}

	// Subscribe to PropertiesChanged on this unit object.
	let rule = MatchRule::builder()
		.msg_type(zbus::message::Type::Signal)
		.sender(SYSTEMD_BUS)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.path(&path)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.interface(PROPERTIES_INTERFACE)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.member(PROPERTIES_CHANGED)
		.map_err(|e| tracing::error!("Failed to create match rule: {e}"))?
		.build();
	let mut state_stream = MessageStream::for_match_rule(rule, conn, None)
		.await
		.map_err(|e| tracing::error!("Failed to create state stream: {e}"))?;

	// Wait for ActiveState to change to a terminal failure state.
	let failure_result = tokio::time::timeout(options.timeout, async {
		while let Some(Ok(signal)) = state_stream.next().await {
			let body = signal.body();
			let (iface, changed, _): (String, HashMap<String, zvariant::Value<'_>>, Vec<String>) =
				body.deserialize()
					.map_err(|e| tracing::error!("Failed to deserialize PropertiesChanged signal: {e}"))?;
			if iface != UNIT_INTERFACE {
				continue;
			}
			if let Some(zvariant::Value::Str(state)) = changed.get(ACTIVE_STATE_PROPERTY) {
				return match state.to_string().as_str() {
					"failed" | "inactive" => {
						tracing::warn!(state = state.to_string(), "Application exited shortly after launch.");
						Err(())
					},
					_ => continue, // e.g. "activating" → "active" — keep waiting
				};
			}
		}
		Ok(())
	})
	.await;

	match failure_result {
		Ok(Ok(())) => {
			// Timeout expired — unit is still alive, launch succeeded.
			tracing::info!("Application launched in service {}", options.unit_name);
			Ok(path)
		},
		Ok(Err(())) => Err(()),
		Err(_) => {
			// Timeout expired — unit is still alive, launch succeeded.
			tracing::info!("Application launched in service {}", options.unit_name);
			Ok(path)
		},
	}
}

/// Build a list of exec command entries from a list of command configs.
/// Each entry is (absolute_path, argv, ignore_errors=false).
fn build_exec_entries(commands: &[Vec<String>]) -> Vec<(String, Vec<String>, bool)> {
	commands
		.iter()
		.filter_map(|cmd| {
			let first = cmd.first()?;
			let abs = which::which(first).ok()?;
			let abs_str = abs.to_str()?.to_string();
			let argv: Vec<String> = std::iter::once(abs_str.clone())
				.chain(cmd[1..].iter().cloned())
				.collect();
			Some((abs_str, argv, false))
		})
		.collect()
}

/// Build a single exec command entry from a program path and args.
/// Returns (absolute_path, argv, ignore_errors=false), or None if the program is not found.
fn build_exec_entry(program: String, args: Vec<String>) -> Option<(String, Vec<String>, bool)> {
	let abs = which::which(&program).ok()?;
	let abs_str = abs.to_str()?.to_string();
	let argv: Vec<String> = std::iter::once(abs_str.clone()).chain(args.iter().cloned()).collect();
	Some((abs_str, argv, false))
}

/// Build a properly-typed `a(sasb)` array for systemd ExecStart/ExecStartPre/ExecStopPost.
///
/// Must use `Array::new(signature)` + `append()` rather than `Array::from(Vec<Value>)`,
/// which always produces `av` regardless of the element type.
fn build_exec_array(entries: &[(String, Vec<String>, bool)]) -> Result<zvariant::Value<'static>, ()> {
	let element_sig = zvariant::Signature::structure([
		zvariant::Signature::Str,
		zvariant::Signature::array(zvariant::Signature::Str),
		zvariant::Signature::Bool,
	]);
	let mut arr = zvariant::Array::new(&element_sig);
	for entry in entries {
		arr.append(zvariant::Value::from(entry.clone()))
			.map_err(|e| tracing::error!("Failed to append exec entry: {e}"))?;
	}
	Ok(zvariant::Value::Array(arr))
}

#[cfg(test)]
mod tests {
	use super::{
		ApplicationConfig, ApplicationContext, LaunchOptions, UnitCommands, make_envs, split_standard_io,
		steam_shutdown_entry, stop_in_background, unit_properties,
	};
	use std::collections::{BTreeMap, HashMap};
	use std::path::PathBuf;
	use std::time::Duration;

	#[test]
	fn stopping_a_game_returns_at_once_and_reports_the_stop_once_it_has_finished() {
		let (unit_stopped, mut stopped) = tokio::sync::oneshot::channel::<()>();
		let (finish, finished) = std::sync::mpsc::channel::<()>();
		stop_in_background(unit_stopped, move || {
			let _ = finished.recv_timeout(Duration::from_secs(5));
		});
		assert_eq!(
			stopped.try_recv(),
			Err(tokio::sync::oneshot::error::TryRecvError::Empty),
			"the caller has control back while the Game's unit is still stopping"
		);

		finish.send(()).unwrap();
		assert!(
			stopped.blocking_recv().is_err(),
			"`unit_stopped` goes once the stop has returned"
		);
	}

	#[test]
	fn test_steam_integration_defaults_to_off() {
		let unset: ApplicationConfig = toml::from_str("title = \"Game\"\ncommand = [\"g\"]").unwrap();
		assert!(!unset.steam_integration);
		let off: ApplicationConfig =
			toml::from_str("title = \"Desktop\"\ncommand = [\"d\"]\nsteam_integration = false").unwrap();
		assert!(!off.steam_integration);
	}

	#[test]
	fn test_standard_io_defaults_to_null() {
		assert_eq!(split_standard_io(&None), ("null".to_string(), None));
	}

	#[test]
	fn test_standard_io_passes_bare_enums_through() {
		assert_eq!(
			split_standard_io(&Some("journal".to_string())),
			("journal".to_string(), None)
		);
		assert_eq!(
			split_standard_io(&Some("inherit".to_string())),
			("inherit".to_string(), None)
		);
		assert_eq!(
			split_standard_io(&Some("kmsg+console".to_string())),
			("kmsg+console".to_string(), None)
		);
	}

	#[test]
	fn test_standard_io_splits_file_paths() {
		assert_eq!(
			split_standard_io(&Some("file:/var/log/app.log".to_string())),
			("file".to_string(), Some(("File", "/var/log/app.log".to_string())))
		);
	}

	#[test]
	fn test_standard_io_splits_append_and_truncate() {
		assert_eq!(
			split_standard_io(&Some("append:/var/log/app.log".to_string())),
			(
				"append".to_string(),
				Some(("FileToAppend", "/var/log/app.log".to_string()))
			)
		);
		assert_eq!(
			split_standard_io(&Some("truncate:/var/log/app.log".to_string())),
			(
				"truncate".to_string(),
				Some(("FileToTruncate", "/var/log/app.log".to_string()))
			)
		);
	}

	#[test]
	fn test_standard_io_splits_fd_names() {
		assert_eq!(
			split_standard_io(&Some("fd:stdout".to_string())),
			("fd".to_string(), Some(("FileDescriptorName", "stdout".to_string())))
		);
	}

	fn test_context() -> ApplicationContext {
		ApplicationContext {
			unit_name: "moonshine-session.service".to_string(),
			pulse_socket_path: PathBuf::from("/run/user/1000/moonshine/pulse/native"),
			xdisplay: 0,
			wayland_display: "wayland-1".to_string(),
			compositor_env: Vec::new(),
			extra_env: HashMap::new(),
		}
	}

	fn property<'p>(properties: &'p [(String, zvariant::Value<'p>)], name: &str) -> Option<&'p zvariant::Value<'p>> {
		properties.iter().find(|(key, _)| key == name).map(|(_, value)| value)
	}

	#[test]
	fn a_steam_application_stops_with_steams_shutdown_and_the_longer_timeout() {
		let envs = make_envs(&test_context(), &BTreeMap::new()).unwrap();
		let shutdown = steam_shutdown_entry("/usr/bin/sh".to_string(), "/usr/bin/steam".to_string());
		let steam = launch_options(&envs, true);
		let game = launch_options(&envs, false);

		let steam = unit_properties(&steam, &commands(Some(shutdown.clone()))).unwrap();
		let game = unit_properties(&game, &commands(None)).unwrap();

		assert_eq!(
			property(&steam, "TimeoutStopUSec"),
			Some(&zvariant::Value::U64(30_000_000))
		);
		assert_eq!(
			property(&steam, "ExecStop"),
			Some(&super::build_exec_array(&[shutdown]).unwrap())
		);
		assert_eq!(
			property(&game, "TimeoutStopUSec"),
			Some(&zvariant::Value::U64(5_000_000))
		);
		assert_eq!(property(&game, "ExecStop"), None);
	}

	/// A stand-in Steam in `dir`: `steam -shutdown` creates `dir/shutdown`.
	fn stand_in_steam(dir: &std::path::Path) -> (String, PathBuf) {
		use std::os::unix::fs::PermissionsExt;
		let flag = dir.join("shutdown");
		let steam = dir.join("steam");
		std::fs::write(
			&steam,
			format!("#!/bin/sh\n[ \"$1\" = -shutdown ] && touch '{}'\n", flag.display()),
		)
		.unwrap();
		std::fs::set_permissions(&steam, std::fs::Permissions::from_mode(0o755)).unwrap();
		(steam.to_str().unwrap().to_string(), flag)
	}

	/// Other tests in this binary change `PATH`; the stand-ins get their own.
	const STAND_IN_PATH: &str = "/usr/bin:/bin";

	/// Run `entry` as systemd's `ExecStop` would, killing it after 10 s.
	fn run_exec_stop(entry: &(String, Vec<String>, bool), main_pid: Option<u32>) -> std::process::ExitStatus {
		let mut command = std::process::Command::new(&entry.0);
		command
			.args(&entry.1[1..])
			.env("PATH", STAND_IN_PATH)
			.env_remove("MAINPID");
		if let Some(pid) = main_pid {
			command.env("MAINPID", pid.to_string());
		}
		let mut child = command.spawn().unwrap();
		let deadline = std::time::Instant::now() + Duration::from_secs(10);
		loop {
			if let Some(status) = child.try_wait().unwrap() {
				return status;
			}
			if std::time::Instant::now() > deadline {
				child.kill().ok();
				child.wait().ok();
				panic!("ExecStop still running after 10 s");
			}
			std::thread::sleep(Duration::from_millis(20));
		}
	}

	#[test]
	fn steams_shutdown_returns_only_after_steam_has_exited() {
		let dir = tempfile::tempdir().unwrap();
		let (steam, flag) = stand_in_steam(dir.path());
		// The unit's main process: Steam, which exits 0.5 s after it is asked to shut down (or
		// after 5 s without being asked).
		let mut main = std::process::Command::new("/bin/sh")
			.args([
				"-c",
				"i=0; while [ ! -e \"$0\" ] && [ $i -lt 100 ]; do sleep 0.05 || exit 1; i=$((i + 1)); done; sleep 0.5",
			])
			.arg(&flag)
			.env("PATH", STAND_IN_PATH)
			.spawn()
			.unwrap();
		let pid = main.id();
		// systemd reaps the main process; here the test does.
		let reaper = std::thread::spawn(move || {
			main.wait().unwrap();
			std::time::Instant::now()
		});

		let status = run_exec_stop(&steam_shutdown_entry("/bin/sh".to_string(), steam), Some(pid));
		let returned = std::time::Instant::now();

		assert!(status.success());
		assert!(flag.exists(), "steam -shutdown was not run");
		assert!(reaper.join().unwrap() <= returned, "returned before Steam exited");
	}

	#[test]
	fn steams_shutdown_starts_no_steam_once_steam_has_exited() {
		let dir = tempfile::tempdir().unwrap();
		let (steam, flag) = stand_in_steam(dir.path());
		let entry = steam_shutdown_entry("/bin/sh".to_string(), steam);
		let mut exited = std::process::Command::new("/bin/true").spawn().unwrap();
		let gone = exited.id();
		exited.wait().unwrap();

		// systemd leaves MAINPID unset once the main process has exited.
		assert!(run_exec_stop(&entry, None).success());
		assert!(run_exec_stop(&entry, Some(gone)).success());

		assert!(!flag.exists(), "steam -shutdown ran with no Steam to shut down");
	}

	#[test]
	fn application_env_comes_after_the_moonshine_variables() {
		let env = BTreeMap::from([("COSMIC_BACKEND".to_string(), "winit".to_string())]);

		let envs = make_envs(&test_context(), &env).unwrap();

		assert_eq!(envs.last().map(String::as_str), Some("COSMIC_BACKEND=winit"));
		assert!(envs.contains(&"DISPLAY=:0".to_string()));
	}

	#[test]
	fn application_pipewire_clients_find_no_desktop_pipewire() {
		let envs = make_envs(&test_context(), &BTreeMap::new()).unwrap();

		// Native PipeWire clients (SDL's default backend, pipewire-alsa, pipewire-jack) look for
		// the server's socket in PIPEWIRE_RUNTIME_DIR, which holds only Moonshine's Pulse socket,
		// so they fail over to Pulse and reach Moonshine instead of the desktop's sinks.
		assert!(envs.contains(&"PIPEWIRE_RUNTIME_DIR=/run/user/1000/moonshine/pulse".to_string()));
		assert!(envs.contains(&"PIPEWIRE_REMOTE=pipewire-0".to_string()));
		assert!(envs.contains(&"PULSE_SERVER=unix:/run/user/1000/moonshine/pulse/native".to_string()));
	}

	/// The value systemd gives `key`: the last assignment wins.
	fn effective<'a>(envs: &'a [String], key: &str) -> Option<&'a str> {
		let prefix = format!("{key}=");
		envs.iter().rev().find_map(|entry| entry.strip_prefix(prefix.as_str()))
	}

	#[test]
	fn a_game_gets_forced_sync_by_default() {
		let envs = make_envs(&test_context(), &BTreeMap::new()).unwrap();

		assert_eq!(effective(&envs, "vblank_mode"), Some("3"));
		assert_eq!(effective(&envs, "__GL_SYNC_TO_VBLANK"), Some("1"));
	}

	#[test]
	fn the_hold_opt_out_keeps_the_games_own_vsync() {
		for value in ["fifo", "off", "0", "false"] {
			let env = BTreeMap::from([("FERRISCOPE_WSI_HOLD".to_string(), value.to_string())]);

			let envs = make_envs(&test_context(), &env).unwrap();

			assert_eq!(effective(&envs, "vblank_mode"), None, "FERRISCOPE_WSI_HOLD={value}");
			assert_eq!(
				effective(&envs, "__GL_SYNC_TO_VBLANK"),
				None,
				"FERRISCOPE_WSI_HOLD={value}"
			);
		}
	}

	#[test]
	fn the_off_switch_turns_off_both_the_hold_and_forced_sync() {
		let env = BTreeMap::from([("FERRISCOPE_WSI_HOLD".to_string(), "off".to_string())]);

		let envs = make_envs(&test_context(), &env).unwrap();

		// The Game's environment tells ferriscope-wsi to hold nothing...
		let hold = effective(&envs, ferriscope::WsiHold::VARIABLE);
		assert_eq!(ferriscope::WsiHold::parse(hold), Ok(ferriscope::WsiHold::Off));
		// ...and carries no forced GL sync.
		assert_eq!(effective(&envs, "vblank_mode"), None);
		assert_eq!(effective(&envs, "__GL_SYNC_TO_VBLANK"), None);
	}

	#[test]
	fn an_unrecognised_hold_value_is_rejected() {
		for value in ["FIFO", "Off", "no", "all"] {
			let env = BTreeMap::from([("FERRISCOPE_WSI_HOLD".to_string(), value.to_string())]);

			assert!(
				make_envs(&test_context(), &env).is_err(),
				"FERRISCOPE_WSI_HOLD={value} was accepted"
			);
		}
	}

	#[test]
	fn a_games_own_vblank_mode_wins() {
		let env = BTreeMap::from([
			("vblank_mode".to_string(), "0".to_string()),
			("__GL_SYNC_TO_VBLANK".to_string(), "0".to_string()),
		]);

		let envs = make_envs(&test_context(), &env).unwrap();

		assert_eq!(effective(&envs, "vblank_mode"), Some("0"));
		assert_eq!(effective(&envs, "__GL_SYNC_TO_VBLANK"), Some("0"));
	}

	#[test]
	fn application_env_is_read_from_toml() {
		let config: ApplicationConfig = toml::from_str(
			r#"
			title = "COSMIC"
			command = ["/usr/bin/start-cosmic"]
			env = { COSMIC_BACKEND = "winit" }
			"#,
		)
		.unwrap();

		assert_eq!(config.env.get("COSMIC_BACKEND").map(String::as_str), Some("winit"));
	}

	fn launch_options<'a>(envs: &'a [String], steam: bool) -> LaunchOptions<'a> {
		static NO_COMMANDS: Vec<Vec<String>> = Vec::new();
		static NO_OUTPUT: Option<String> = None;
		LaunchOptions {
			unit_name: "moonshine-session.service",
			program: "game",
			args: &[],
			envs,
			timeout: Duration::from_secs(2),
			pre_commands: &NO_COMMANDS,
			post_commands: &NO_COMMANDS,
			stdout_value: &NO_OUTPUT,
			stderr_value: &NO_OUTPUT,
			steam,
		}
	}

	fn commands(stop: Option<(String, Vec<String>, bool)>) -> UnitCommands {
		UnitCommands {
			pre: Vec::new(),
			main: ("/usr/bin/game".to_string(), vec!["/usr/bin/game".to_string()], false),
			stop,
			post: Vec::new(),
		}
	}

	/// The environment systemd gives the unit's processes (systemd.exec(5)): the user manager's,
	/// then `Environment=` assignments in order, then `UnsetEnvironment=` removes names (and exact
	/// assignments) last, whoever set them.
	fn unit_environment(
		manager: &[(&str, &str)],
		properties: &[(String, zvariant::Value<'_>)],
	) -> BTreeMap<String, String> {
		let strings = |name: &str| -> Vec<String> {
			properties
				.iter()
				.filter(|(key, _)| key == name)
				.flat_map(|(_, value)| <Vec<String>>::try_from(value.try_clone().unwrap()).unwrap())
				.collect()
		};
		let mut env: BTreeMap<String, String> = manager
			.iter()
			.map(|(key, value)| (key.to_string(), value.to_string()))
			.collect();
		for assignment in strings("Environment") {
			let (key, value) = assignment.split_once('=').unwrap();
			env.insert(key.to_string(), value.to_string());
		}
		for unset in strings("UnsetEnvironment") {
			match unset.split_once('=') {
				Some((key, value)) if env.get(key).map(String::as_str) == Some(value) => {
					env.remove(key);
				},
				Some(_) => {},
				None => {
					env.remove(&unset);
				},
			}
		}
		env
	}

	#[test]
	fn a_game_unit_has_no_desktop_compositor_or_ipc_variables() {
		// The shared Host's user manager (`systemctl --user show-environment` under niri), plus the
		// sockets of other desktops a user session may export.
		let manager = [
			("HOME", "/home/kamil"),
			("DBUS_SESSION_BUS_ADDRESS", "unix:path=/run/user/1000/bus"),
			("DISPLAY", ":13"),
			("WAYLAND_DISPLAY", "wayland-9"),
			("NIRI_SOCKET", "/run/user/1000/niri.wayland-9.462431.sock"),
			("SWAYSOCK", "/run/user/1000/sway-ipc.1000.1234.sock"),
			("I3SOCK", "/run/user/1000/i3/ipc-socket.1234"),
			("HYPRLAND_INSTANCE_SIGNATURE", "abc_123"),
			("XAUTHORITY", "/run/user/1000/xauth_abc"),
			("XDG_CURRENT_DESKTOP", "niri"),
			("XDG_SESSION_DESKTOP", "niri"),
			("DESKTOP_STARTUP_ID", "niri-1"),
			("XDG_ACTIVATION_TOKEN", "token"),
		];
		let envs = make_envs(&test_context(), &BTreeMap::new()).unwrap();
		let options = launch_options(&envs, false);

		let env = unit_environment(&manager, &unit_properties(&options, &commands(None)).unwrap());

		for desktop in [
			"NIRI_SOCKET",
			"SWAYSOCK",
			"I3SOCK",
			"HYPRLAND_INSTANCE_SIGNATURE",
			"XAUTHORITY",
			"XDG_CURRENT_DESKTOP",
			"XDG_SESSION_DESKTOP",
			"DESKTOP_STARTUP_ID",
			"XDG_ACTIVATION_TOKEN",
		] {
			assert_eq!(env.get(desktop), None, "{desktop}");
		}
		// The Stream's own displays replace the desktop's, and the rest of the session stays.
		assert_eq!(env.get("DISPLAY").map(String::as_str), Some(":0"));
		assert_eq!(env.get("WAYLAND_DISPLAY").map(String::as_str), Some("wayland-1"));
		assert_eq!(env.get("HOME").map(String::as_str), Some("/home/kamil"));
	}

	#[test]
	fn the_compositors_own_desktop_identity_reaches_the_game() {
		// ferriscope gives Steam gamescope's identity; only the desktop's is removed.
		let mut context = test_context();
		context.compositor_env = vec![("XDG_CURRENT_DESKTOP".to_string(), "gamescope".to_string())];
		let envs = make_envs(&context, &BTreeMap::new()).unwrap();
		let options = launch_options(&envs, true);

		let env = unit_environment(
			&[("XDG_CURRENT_DESKTOP", "niri")],
			&unit_properties(&options, &commands(None)).unwrap(),
		);

		assert_eq!(env.get("XDG_CURRENT_DESKTOP").map(String::as_str), Some("gamescope"));
	}

	#[test]
	fn a_variable_set_twice_reaches_systemd_once_with_the_games_value() {
		// systemd's StartTransientUnit rejects an `Environment=` block that assigns a name twice
		// ("Invalid environment block"), so the Game never starts. Moonshine forces `vblank_mode`
		// and `__GL_SYNC_TO_VBLANK`, the compositor and the client's mode set theirs, and the
		// Game's env may set any of them again.
		let mut context = test_context();
		context.compositor_env = vec![("XDG_CURRENT_DESKTOP".to_string(), "gamescope".to_string())];
		context.extra_env = HashMap::from([("XDG_CURRENT_DESKTOP".to_string(), "client".to_string())]);
		let env = BTreeMap::from([
			("vblank_mode".to_string(), "0".to_string()),
			("XDG_CURRENT_DESKTOP".to_string(), "game".to_string()),
		]);
		let envs = make_envs(&context, &env).unwrap();
		let options = launch_options(&envs, false);
		let properties = unit_properties(&options, &commands(None)).unwrap();

		let block: Vec<String> = properties
			.iter()
			.filter(|(key, _)| key == "Environment")
			.flat_map(|(_, value)| <Vec<String>>::try_from(value.try_clone().unwrap()).unwrap())
			.collect();
		let mut keys: Vec<&str> = block.iter().map(|entry| entry.split_once('=').unwrap().0).collect();
		keys.sort_unstable();
		let mut duplicates = keys.clone();
		duplicates.dedup();
		assert_eq!(keys, duplicates, "duplicate keys in the Environment block {block:?}");
		assert_eq!(effective(&block, "vblank_mode"), Some("0"));
		assert_eq!(effective(&block, "__GL_SYNC_TO_VBLANK"), Some("1"));
		assert_eq!(effective(&block, "XDG_CURRENT_DESKTOP"), Some("game"));
	}
}
