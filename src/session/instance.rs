//! Names the per-session resources that every Moonshine run by one user would otherwise share.
//!
//! Each session runs its application as a unit of the user's systemd manager and binds a Pulse
//! socket under `$XDG_RUNTIME_DIR`. Two Moonshines of one user (moonlight-ios runs its shared
//! Host and a LatencyHost throwaway side by side) would stop each other's application at session
//! start and take each other's Pulse socket. `MOONSHINE_INSTANCE=<name>` gives one of them its
//! own names; unset or empty keeps the default ones.

/// Environment variable naming this Moonshine's instance.
pub const INSTANCE_ENV: &str = "MOONSHINE_INSTANCE";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInstance {
	name: Option<String>,
}

impl SessionInstance {
	/// The instance named by `MOONSHINE_INSTANCE`.
	pub fn from_env() -> Result<Self, ()> {
		Self::named(std::env::var(INSTANCE_ENV).ok().as_deref())
	}

	/// An instance name may hold ASCII letters, digits, `-` and `_`, so it fits a systemd unit
	/// name and a file name.
	pub fn named(name: Option<&str>) -> Result<Self, ()> {
		match name {
			None | Some("") => Ok(Self { name: None }),
			Some(name) if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') => Ok(Self {
				name: Some(name.to_string()),
			}),
			Some(name) => {
				tracing::error!("{INSTANCE_ENV} '{name}' may only hold ASCII letters, digits, '-' and '_'.");
				Err(())
			},
		}
	}

	/// The user unit the session's application runs as.
	pub fn unit_name(&self) -> String {
		match &self.name {
			None => "moonshine-session.service".to_string(),
			Some(name) => format!("moonshine-{name}-session.service"),
		}
	}

	/// The directory under `$XDG_RUNTIME_DIR` that holds the session's Pulse socket (`pulse/native`).
	pub fn runtime_dir_name(&self) -> String {
		match &self.name {
			None => "moonshine".to_string(),
			Some(name) => format!("moonshine-{name}"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn unset_or_empty_keeps_the_default_names() {
		for name in [None, Some("")] {
			let instance = SessionInstance::named(name).unwrap();
			assert_eq!(instance.unit_name(), "moonshine-session.service");
			assert_eq!(instance.runtime_dir_name(), "moonshine");
		}
	}

	#[test]
	fn a_named_instance_gets_its_own_unit_and_runtime_directory() {
		let instance = SessionInstance::named(Some("shared")).unwrap();
		assert_eq!(instance.unit_name(), "moonshine-shared-session.service");
		assert_eq!(instance.runtime_dir_name(), "moonshine-shared");
	}

	#[test]
	fn a_name_that_is_not_a_unit_or_file_name_part_is_refused() {
		for name in ["a/b", "two words", "dot.ted", "ünï"] {
			assert_eq!(SessionInstance::named(Some(name)), Err(()), "{name}");
		}
		assert!(SessionInstance::named(Some("Latency_Host-2")).is_ok());
	}
}
