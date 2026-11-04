//! Negotiation of moonlight-ios' latency protocol (fork extension).
//!
//! The latency protocol carries clock sync and frame-time corrections between a client and this
//! host as opaque payloads of one control-stream message type. The host advertises it in
//! `x-ss-general.featureFlags` of its DESCRIBE reply when built with it (the `latency-protocol`
//! feature), and a session speaks it only when the client sets the matching bit in
//! `x-ml-general.featureFlags` of its ANNOUNCE. Its messages count only on the encrypted control
//! stream. Other clients see a normal Moonshine: they ignore unknown host feature bits.
//!
//! The wire values match the KamilPostrozny moonlight-common-c fork (`LI_FF_LATENCY_PROTOCOL`,
//! `ML_FF_LATENCY_PROTOCOL`, message type `0x55f0`).

/// Whether this build speaks the latency protocol (the `latency-protocol` feature).
pub const BUILT: bool = cfg!(feature = "latency-protocol");

/// Host bit in `x-ss-general.featureFlags` (moonlight-common-c's `LI_FF_LATENCY_PROTOCOL`).
pub const HOST_FEATURE_FLAG: u32 = 0x4000_0000;

/// Client bit in `x-ml-general.featureFlags` (moonlight-common-c's `ML_FF_LATENCY_PROTOCOL`).
pub const CLIENT_FEATURE_FLAG: u32 = 0x4000_0000;

/// Control-stream message type carrying a latency protocol payload (Sunshine extension range).
pub const MESSAGE_TYPE: u16 = 0x55f0;

/// Add the latency protocol to the host feature flags of a DESCRIBE reply when `built`.
pub const fn advertise(feature_flags: u32, built: bool) -> u32 {
	if built {
		feature_flags | HOST_FEATURE_FLAG
	} else {
		feature_flags
	}
}

/// Whether a session speaks the latency protocol: the build speaks it and the client's
/// `x-ml-general.featureFlags` (absent for clients that send none) offered it.
pub const fn negotiated(client_feature_flags: Option<u32>, built: bool) -> bool {
	match client_feature_flags {
		Some(flags) => built && flags & CLIENT_FEATURE_FLAG != 0,
		None => false,
	}
}

/// Whether the control stream takes a latency protocol message: only in a negotiated session, and
/// only when the message arrived inside an encrypted control message.
pub const fn accepts_message(negotiated: bool, encrypted: bool) -> bool {
	negotiated && encrypted
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn wire_values_match_the_client_fork() {
		assert_eq!(HOST_FEATURE_FLAG, 0x4000_0000);
		assert_eq!(CLIENT_FEATURE_FLAG, 0x4000_0000);
		assert_eq!(MESSAGE_TYPE, 0x55f0);
	}

	#[test]
	fn advertises_only_when_built() {
		assert_eq!(advertise(0x03, true), 0x4000_0003);
		assert_eq!(advertise(0x03, false), 0x03);
	}

	#[test]
	fn negotiates_only_when_built_and_offered() {
		assert!(negotiated(Some(CLIENT_FEATURE_FLAG | 0x03), true));
		assert!(!negotiated(Some(CLIENT_FEATURE_FLAG | 0x03), false));
		assert!(!negotiated(Some(0x03), true));
		assert!(!negotiated(None, true));
	}

	#[test]
	fn accepts_messages_only_encrypted_in_a_negotiated_session() {
		assert!(accepts_message(true, true));
		assert!(!accepts_message(true, false));
		assert!(!accepts_message(false, true));
		assert!(!accepts_message(false, false));
	}
}
