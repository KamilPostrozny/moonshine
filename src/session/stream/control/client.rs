//! Which ENet peer is the Stream's client.
//!
//! A connection alone doesn't make a client: anyone on the LAN can open one to
//! the control port. A peer becomes the client with its first message that
//! decrypts under the Stream's key, which only the launching client holds, so a
//! stray connection can't reset or take over a live Stream's control session.

use tokio_enet::PeerId;

/// Who sent a control message, as far as the Stream's client goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Sender {
	/// The connected client.
	Client,
	/// A peer that has just become the client with this message: per-client
	/// state starts over before the message is handled.
	NewClient,
	/// Any other peer: its message is ignored.
	Stranger,
}

/// The connected client: the last peer whose message decrypted under the
/// Stream's key, until it disconnects.
#[derive(Debug, Default)]
pub(super) struct ClientPeer {
	client: Option<PeerId>,
	/// Connections that were the client until a newer one replaced it, until
	/// they disconnect: a late message from one doesn't take the Stream back.
	replaced: Vec<PeerId>,
}

impl ClientPeer {
	/// The connected client, if any.
	pub(super) fn peer(&self) -> Option<PeerId> {
		self.client
	}

	/// `peer` sent a message; `decrypted` when it decrypted under the Stream's
	/// key. Only such a message, from a connection that was never the client,
	/// lets a peer other than the client replace it.
	pub(super) fn received(&mut self, peer: PeerId, decrypted: bool) -> Sender {
		if self.client == Some(peer) {
			Sender::Client
		} else if decrypted && !self.replaced.contains(&peer) {
			self.replaced.extend(self.client.replace(peer));
			Sender::NewClient
		} else {
			Sender::Stranger
		}
	}

	/// `peer` connected. On the slot of the client or a replaced one, that
	/// connection is gone: ENet reuses a slot only once its last peer has been
	/// reset.
	pub(super) fn connected(&mut self, peer: PeerId) {
		self.disconnected(peer);
	}

	/// `peer` disconnected.
	pub(super) fn disconnected(&mut self, peer: PeerId) {
		if self.client == Some(peer) {
			self.client = None;
		}
		self.replaced.retain(|replaced| *replaced != peer);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const PHONE: PeerId = PeerId(0);
	const STRAY: PeerId = PeerId(1);

	fn with_client(peer: PeerId) -> ClientPeer {
		let mut client = ClientPeer::default();
		client.connected(peer);
		assert_eq!(client.received(peer, true), Sender::NewClient);
		client
	}

	#[test]
	fn a_connection_alone_is_not_a_client() {
		let mut client = ClientPeer::default();
		client.connected(PHONE);
		assert_eq!(client.peer(), None);
		assert_eq!(client.received(PHONE, false), Sender::Stranger);
		assert_eq!(client.peer(), None);
	}

	#[test]
	fn a_peers_first_decrypted_message_makes_it_the_client() {
		let mut client = with_client(PHONE);
		assert_eq!(client.peer(), Some(PHONE));
		assert_eq!(client.received(PHONE, true), Sender::Client);
		assert_eq!(client.received(PHONE, false), Sender::Client);
	}

	#[test]
	fn a_peer_without_a_decrypted_message_does_not_replace_the_client() {
		let mut client = with_client(PHONE);
		client.connected(STRAY);
		assert_eq!(client.received(STRAY, false), Sender::Stranger);
		assert_eq!(client.received(STRAY, false), Sender::Stranger);
		client.disconnected(STRAY);
		assert_eq!(client.peer(), Some(PHONE));
		assert_eq!(client.received(PHONE, true), Sender::Client);
	}

	#[test]
	fn a_reconnecting_client_replaces_the_old_one_with_its_first_decrypted_message() {
		let mut client = with_client(PHONE);
		// Force-quit: the old peer hasn't timed out when the new one connects.
		let reconnected = PeerId(2);
		client.connected(reconnected);
		assert_eq!(client.peer(), Some(PHONE));
		assert_eq!(client.received(reconnected, true), Sender::NewClient);
		assert_eq!(client.peer(), Some(reconnected));
		// The old connection's late messages are ignored.
		assert_eq!(client.received(PHONE, true), Sender::Stranger);
		client.disconnected(PHONE);
		assert_eq!(client.peer(), Some(reconnected));
		// Its slot, reused by a later connection, may become the client again.
		client.connected(PHONE);
		assert_eq!(client.received(PHONE, true), Sender::NewClient);
		assert_eq!(client.peer(), Some(PHONE));
	}

	#[test]
	fn a_client_on_a_reused_slot_starts_over() {
		let mut client = with_client(PHONE);
		client.disconnected(PHONE);
		assert_eq!(client.peer(), None);
		assert_eq!(client.received(PHONE, true), Sender::NewClient);
		// A connection on the client's slot means the client is gone.
		client.connected(PHONE);
		assert_eq!(client.peer(), None);
		assert_eq!(client.received(PHONE, true), Sender::NewClient);
	}
}
