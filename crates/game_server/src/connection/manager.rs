//! Connection manager for tracking and managing client connections.
//!
//! This module provides the central management system for all client connections,
//! handling connection lifecycle, player ID assignment, and message delivery.

use super::{
    client::ClientConnection,
    compress::ListenerCompression,
    outbox::{outbox, Control, Outbox, OutboxReceiver, Pushed, DEFAULT_CHANNEL_CAPACITY},
    CloseCode, ConnectionId,
};
use dashmap::DashMap;
use horizon_event_system::{AuthenticationStatus, ListenerProfile, Outbound, PlayerId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// What the server holds to reach one connection.
#[derive(Debug, Clone)]
struct ConnectionHandle {
    outbox: Outbox,
    profile: ListenerProfile,
    compression: Arc<ListenerCompression>,
}

impl ConnectionHandle {
    /// Queues a frame: large ones wrapped now (compressed once for all recipients),
    /// small ones left for the writer to batch.
    fn push(&self, frame: Outbound) -> Pushed {
        match self.compression.wrap_large(&frame.bytes) {
            Some(bytes) => self.outbox.push_wrapped(Outbound { bytes, delivery: frame.delivery }),
            None => self.outbox.push(frame),
        }
    }
}

/// Central manager for all client connections.
///
/// The `ConnectionManager` tracks active connections, assigns unique IDs, manages player
/// associations, and queues frames for each connection.
///
/// # Architecture
///
/// * Every connection has its own [`Outbox`]: a slow client never delays the others and
///   never loses a reliable frame (see the `outbox` module).
/// * Players are indexed by ID, so delivering to a player is a map lookup, not a scan.
/// * Kicking a connection asks its writer to send the close frame; the connection's
///   handler then cleans up and emits `player_disconnected` like any other disconnect.
#[derive(Debug)]
pub struct ConnectionManager {
    /// Map of connection ID to client connection information
    connections: Arc<RwLock<HashMap<ConnectionId, ClientConnection>>>,

    /// How to reach each connection's writer.
    handles: DashMap<ConnectionId, ConnectionHandle>,

    /// Which connection each player is on.
    players: DashMap<PlayerId, ConnectionId>,

    /// Atomic counter for generating unique connection IDs
    next_id: Arc<std::sync::atomic::AtomicUsize>,

    /// Frames each connection's channel holds before its backlog takes over.
    channel_capacity: usize,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    /// Creates a new connection manager.
    pub fn new() -> Self {
        Self::with_channel_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    /// Creates a connection manager whose connections hold `capacity` frames in their fast
    /// path before queuing the rest in their backlog.
    pub fn with_channel_capacity(capacity: usize) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            handles: DashMap::new(),
            players: DashMap::new(),
            next_id: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            channel_capacity: capacity,
        }
    }

    /// Registers a new connection and returns its ID with the receiving half of its
    /// outbox, which the connection's writer owns.
    pub async fn open_connection(
        &self,
        remote_addr: SocketAddr,
        profile: ListenerProfile,
    ) -> (ConnectionId, OutboxReceiver) {
        self.open_connection_with(remote_addr, profile, Arc::new(ListenerCompression::none()))
            .await
    }

    /// Like [`open_connection`](Self::open_connection), for a listener with compression.
    pub async fn open_connection_with(
        &self,
        remote_addr: SocketAddr,
        profile: ListenerProfile,
        compression: Arc<ListenerCompression>,
    ) -> (ConnectionId, OutboxReceiver) {
        let connection_id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = outbox(connection_id, self.channel_capacity);
        self.handles
            .insert(connection_id, ConnectionHandle { outbox: tx, profile, compression });
        let connection = ClientConnection::new(remote_addr);
        self.connections.write().await.insert(connection_id, connection);
        info!("🔗 Connection {} from {}", connection_id, remote_addr);
        (connection_id, rx)
    }

    /// Adds a new connection and returns its unique ID.
    ///
    /// The connection has no writer: frames sent to it are not delivered. This is meant
    /// for bookkeeping and tests; network listeners use [`open_connection`].
    ///
    /// [`open_connection`]: ConnectionManager::open_connection
    pub async fn add_connection(&self, remote_addr: SocketAddr) -> ConnectionId {
        let (connection_id, _rx) = self
            .open_connection(remote_addr, ListenerProfile::default())
            .await;
        connection_id
    }

    /// Asks a connection to close with `code`. Its writer sends the close frame, then the
    /// connection's handler removes it and emits `player_disconnected`.
    pub fn close_connection(
        &self,
        connection_id: ConnectionId,
        code: CloseCode,
        reason: Option<String>,
    ) -> Result<(), String> {
        let handle = self
            .handles
            .get(&connection_id)
            .ok_or_else(|| format!("Connection {connection_id} not found"))?;
        let reason = reason.unwrap_or_else(|| code.default_reason().to_string());
        if handle.outbox.control(Control::Close(code, reason)) {
            Ok(())
        } else {
            Err(format!("Connection {connection_id} is already closing"))
        }
    }

    /// Kick (disconnect) a connection by ID, sending a close frame
    pub async fn kick_connection(
        &self,
        connection_id: ConnectionId,
        reason: Option<String>,
    ) -> Result<(), String> {
        self.close_connection(connection_id, CloseCode::Kicked, reason)
    }

    /// Kick (disconnect) a player by PlayerId
    pub async fn kick_player(&self, player_id: PlayerId, reason: Option<String>) -> Result<(), String> {
        match self.get_connection_id_by_player(player_id).await {
            Some(conn_id) => self.kick_connection(conn_id, reason).await,
            None => Err("Player not connected".to_string()),
        }
    }

    /// Removes a connection from the manager.
    ///
    /// Called by the connection's handler once the connection has ended.
    pub async fn remove_connection(&self, connection_id: ConnectionId) {
        self.handles.remove(&connection_id);
        let removed = self.connections.write().await.remove(&connection_id);
        if let Some(connection) = removed {
            if let Some(player_id) = connection.player_id {
                self.players.remove_if(&player_id, |_, conn| *conn == connection_id);
            }
            info!(
                "❌ Connection {} from {} disconnected",
                connection_id, connection.remote_addr
            );
        }
    }

    /// Associates a player ID with a connection.
    ///
    /// This is typically called after successful authentication or when a player is
    /// assigned to a connection. A connection has one player: assigning a new one
    /// replaces the old one.
    pub async fn set_player_id(&self, connection_id: ConnectionId, player_id: PlayerId) {
        let mut connections = self.connections.write().await;
        if let Some(connection) = connections.get_mut(&connection_id) {
            if let Some(old) = connection.player_id.replace(player_id) {
                self.players.remove_if(&old, |_, conn| *conn == connection_id);
            }
            self.players.insert(player_id, connection_id);
        }
    }

    /// Retrieves the player ID associated with a connection.
    pub async fn get_player_id(&self, connection_id: ConnectionId) -> Option<PlayerId> {
        let connections = self.connections.read().await;
        connections.get(&connection_id).and_then(|c| c.player_id)
    }

    /// Queues a frame for a specific connection, without waiting.
    pub fn send_frame(&self, connection_id: ConnectionId, frame: Outbound) -> Pushed {
        match self.handles.get(&connection_id) {
            Some(handle) => handle.push(frame),
            None => Pushed::Closed,
        }
    }

    /// Queues a reliable message for a specific connection.
    pub async fn send_to_connection(&self, connection_id: ConnectionId, message: Vec<u8>) {
        let pushed = self.send_frame(connection_id, Outbound::reliable(message));
        if !pushed.is_accepted() {
            debug!("Message to connection {} not delivered: {:?}", connection_id, pushed);
        }
    }

    /// Hands a control item (a pong, a close request) to a connection's writer.
    pub fn send_control(&self, connection_id: ConnectionId, item: Control) -> bool {
        self.handles
            .get(&connection_id)
            .map(|handle| handle.outbox.control(item))
            .unwrap_or(false)
    }

    /// Broadcasts a message to all currently connected clients.
    ///
    /// The frame's bytes are shared between connections, not copied.
    ///
    /// # Returns
    ///
    /// The number of connections the message was queued for.
    pub async fn broadcast_to_all(&self, message: Vec<u8>) -> usize {
        self.broadcast_frame(Outbound::reliable(message))
    }

    /// Queues the same frame for every connection; returns how many accepted it.
    pub fn broadcast_frame(&self, frame: Outbound) -> usize {
        let mut delivered = 0;
        for handle in self.handles.iter() {
            if handle.push(frame.clone()).is_accepted() {
                delivered += 1;
            }
        }
        debug!("📡 Broadcasted message to {} connections", delivered);
        delivered
    }

    /// The listener profile of a connection.
    pub fn connection_profile(&self, connection_id: ConnectionId) -> Option<ListenerProfile> {
        self.handles.get(&connection_id).map(|handle| handle.profile)
    }

    /// Whether a connection's frames are waiting in its backlog.
    pub fn is_behind(&self, connection_id: ConnectionId) -> bool {
        self.handles
            .get(&connection_id)
            .map(|handle| handle.outbox.is_behind())
            .unwrap_or(false)
    }

    /// Number of open connections.
    pub fn connection_count(&self) -> usize {
        self.handles.len()
    }

    /// Finds the connection ID associated with a player.
    pub async fn get_connection_id_by_player(&self, player_id: PlayerId) -> Option<ConnectionId> {
        self.connection_of(player_id)
    }

    /// Finds the connection ID associated with a player, without waiting.
    pub fn connection_of(&self, player_id: PlayerId) -> Option<ConnectionId> {
        self.players.get(&player_id).map(|conn| *conn)
    }

    /// Sets the authentication status for a connection.
    pub async fn set_auth_status(&self, connection_id: ConnectionId, status: AuthenticationStatus) {
        let mut connections = self.connections.write().await;
        if let Some(connection) = connections.get_mut(&connection_id) {
            connection.set_auth_status(status);
        }
    }

    /// Gets the authentication status for a connection.
    pub async fn get_auth_status(&self, connection_id: ConnectionId) -> Option<AuthenticationStatus> {
        let connections = self.connections.read().await;
        connections.get(&connection_id).map(|c| c.auth_status())
    }

    /// Gets the authentication status for a player.
    pub async fn get_auth_status_by_player(&self, player_id: PlayerId) -> Option<AuthenticationStatus> {
        let connection_id = self.connection_of(player_id)?;
        self.get_auth_status(connection_id).await
    }

    /// Sets the authentication status for a player.
    ///
    /// # Returns
    ///
    /// `true` if the player was found and updated, `false` otherwise.
    pub async fn set_auth_status_by_player(&self, player_id: PlayerId, status: AuthenticationStatus) -> bool {
        let Some(connection_id) = self.connection_of(player_id) else {
            return false;
        };
        let mut connections = self.connections.write().await;
        match connections.get_mut(&connection_id) {
            Some(connection) => {
                connection.set_auth_status(status);
                true
            }
            None => false,
        }
    }

    /// Gets detailed connection information for a player.
    pub async fn get_connection_info_by_player(
        &self,
        player_id: PlayerId,
    ) -> Option<(ConnectionId, SocketAddr, std::time::SystemTime, AuthenticationStatus)> {
        let connection_id = self.connection_of(player_id)?;
        let connections = self.connections.read().await;
        let connection = connections.get(&connection_id)?;
        Some((
            connection_id,
            connection.remote_addr,
            connection.connected_at,
            connection.auth_status(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::outbox::{Next, Queued};

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn players_are_indexed_and_reassigned() {
        let manager = ConnectionManager::new();
        let (conn, _rx) = manager.open_connection(addr(), ListenerProfile::default()).await;
        let first = PlayerId::new();
        let second = PlayerId::new();

        manager.set_player_id(conn, first).await;
        assert_eq!(manager.get_connection_id_by_player(first).await, Some(conn));

        // A connection carries one player: the old one is no longer reachable.
        manager.set_player_id(conn, second).await;
        assert_eq!(manager.get_connection_id_by_player(first).await, None);
        assert_eq!(manager.get_connection_id_by_player(second).await, Some(conn));

        manager.remove_connection(conn).await;
        assert_eq!(manager.get_connection_id_by_player(second).await, None);
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn frames_reach_the_connection_writer() {
        let manager = ConnectionManager::new();
        let (conn, mut rx) = manager.open_connection(addr(), ListenerProfile::default()).await;
        manager.send_to_connection(conn, b"hello".to_vec()).await;
        assert_eq!(
            rx.next().await,
            Next::Frame(Queued { frame: Outbound::reliable(b"hello".to_vec()), wrapped: false })
        );
        assert_eq!(manager.send_frame(999, Outbound::reliable(Vec::new())), Pushed::Closed);
    }

    #[tokio::test]
    async fn broadcast_shares_one_frame() {
        let manager = ConnectionManager::new();
        let (_a, mut rx_a) = manager.open_connection(addr(), ListenerProfile::default()).await;
        let (_b, mut rx_b) = manager.open_connection(addr(), ListenerProfile::default()).await;
        assert_eq!(manager.broadcast_to_all(b"all".to_vec()).await, 2);
        let (Next::Frame(Queued { frame: a, .. }), Next::Frame(Queued { frame: b, .. })) =
            (rx_a.next().await, rx_b.next().await)
        else {
            panic!("both connections should get the frame");
        };
        // Same allocation, not a copy.
        assert_eq!(a.bytes.as_ptr(), b.bytes.as_ptr());
    }

    #[tokio::test]
    async fn kick_asks_the_writer_to_close() {
        let manager = ConnectionManager::new();
        let (conn, mut rx) = manager.open_connection(addr(), ListenerProfile::default()).await;
        let player = PlayerId::new();
        manager.set_player_id(conn, player).await;

        manager.kick_player(player, Some("cheating".into())).await.unwrap();
        assert_eq!(
            rx.next().await,
            Next::Control(Control::Close(CloseCode::Kicked, "cheating".into()))
        );
        // The connection stays registered until its handler has cleaned up, so the
        // handler can still emit player_disconnected for it.
        assert_eq!(manager.get_player_id(conn).await, Some(player));
        assert!(manager.kick_player(PlayerId::new(), None).await.is_err());
    }
}
