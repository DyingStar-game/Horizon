/// Client connection and response handling
use crate::events::EventError;
use crate::types::{PlayerId, AuthenticationStatus};
// use serde::{Deserialize, Serialize}; // Unused
use horizon_protocol::{json, payload, EventKey, Format, ServerFrame};
use std::net::SocketAddr;
use std::sync::Arc;

/// Connection-aware client reference that provides handlers with access to the client connection
/// and methods to respond directly to that specific client.
#[derive(Clone)]
pub struct ClientConnectionRef {
    /// The player ID associated with this connection
    pub player_id: PlayerId,
    /// The remote address of the client
    pub remote_addr: SocketAddr,
    /// Connection ID for internal tracking
    pub connection_id: String,
    /// Timestamp when the connection was established
    pub connected_at: u64,
    /// Current authentication status of the connection
    pub auth_status: AuthenticationStatus,
    /// Message format of the client's listener
    format: Format,
    /// Sender for direct response to this specific client
    response_sender: Arc<dyn ClientResponseSender + Send + Sync>,
}

impl std::fmt::Debug for ClientConnectionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConnectionRef")
            .field("player_id", &self.player_id)
            .field("remote_addr", &self.remote_addr)
            .field("connection_id", &self.connection_id)
            .field("connected_at", &self.connected_at)
            .field("auth_status", &self.auth_status)
            .field("format", &self.format)
            .field("response_sender", &"[response_sender]")
            .finish()
    }
}

impl ClientConnectionRef {
    /// Kick (disconnect) this client from the server, with an optional reason
    pub async fn kick(&self, reason: Option<String>) -> Result<(), EventError> {
        self.response_sender
            .kick(self.player_id, reason)
            .await
            .map_err(|e| EventError::HandlerExecution(format!("Failed to kick client: {}", e)))
    }
    /// Creates a new client connection reference
    pub fn new(
        player_id: PlayerId,
        remote_addr: SocketAddr,
        connection_id: String,
        connected_at: u64,
        auth_status: AuthenticationStatus,
        response_sender: Arc<dyn ClientResponseSender + Send + Sync>,
    ) -> Self {
        Self {
            player_id,
            remote_addr,
            connection_id,
            connected_at,
            auth_status,
            format: Format::Json,
            response_sender,
        }
    }

    /// The same connection, speaking `format`.
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    /// The message format of the client's listener.
    pub fn format(&self) -> Format {
        self.format
    }

    /// Gets the current authentication status of this connection
    pub fn auth_status(&self) -> AuthenticationStatus {
        self.auth_status
    }

    /// Checks if the connection is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.auth_status == AuthenticationStatus::Authenticated
    }

    /// Send a direct response to this specific client
    pub async fn respond(&self, data: &[u8]) -> Result<(), EventError> {
        self.response_sender
            .send_to_client(self.player_id, data.to_vec())
            .await
            .map_err(|e| EventError::HandlerExecution(format!("Failed to send response: {}", e)))
    }

    /// Send a JSON response to this specific client
    pub async fn respond_json<T: serde::Serialize>(&self, data: &T) -> Result<(), EventError> {
        let json = serde_json::to_vec(data)
            .map_err(|e| EventError::HandlerExecution(format!("JSON serialization failed: {}", e)))?;
        self.respond(&json).await
    }

    /// Send a client event to this client, encoded in its listener's format: the
    /// `{namespace, event, data}` envelope in JSON, a [`ServerFrame::Event`] in bitcode
    /// (payload through serde).
    ///
    /// [`ServerFrame::Event`]: horizon_protocol::ServerFrame::Event
    pub async fn respond_event<T: serde::Serialize>(&self, namespace: &str, event: &str, data: &T) -> Result<(), EventError> {
        let frame = encode_event(self.format, namespace, event, data)?;
        self.send_event_frame(frame).await
    }

    /// Like [`respond_event`](Self::respond_event), with the bitcode derive for bitcode
    /// clients: for hot types.
    pub async fn respond_event_packed<T>(&self, namespace: &str, event: &str, data: &T) -> Result<(), EventError>
    where
        T: serde::Serialize + horizon_protocol::bitcode::Encode,
    {
        let frame = match self.format {
            Format::Json => encode_event(Format::Json, namespace, event, data)?,
            Format::Bitcode => {
                let payload = horizon_protocol::bitcode::encode(data);
                event_frame_bitcode(namespace, event, payload)
            }
        };
        self.send_event_frame(frame).await
    }

    async fn send_event_frame(&self, frame: bytes::Bytes) -> Result<(), EventError> {
        self.response_sender
            .send_frame(self.player_id, Outbound::reliable(frame))
            .await
            .map_err(|e| EventError::HandlerExecution(format!("Failed to send response: {}", e)))
    }

    /// Check if this connection is still active
    pub async fn is_active(&self) -> bool {
        self.response_sender.is_connection_active(self.player_id).await
    }
}

/// Trait for sending responses to clients - implemented by the server/connection manager
pub trait ClientResponseSender: std::fmt::Debug {
    /// Send data to a specific client
    fn send_to_client(&self, player_id: PlayerId, data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>;

    /// Check if a client connection is still active
    fn is_connection_active(&self, player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>;

    /// Get the authentication status of a client
    fn get_auth_status(&self, player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<AuthenticationStatus>> + Send + '_>>;

    /// Kick (disconnect) a client by player ID, sending a close frame and removing the connection.
    fn kick(&self, player_id: PlayerId, reason: Option<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>;

    /// Broadcast data to all connected clients
    fn broadcast_to_all(&self, _data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send + '_>> {
        // Default implementation that provides a working fallback - returns 0 clients reached
        // Individual implementations should override this with actual broadcast functionality
        Box::pin(async move { Ok(0) })
    }

    /// Get connection information for a client (optional implementation)
    fn get_connection_info(&self, _player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<ClientConnectionInfo>> + Send + '_>> {
        // Default implementation returns None to maintain backwards compatibility
        Box::pin(async move { None::<ClientConnectionInfo> })
    }

    /// Send an already encoded frame to a client, with its delivery class.
    ///
    /// The bytes are shared, so the same frame can be queued for many players without
    /// copying it. The default implementation falls back to [`send_to_client`], which is
    /// reliable; senders that know the transport override it.
    ///
    /// [`send_to_client`]: ClientResponseSender::send_to_client
    fn send_frame(&self, player_id: PlayerId, frame: Outbound) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        self.send_to_client(player_id, frame.bytes.to_vec())
    }

    /// The listener a client is connected through, or `None` if unknown.
    fn connection_profile(&self, _player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<ListenerProfile>> + Send + '_>> {
        Box::pin(async move { None })
    }

    /// The round-trip time measured by the transport, when it measures one.
    fn connection_rtt(&self, _player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<std::time::Duration>> + Send + '_>> {
        Box::pin(async move { None })
    }

    /// The largest frame that fits in one unreliable datagram, when the transport has them.
    fn max_datagram_size(&self, _player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<usize>> + Send + '_>> {
        Box::pin(async move { None })
    }
}

/// How a frame travels to the client.
///
/// Unreliable frames are "latest wins" data, such as positions: they may be dropped, and
/// a transport without datagrams sends them reliably instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    /// Delivered once, in order.
    #[default]
    Reliable,
    /// May be dropped or arrive out of order; a newer frame replaces it.
    Unreliable,
}

/// An encoded frame queued for one client.
///
/// `bytes` is reference counted: cloning an `Outbound` to fan it out to many players does
/// not copy the frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    /// The frame as it goes on the wire.
    pub bytes: bytes::Bytes,
    /// How the frame should travel.
    pub delivery: Delivery,
}

impl Outbound {
    /// A frame delivered once, in order.
    pub fn reliable(bytes: impl Into<bytes::Bytes>) -> Self {
        Self { bytes: bytes.into(), delivery: Delivery::Reliable }
    }

    /// A frame that may be dropped and replaced by a newer one.
    pub fn unreliable(bytes: impl Into<bytes::Bytes>) -> Self {
        Self { bytes: bytes.into(), delivery: Delivery::Unreliable }
    }

    /// The frame's size on the wire, in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the frame is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// The transport a listener accepts clients on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// WebSocket over TCP: every frame is reliable.
    #[default]
    WebSocket,
    /// WebTransport over QUIC: reliable streams and unreliable datagrams.
    WebTransport,
}

/// What a client's listener is configured for. New settings are added as fields, so
/// the struct is non-exhaustive: read its fields, build it with [`ListenerProfile::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct ListenerProfile {
    /// The transport the client is connected through.
    pub transport: Transport,
    /// The message format of the listener.
    pub format: Format,
    /// The compression codec of the listener.
    pub compression: horizon_protocol::Codec,
}

impl ListenerProfile {
    /// A profile for the given transport, every other setting at its default.
    pub fn new(transport: Transport) -> Self {
        Self { transport, format: Format::Json, compression: horizon_protocol::Codec::None }
    }

    /// The same profile with another compression codec.
    pub fn with_compression(mut self, codec: horizon_protocol::Codec) -> Self {
        self.compression = codec;
        self
    }

    /// The same profile with another message format.
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    /// Whether unreliable frames can travel as datagrams on this listener.
    pub fn has_datagrams(&self) -> bool {
        self.transport == Transport::WebTransport
    }
}

/// Information about a client connection
#[derive(Debug, Clone)]
pub struct ClientConnectionInfo {
    pub player_id: PlayerId,
    pub remote_addr: std::net::SocketAddr,
    pub connection_id: String,
    pub connected_at: u64,
    pub auth_status: AuthenticationStatus,
}
/// A client event frame in `format`.
pub(crate) fn encode_event<T: serde::Serialize + ?Sized>(
    format: Format,
    namespace: &str,
    event: &str,
    data: &T,
) -> Result<bytes::Bytes, EventError> {
    match format {
        Format::Json => {
            let envelope = json::Event { namespace, event, data };
            Ok(bytes::Bytes::from(serde_json::to_vec(&envelope)?))
        }
        Format::Bitcode => {
            let payload = payload::encode_serde(Format::Bitcode, data)
                .map_err(|e| EventError::HandlerExecution(e.to_string()))?;
            Ok(event_frame_bitcode(namespace, event, payload))
        }
    }
}

fn event_frame_bitcode(namespace: &str, event: &str, payload: Vec<u8>) -> bytes::Bytes {
    horizon_protocol::encode_server(&ServerFrame::Event {
        key: EventKey::Name(format!("{namespace}:{event}")),
        payload,
    })
}
