//! Connection handling logic for WebSocket clients.
//!
//! This module contains the core connection handling logic that manages
//! the lifecycle of individual client connections, including WebSocket
//! handshaking, message processing, and cleanup.

use crate::{
    connection::{
        outbox::{Control, Next, OutboxReceiver},
        CloseCode, ConnectionManager,
    },
    error::ServerError,
    messaging::{route_client_frame, Routed, Session},
};
use bytes::Bytes;
use futures::{Sink, SinkExt, StreamExt};
use horizon_event_system::{
    current_timestamp, DisconnectReason, EventSystem, Format, ListenerProfile, Outbound,
    PlayerConnectedEvent, PlayerDisconnectedEvent, PlayerId, Transport,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    accept_async,
    tungstenite::{self, protocol::{frame::coding::CloseCode as WsCloseCode, CloseFrame}, Message, Utf8Bytes},
    WebSocketStream,
};
use tracing::{debug, error, trace, warn};

/// Most frames written before one flush, so a long backlog still flushes regularly.
const MAX_FRAMES_PER_FLUSH: usize = 256;

/// How long the writer waits to deliver a close frame before giving up on the client.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// What a listener accepts, applied to each of its connections.
#[derive(Debug, Clone, Copy)]
pub struct ListenerSettings {
    /// Transport and message format.
    pub profile: ListenerProfile,
    /// Largest frame accepted from a client, in bytes.
    pub max_frame_size: usize,
    /// Undecodable frames tolerated before the connection is closed.
    pub max_decode_errors: u32,
}

impl Default for ListenerSettings {
    fn default() -> Self {
        Self {
            profile: ListenerProfile::new(Transport::WebSocket),
            max_frame_size: horizon_event_system::protocol::DEFAULT_MAX_FRAME_SIZE,
            max_decode_errors: 5,
        }
    }
}

/// Handles a single client connection from establishment to cleanup.
///
/// # Connection Flow
///
/// 1. Perform WebSocket handshake
/// 2. Register the connection, with its own outbox, in the connection manager
/// 3. Generate and assign a player ID
/// 4. Emit player connected event
/// 5. Run the reader (client to plugins) and the writer (outbox to client)
/// 6. When either ends, clean up and emit player disconnected
pub async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    connection_manager: Arc<ConnectionManager>,
    horizon_event_system: Arc<EventSystem>,
    settings: ListenerSettings,
) -> Result<(), ServerError> {
    let ws_stream = accept_async(stream)
        .await
        .map_err(|e| ServerError::Network(format!("WebSocket handshake failed: {e}")))?;
    serve_websocket(ws_stream, addr, connection_manager, horizon_event_system, settings).await
}

/// Why a connection ended, as seen by its handler.
#[derive(Debug)]
enum Ended {
    /// The client closed the connection or its stream ended.
    ByClient,
    /// The server closed it with this code and reason.
    ByServer(CloseCode, String),
    /// Writing to the client failed.
    WriteFailed(String),
    /// Every sender to the connection is gone (server shutting down).
    SendersGone,
}

impl Ended {
    fn disconnect_reason(&self) -> DisconnectReason {
        match self {
            Ended::ByClient => DisconnectReason::ClientDisconnect,
            Ended::ByServer(CloseCode::Kicked, reason) => DisconnectReason::Kicked(reason.clone()),
            Ended::ByServer(CloseCode::SlowClient, _) => DisconnectReason::SlowClient,
            Ended::ByServer(CloseCode::IdleTimeout, _) => DisconnectReason::Timeout,
            Ended::ByServer(code, reason) => {
                DisconnectReason::Error(format!("{} ({}): {}", code.default_reason(), code.code(), reason))
            }
            Ended::WriteFailed(e) => DisconnectReason::Error(e.clone()),
            Ended::SendersGone => DisconnectReason::ServerShutdown,
        }
    }
}

/// Serves an established WebSocket connection until it ends.
///
/// Generic over the stream so the same code serves plain and TLS connections.
pub(crate) async fn serve_websocket<S>(
    ws_stream: WebSocketStream<S>,
    addr: SocketAddr,
    connection_manager: Arc<ConnectionManager>,
    horizon_event_system: Arc<EventSystem>,
    settings: ListenerSettings,
) -> Result<(), ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (connection_id, mut outbox) = connection_manager.open_connection(addr, settings.profile).await;

    // Generate player ID and emit connection event
    let player_id = PlayerId::new();
    connection_manager.set_player_id(connection_id, player_id).await;

    let connected = horizon_event_system
        .emit_core(
            "player_connected",
            &PlayerConnectedEvent {
                player_id,
                connection_id: connection_id.to_string(),
                remote_addr: addr.to_string(),
                timestamp: current_timestamp(),
            },
        )
        .await;
    if let Err(e) = connected {
        connection_manager.remove_connection(connection_id).await;
        return Err(ServerError::Internal(e.to_string()));
    }

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Incoming message task - routes raw messages to plugins
    let incoming_task = {
        let connection_manager = connection_manager.clone();
        let horizon_event_system = horizon_event_system.clone();
        async move {
            let mut session = Session::new(
                connection_id,
                settings.profile,
                settings.max_frame_size,
                settings.max_decode_errors,
            );
            let mut closing = false;
            while let Some(msg) = ws_receiver.next().await {
                let payload = match msg {
                    Ok(Message::Text(text)) => Bytes::from(text),
                    Ok(Message::Binary(data)) => data,
                    Ok(Message::Ping(data)) => {
                        // Answered by the writer, ahead of queued frames.
                        connection_manager.send_control(connection_id, Control::Pong(data));
                        continue;
                    }
                    Ok(Message::Close(_)) => {
                        debug!("🔌 Client {} requested close", connection_id);
                        break;
                    }
                    Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => continue,
                    Err(e) => {
                        debug!("WebSocket error for connection {}: {}", connection_id, e);
                        break;
                    }
                };
                if closing {
                    // The writer is sending the close frame; nothing more is routed.
                    continue;
                }
                let routed =
                    route_client_frame(&payload, &mut session, &connection_manager, &horizon_event_system).await;
                if let Routed::Close(code, reason) = routed {
                    trace!("Closing connection {}: {:?} ({})", connection_id, code, reason);
                    closing = true;
                    let _ = connection_manager.close_connection(connection_id, code, Some(reason));
                }
            }
            Ended::ByClient
        }
    };

    // JSON listeners send text frames, as they always have; every other format binary.
    let text_frames = settings.profile.format == Format::Json;
    let outgoing_task = write_frames(&mut ws_sender, &mut outbox, text_frames);

    // Run both tasks concurrently until one completes
    let ended = tokio::select! {
        ended = incoming_task => ended,
        ended = outgoing_task => ended,
    };
    match &ended {
        Ended::WriteFailed(e) => error!("Failed to write to connection {}: {}", connection_id, e),
        Ended::ByServer(code, reason) => {
            debug!("Connection {} closed by the server: {:?} ({})", connection_id, code, reason)
        }
        _ => {}
    }

    // Emit disconnection event
    let disconnected = match connection_manager.get_player_id(connection_id).await {
        Some(player_id) => horizon_event_system
            .emit_core(
                "player_disconnected",
                &PlayerDisconnectedEvent {
                    player_id,
                    connection_id: connection_id.to_string(),
                    reason: ended.disconnect_reason(),
                    timestamp: current_timestamp(),
                },
            )
            .await
            .map_err(|e| ServerError::Internal(e.to_string())),
        None => Ok(()),
    };

    connection_manager.remove_connection(connection_id).await;
    disconnected
}

/// Writes the connection's outbox to the client until the connection ends.
///
/// Control items go first. Frames already queued are written together and flushed
/// once. A write blocked on a client that stopped reading is abandoned as soon as the
/// outbox cuts the connection.
async fn write_frames<W>(sink: &mut W, outbox: &mut OutboxReceiver, text_frames: bool) -> Ended
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let cut = outbox.cut_signal();
    loop {
        match outbox.next().await {
            Next::Control(Control::Pong(data)) => {
                if let Err(e) = sink.send(Message::Pong(data)).await {
                    return Ended::WriteFailed(e.to_string());
                }
            }
            Next::Control(Control::Close(code, reason)) => {
                send_close(sink, code, &reason).await;
                return Ended::ByServer(code, reason);
            }
            Next::Frame(first) => {
                let mut batch = vec![first];
                while batch.len() < MAX_FRAMES_PER_FLUSH && !outbox.has_control() {
                    match outbox.try_next_frame() {
                        Some(frame) => batch.push(frame),
                        None => break,
                    }
                }
                let written = tokio::select! {
                    written = write_batch(sink, batch, text_frames) => written,
                    _ = cut.cut() => Err(None),
                };
                match written {
                    Ok(()) => {}
                    Err(Some(e)) => return Ended::WriteFailed(e.to_string()),
                    Err(None) => return slow_client(sink).await,
                }
            }
            Next::Cut => return slow_client(sink).await,
            Next::Closed => return Ended::SendersGone,
        }
    }
}

/// Feeds every frame of `batch`, then flushes once. `Err(None)` is never returned here;
/// it stands for "abandoned" in the caller.
async fn write_batch<W>(sink: &mut W, batch: Vec<Outbound>, text_frames: bool) -> Result<(), Option<tungstenite::Error>>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    for frame in batch {
        let message = if text_frames { frame_message(frame) } else { Message::Binary(frame.bytes) };
        sink.feed(message).await.map_err(Some)?;
    }
    sink.flush().await.map_err(Some)
}

async fn slow_client<W>(sink: &mut W) -> Ended
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let code = CloseCode::SlowClient;
    warn!("Closing a connection that does not read its frames: {}", code.default_reason());
    send_close(sink, code, code.default_reason()).await;
    Ended::ByServer(code, code.default_reason().to_string())
}

/// Sends a close frame, giving up after [`CLOSE_TIMEOUT`] on a client that does not read.
async fn send_close<W>(sink: &mut W, code: CloseCode, reason: &str)
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let frame = Message::Close(Some(CloseFrame {
        code: WsCloseCode::from(code.code()),
        reason: Utf8Bytes::from(reason),
    }));
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, sink.send(frame)).await;
}

/// The WebSocket message carrying a frame on a JSON listener: text when it is valid
/// UTF-8 (every JSON frame), binary otherwise. Before, every frame went through `from_utf8_lossy`, which
/// corrupted binary payloads.
fn frame_message(frame: Outbound) -> Message {
    let bytes = frame.bytes;
    if std::str::from_utf8(&bytes).is_ok() {
        // SAFETY: checked just above.
        Message::Text(unsafe { Utf8Bytes::from_bytes_unchecked(bytes) })
    } else {
        Message::Binary(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_frames_go_as_text_and_others_as_binary() {
        let text = frame_message(Outbound::reliable(br#"{"a":1}"#.to_vec()));
        assert_eq!(text, Message::Text(Utf8Bytes::from(r#"{"a":1}"#)));
        let binary = frame_message(Outbound::reliable(vec![0xff, 0x00, 0xfe]));
        assert_eq!(binary, Message::Binary(Bytes::from(vec![0xff, 0x00, 0xfe])));
    }

    #[test]
    fn close_codes_map_to_disconnect_reasons() {
        assert!(matches!(
            Ended::ByServer(CloseCode::Kicked, "bye".into()).disconnect_reason(),
            DisconnectReason::Kicked(r) if r == "bye"
        ));
        assert!(matches!(
            Ended::ByServer(CloseCode::SlowClient, String::new()).disconnect_reason(),
            DisconnectReason::SlowClient
        ));
        assert!(matches!(Ended::ByClient.disconnect_reason(), DisconnectReason::ClientDisconnect));
    }
}
