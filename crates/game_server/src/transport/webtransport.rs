//! WebTransport listener: HTTP/3 over QUIC, one UDP port.
//!
//! Each session carries all reliable frames in both directions on one bidirectional
//! stream the client opens right after the session is accepted, each frame preceded by
//! its length ([`horizon_protocol::stream`]). Unreliable frames travel as datagrams,
//! several small ones packed into one batch when they wait together.
//!
//! Unreliable frames must not wait: a position sent late is worse than one dropped,
//! since a newer one follows. So an unreliable frame is dropped when the connection is
//! behind (its outbox backlog holds frames), and when it waited longer than
//! `unreliable_max_age`. quinn's own datagram buffer is kept small for the same
//! reason; it drops its oldest datagrams when full. A frame too large for a datagram
//! is sent on the reliable stream instead.

use crate::{
    connection::{
        manager::{DeliveryStats, TransportInfo},
        outbox::{Control, Next, OutboxReceiver, Queued},
        CloseCode, ConnectionId, ConnectionManager,
    },
    error::ServerError,
    messaging::{route_client_frame, Routed, Session},
    server::handlers::ListenerSettings,
    transport::tls::{watch, TlsFiles},
};
use bytes::Bytes;
use horizon_event_system::{
    current_timestamp,
    protocol::{encode_server, json, stream as framing, ServerFrame},
    Delivery, DisconnectReason, EventSystem, Format, PlayerConnectedEvent, PlayerDisconnectedEvent, PlayerId,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use wtransport::{endpoint::IncomingSession, Connection, Endpoint, Identity, ServerConfig, VarInt};

/// Settings of the WebTransport listener.
#[derive(Debug, Clone)]
pub struct WebTransportSettings {
    /// UDP address to listen on.
    pub bind_address: SocketAddr,
    /// TLS certificate and key (required by WebTransport).
    pub tls: TlsFiles,
    /// How often the certificate files are checked for changes.
    pub reload_interval: Duration,
    /// Close sessions without traffic for this long.
    pub idle_timeout: Duration,
    /// Send QUIC keep-alives this often.
    pub keepalive: Duration,
    /// Cap on the datagram size (the path's own limit may be lower).
    pub max_datagram_size: usize,
    /// quinn's buffer of datagrams waiting for congestion control, in bytes.
    pub datagram_send_buffer_bytes: usize,
    /// Unreliable frames older than this are dropped instead of sent.
    pub unreliable_max_age: Duration,
    /// How long the client may take to open its reliable stream.
    pub stream_timeout: Duration,
}

/// What the transport knows about a session.
#[derive(Debug)]
struct SessionInfo {
    connection: Connection,
    max_datagram_size: usize,
}

impl TransportInfo for SessionInfo {
    fn rtt(&self) -> Option<Duration> {
        Some(self.connection.rtt())
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.connection
            .max_datagram_size()
            .map(|size| size.min(self.max_datagram_size))
    }
}

/// The QUIC server configuration for these settings.
async fn server_config(settings: &WebTransportSettings) -> Result<ServerConfig, ServerError> {
    let identity = Identity::load_pemfiles(&settings.tls.cert_path, &settings.tls.key_path)
        .await
        .map_err(|e| ServerError::Internal(format!("Cannot load the WebTransport certificate: {e}")))?;
    let mut transport = wtransport::config::QuicTransportConfig::default();
    let idle = wtransport::quinn::IdleTimeout::try_from(settings.idle_timeout)
        .map_err(|e| ServerError::Internal(format!("Invalid idle timeout: {e}")))?;
    transport
        .max_idle_timeout(Some(idle))
        .keep_alive_interval(Some(settings.keepalive))
        .datagram_send_buffer_size(settings.datagram_send_buffer_bytes);
    Ok(ServerConfig::builder()
        .with_bind_address(settings.bind_address)
        .with_custom_transport(identity, transport)
        .build())
}

/// Runs the WebTransport listener until the server shuts down.
pub async fn run(
    settings: WebTransportSettings,
    listener: ListenerSettings,
    connection_manager: Arc<ConnectionManager>,
    events: Arc<EventSystem>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), ServerError> {
    let endpoint = Arc::new(
        Endpoint::server(server_config(&settings).await?)
            .map_err(|e| ServerError::Network(format!("Cannot bind WebTransport on {}: {e}", settings.bind_address)))?,
    );
    info!(
        "🌐 WebTransport listener on udp://{} speaks {}, compression {}",
        settings.bind_address, listener.profile.format, listener.profile.compression
    );

    // New sessions get the new certificate; established ones keep theirs.
    let weak = Arc::downgrade(&endpoint);
    let reload_settings = settings.clone();
    watch(settings.tls.clone(), settings.reload_interval, move || {
        let Some(endpoint) = weak.upgrade() else { return false };
        let reload_settings = reload_settings.clone();
        tokio::spawn(async move {
            match server_config(&reload_settings).await {
                Ok(config) => match endpoint.reload_config(config, false) {
                    Ok(()) => info!("🔐 Reloaded the WebTransport certificate"),
                    Err(e) => warn!("Keeping the current WebTransport certificate: {}", e),
                },
                Err(e) => warn!("Keeping the current WebTransport certificate: {}", e),
            }
        });
        true
    });

    let settings = Arc::new(settings);
    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let settings = settings.clone();
                let listener = listener.clone();
                let connection_manager = connection_manager.clone();
                let events = events.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_session(incoming, settings, listener, connection_manager, events).await {
                        debug!("WebTransport session ended: {}", e);
                    }
                });
            }
            _ = shutdown.recv() => {
                endpoint.close(VarInt::from_u32(0), b"server shutting down");
                return Ok(());
            }
        }
    }
}

async fn handle_session(
    incoming: IncomingSession,
    settings: Arc<WebTransportSettings>,
    listener: ListenerSettings,
    connection_manager: Arc<ConnectionManager>,
    events: Arc<EventSystem>,
) -> Result<(), ServerError> {
    let request = incoming
        .await
        .map_err(|e| ServerError::Network(format!("WebTransport handshake failed: {e}")))?;
    let addr = request.remote_address();
    if listener.banned_ips.contains(&addr.ip()) {
        request.forbidden().await;
        return Ok(());
    }
    let connection = request
        .accept()
        .await
        .map_err(|e| ServerError::Network(format!("WebTransport session refused: {e}")))?;
    if connection_manager.connection_count() >= listener.max_connections {
        close(&connection, CloseCode::ServerFull, CloseCode::ServerFull.default_reason());
        return Ok(());
    }
    serve(connection, addr, settings, listener, connection_manager, events).await
}

fn close(connection: &Connection, code: CloseCode, reason: &str) {
    connection.close(VarInt::from_u32(code.code() as u32), reason.as_bytes());
}

/// Why a session ended.
#[derive(Debug)]
enum Ended {
    ByClient(String),
    ByServer(CloseCode, String),
    TimedOut,
    SendersGone,
}

impl Ended {
    fn disconnect_reason(&self) -> DisconnectReason {
        match self {
            Ended::ByClient(_) => DisconnectReason::ClientDisconnect,
            Ended::ByServer(CloseCode::Kicked, reason) => DisconnectReason::Kicked(reason.clone()),
            Ended::ByServer(CloseCode::SlowClient, _) => DisconnectReason::SlowClient,
            Ended::ByServer(code, reason) => {
                DisconnectReason::Error(format!("{} ({}): {}", code.default_reason(), code.code(), reason))
            }
            Ended::TimedOut => DisconnectReason::Timeout,
            Ended::SendersGone => DisconnectReason::ServerShutdown,
        }
    }
}

async fn serve(
    connection: Connection,
    addr: SocketAddr,
    settings: Arc<WebTransportSettings>,
    listener: ListenerSettings,
    connection_manager: Arc<ConnectionManager>,
    events: Arc<EventSystem>,
) -> Result<(), ServerError> {
    let (send, recv) = match tokio::time::timeout(settings.stream_timeout, connection.accept_bi()).await {
        Ok(Ok(streams)) => streams,
        Ok(Err(e)) => return Err(ServerError::Network(format!("No reliable stream: {e}"))),
        Err(_) => {
            close(&connection, CloseCode::SettingsMismatch, "open a bidirectional stream first");
            return Ok(());
        }
    };

    let (connection_id, outbox) = connection_manager
        .open_connection_with(addr, listener.profile, listener.compression.clone())
        .await;
    connection_manager.set_transport_info(
        connection_id,
        Arc::new(SessionInfo { connection: connection.clone(), max_datagram_size: settings.max_datagram_size }),
    );
    let player_id = PlayerId::new();
    connection_manager.set_player_id(connection_id, player_id).await;
    let connected = events
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

    let ended = tokio::select! {
        ended = read_frames(&connection, recv, connection_id, &listener, &connection_manager, &events) => ended,
        ended = write_frames(&connection, send, outbox, &listener, &settings, &connection_manager) => ended,
    };
    match &ended {
        Ended::ByServer(code, reason) => {
            debug!("WebTransport connection {} closed by the server: {:?} ({})", connection_id, code, reason)
        }
        Ended::ByClient(reason) => debug!("WebTransport connection {} ended: {}", connection_id, reason),
        _ => {}
    }

    let disconnected = match connection_manager.get_player_id(connection_id).await {
        Some(player_id) => events
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

/// Reads the reliable stream and the datagrams, and routes their frames.
async fn read_frames(
    connection: &Connection,
    mut recv: wtransport::RecvStream,
    connection_id: ConnectionId,
    listener: &ListenerSettings,
    connection_manager: &ConnectionManager,
    events: &EventSystem,
) -> Ended {
    // The stream is read by its own task: a length-prefixed read cut in the middle by
    // a datagram would lose bytes.
    let (frames_tx, mut frames_rx) = mpsc::channel::<Result<Vec<u8>, String>>(256);
    // Frames carry a header byte on listeners with compression.
    let max_wire = listener.max_frame_size + 1;
    let reader = tokio::spawn(async move {
        let mut splitter = framing::FrameSplitter::new();
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            let read = match recv.read(&mut buffer).await {
                Ok(Some(read)) => read,
                Ok(None) => return,
                Err(e) => {
                    let _ = frames_tx.send(Err(e.to_string())).await;
                    return;
                }
            };
            splitter.push(&buffer[..read]);
            loop {
                match splitter.next_frame(max_wire) {
                    Ok(Some(frame)) => {
                        if frames_tx.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = frames_tx.send(Err(e.to_string())).await;
                        return;
                    }
                }
            }
        }
    });

    let mut session = Session::new(
        connection_id,
        listener.profile,
        listener.max_frame_size,
        listener.max_decode_errors,
    )
    .with_compression(listener.compression.clone());
    let mut closing = false;
    let ended = loop {
        let bytes: Bytes = tokio::select! {
            frame = frames_rx.recv() => match frame {
                Some(Ok(frame)) => Bytes::from(frame),
                Some(Err(e)) if e.contains("exceeds") => {
                    let _ = connection_manager.close_connection(connection_id, CloseCode::MessageTooLarge, None);
                    closing = true;
                    continue;
                }
                Some(Err(e)) => break Ended::ByClient(e),
                None => break Ended::ByClient("stream finished".into()),
            },
            datagram = connection.receive_datagram() => match datagram {
                Ok(datagram) => datagram.payload(),
                Err(wtransport::error::ConnectionError::TimedOut) => break Ended::TimedOut,
                Err(e) => break Ended::ByClient(e.to_string()),
            },
        };
        if closing {
            continue;
        }
        if let Routed::Close(code, reason) = route_client_frame(&bytes, &mut session, connection_manager, events).await {
            closing = true;
            let _ = connection_manager.close_connection(connection_id, code, Some(reason));
        }
    };
    reader.abort();
    ended
}

/// Writes the outbox: reliable frames on the stream, unreliable ones as datagrams.
async fn write_frames(
    connection: &Connection,
    mut send: wtransport::SendStream,
    mut outbox: OutboxReceiver,
    listener: &ListenerSettings,
    settings: &WebTransportSettings,
    connection_manager: &ConnectionManager,
) -> Ended {
    let cut = outbox.cut_signal();
    let format = listener.profile.format;
    let compression = &listener.compression;
    let stats = connection_manager.delivery_stats();
    loop {
        match outbox.next().await {
            // QUIC answers its own pings; nothing to do at this level.
            Next::Control(Control::Pong(_)) => {}
            Next::Control(Control::Close(code, reason)) => {
                let frame = disconnect_frame(format, code, &reason);
                let mut bytes = Vec::new();
                framing::write_frame(&mut bytes, &compression.settings().wrap(&frame));
                // Closing the session drops unsent data: deliver the notice first.
                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                    send.write_all(&bytes).await?;
                    send.finish().await
                })
                .await;
                close(connection, code, &reason);
                return Ended::ByServer(code, reason);
            }
            Next::Frame(first) => {
                if !first.wrapped && compression.has_header() && !listener.batch_delay.is_zero() {
                    tokio::time::sleep(listener.batch_delay).await;
                }
                let mut batch = vec![first];
                while batch.len() < 256 && !outbox.has_control() {
                    match outbox.try_next_frame() {
                        Some(frame) => batch.push(frame),
                        None => break,
                    }
                }
                let max_datagram = connection
                    .max_datagram_size()
                    .map(|size| size.min(settings.max_datagram_size));
                let plan = plan_frames(
                    batch,
                    outbox.is_behind(),
                    Instant::now(),
                    settings.unreliable_max_age,
                    max_datagram,
                    stats,
                );
                for datagram in datagrams(format, compression, plan.unreliable, max_datagram, stats) {
                    match datagram {
                        Ok(bytes) => match connection.send_datagram(&bytes) {
                            Ok(()) => DeliveryStats::count(&stats.datagrams_sent),
                            Err(e) => debug!("Datagram not sent: {}", e),
                        },
                        // Too large for a datagram after all: the stream takes it.
                        Err(frame) => {
                            DeliveryStats::count(&stats.sent_reliably);
                            write_reliable(&mut send, compression, format, vec![(frame, false)], &cut).await;
                        }
                    }
                }
                if !plan.reliable.is_empty() {
                    match write_reliable(&mut send, compression, format, plan.reliable, &cut).await {
                        WriteResult::Written => {}
                        WriteResult::Failed(e) => return Ended::ByClient(e),
                        WriteResult::Cut => {
                            close(connection, CloseCode::SlowClient, CloseCode::SlowClient.default_reason());
                            return Ended::ByServer(CloseCode::SlowClient, CloseCode::SlowClient.default_reason().into());
                        }
                    }
                }
            }
            Next::Cut => {
                close(connection, CloseCode::SlowClient, CloseCode::SlowClient.default_reason());
                return Ended::ByServer(CloseCode::SlowClient, CloseCode::SlowClient.default_reason().into());
            }
            Next::Closed => return Ended::SendersGone,
        }
    }
}

enum WriteResult {
    Written,
    Failed(String),
    Cut,
}

async fn write_reliable(
    send: &mut wtransport::SendStream,
    compression: &crate::connection::ListenerCompression,
    format: Format,
    frames: Vec<(Bytes, bool)>,
    cut: &crate::connection::outbox::CutSignal,
) -> WriteResult {
    let mut bytes = Vec::new();
    for frame in compression.wire_frames(format, frames) {
        framing::write_frame(&mut bytes, &frame);
    }
    tokio::select! {
        written = send.write_all(&bytes) => match written {
            Ok(()) => WriteResult::Written,
            Err(e) => {
                error!("Failed to write to a WebTransport stream: {}", e);
                WriteResult::Failed(e.to_string())
            }
        },
        _ = cut.cut() => WriteResult::Cut,
    }
}

/// The frames of one writer pass, sorted by how they travel.
#[derive(Debug, Default)]
struct Plan {
    /// For the stream, in order: (bytes, wrapped).
    reliable: Vec<(Bytes, bool)>,
    /// Candidates for datagrams: (bytes, wrapped).
    unreliable: Vec<(Bytes, bool)>,
}

/// Sorts frames: reliable ones for the stream; unreliable ones for datagrams, unless
/// the connection is behind or they are too old (dropped), or there are no datagrams
/// (sent reliably).
fn plan_frames(
    batch: Vec<Queued>,
    behind: bool,
    now: Instant,
    max_age: Duration,
    max_datagram: Option<usize>,
    stats: &DeliveryStats,
) -> Plan {
    let mut plan = Plan::default();
    for queued in batch {
        let frame = (queued.frame.bytes, queued.wrapped);
        if queued.frame.delivery == Delivery::Reliable {
            plan.reliable.push(frame);
        } else if max_datagram.is_none() {
            DeliveryStats::count(&stats.sent_reliably);
            plan.reliable.push(frame);
        } else if behind {
            DeliveryStats::count(&stats.dropped_behind);
        } else if now.duration_since(queued.queued_at) > max_age {
            DeliveryStats::count(&stats.dropped_too_old);
        } else {
            plan.unreliable.push(frame);
        }
    }
    plan
}

/// Packs unreliable frames into datagrams: small ones together in batches that fit,
/// each datagram wrapped for the wire. A frame that cannot fit alone comes back as
/// `Err` for the stream.
fn datagrams(
    format: Format,
    compression: &crate::connection::ListenerCompression,
    frames: Vec<(Bytes, bool)>,
    max_datagram: Option<usize>,
    stats: &DeliveryStats,
) -> Vec<Result<Bytes, Bytes>> {
    let Some(max) = max_datagram else {
        return frames.into_iter().map(|(bytes, _)| Err(bytes)).collect();
    };
    // Room for the header byte and the batch's own overhead.
    let budget = max.saturating_sub(16);
    let mut out = Vec::new();
    let mut group: Vec<Bytes> = Vec::new();
    let mut group_bytes = 0;
    let flush = |group: &mut Vec<Bytes>, group_bytes: &mut usize, out: &mut Vec<Result<Bytes, Bytes>>| {
        *group_bytes = 0;
        match group.len() {
            0 => {}
            1 => out.push(Ok(compression.settings().wrap(&group.pop().unwrap_or_default()))),
            _ => {
                let batch = crate::connection::compress::batch(format, group);
                group.clear();
                out.push(Ok(compression.settings().wrap(&batch)));
            }
        }
    };
    for (bytes, wrapped) in frames {
        if wrapped {
            // Already wrapped for the wire (large and compressed once): alone.
            if bytes.len() <= max {
                out.push(Ok(bytes));
            } else {
                DeliveryStats::count(&stats.sent_reliably);
                out.push(Err(bytes));
            }
            continue;
        }
        if bytes.len() > budget {
            flush(&mut group, &mut group_bytes, &mut out);
            out.push(Err(bytes));
            continue;
        }
        if group_bytes + bytes.len() + 4 > budget {
            flush(&mut group, &mut group_bytes, &mut out);
        }
        group_bytes += bytes.len() + 4;
        group.push(bytes);
    }
    flush(&mut group, &mut group_bytes, &mut out);
    out
}

/// The frame telling a client why it is being disconnected.
fn disconnect_frame(format: Format, code: CloseCode, reason: &str) -> Vec<u8> {
    match format {
        Format::Bitcode => encode_server(&ServerFrame::Disconnect { code: code.code(), reason: reason.to_string() }).to_vec(),
        Format::Json => serde_json::to_vec(&json::Disconnect { kind: json::Disconnect::TYPE, code: code.code(), reason })
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ListenerCompression;
    use horizon_event_system::Outbound;

    fn queued(bytes: &'static [u8], delivery: Delivery, age: Duration) -> Queued {
        Queued {
            frame: Outbound { bytes: Bytes::from_static(bytes), delivery },
            wrapped: false,
            queued_at: Instant::now() - age,
        }
    }

    #[test]
    fn unreliable_frames_are_dropped_when_behind_or_too_old() {
        let stats = DeliveryStats::default();
        let max_age = Duration::from_millis(100);
        let batch = || {
            vec![
                queued(b"reliable", Delivery::Reliable, Duration::ZERO),
                queued(b"fresh", Delivery::Unreliable, Duration::ZERO),
                queued(b"stale", Delivery::Unreliable, Duration::from_millis(500)),
            ]
        };

        let plan = plan_frames(batch(), false, Instant::now(), max_age, Some(1200), &stats);
        assert_eq!(plan.reliable.len(), 1);
        assert_eq!(plan.unreliable, vec![(Bytes::from_static(b"fresh"), false)]);
        assert_eq!(DeliveryStats::get(&stats.dropped_too_old), 1);

        // Behind: no unreliable frame competes with the backlog.
        let plan = plan_frames(batch(), true, Instant::now(), max_age, Some(1200), &stats);
        assert!(plan.unreliable.is_empty());
        assert_eq!(DeliveryStats::get(&stats.dropped_behind), 2);

        // No datagrams: unreliable frames go reliably, in order, never dropped.
        let plan = plan_frames(batch(), false, Instant::now(), max_age, None, &stats);
        assert_eq!(plan.reliable.len(), 3);
        assert_eq!(DeliveryStats::get(&stats.sent_reliably), 2);
    }

    #[test]
    fn small_unreliable_frames_share_datagrams() {
        let stats = DeliveryStats::default();
        let frames: Vec<(Bytes, bool)> = (0..100u32)
            .map(|i| (encode_server(&ServerFrame::Pong { nonce: i }), false))
            .collect();
        let out = datagrams(Format::Bitcode, &ListenerCompression::none(), frames, Some(1200), &stats);
        assert!(out.len() < 10, "{} datagrams for 100 frames", out.len());
        let mut nonces = Vec::new();
        for datagram in out {
            let bytes = datagram.unwrap();
            assert!(bytes.len() <= 1200);
            match horizon_event_system::protocol::decode_server(&bytes, 0).unwrap() {
                ServerFrame::Batch { frames } => {
                    for frame in ServerFrame::unbatch(&frames).unwrap() {
                        if let ServerFrame::Pong { nonce } = frame {
                            nonces.push(nonce);
                        }
                    }
                }
                ServerFrame::Pong { nonce } => nonces.push(nonce),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(nonces, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn frames_too_large_for_a_datagram_go_to_the_stream() {
        let stats = DeliveryStats::default();
        let big = Bytes::from(vec![7u8; 5000]);
        let out = datagrams(Format::Bitcode, &ListenerCompression::none(), vec![(big.clone(), false)], Some(1200), &stats);
        assert_eq!(out, vec![Err(big)]);
    }
}
