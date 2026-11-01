//! End-to-end tests of the WebTransport listener, TLS on WebSocket and connection
//! limits, with real clients and self-signed certificates made for each test.

use crate::ws_tests::{next_data, within, TestServer};
use futures::{SinkExt, StreamExt};
use horizon_event_system::{
    protocol::{decode_server, encode_client, stream::FrameSplitter, ClientFrame, EventKey, ServerFrame, PROTOCOL_VERSION},
    protocol::stream as framing,
    protocol::encode_server,
    Format, Outbound,
};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use wtransport::{tls::Sha256Digest, ClientConfig, Endpoint, Identity};

struct Certificate {
    dir: tempfile::TempDir,
    hash: Sha256Digest,
}

impl Certificate {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let hash = write_identity(dir.path()).await;
        Self { dir, hash }
    }

    fn cert_path(&self) -> PathBuf {
        self.dir.path().join("cert.pem")
    }

    fn key_path(&self) -> PathBuf {
        self.dir.path().join("key.pem")
    }
}

/// Writes a fresh self-signed certificate and key; returns the certificate's hash.
async fn write_identity(dir: &Path) -> Sha256Digest {
    let identity = Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
    identity.certificate_chain().store_pemfile(dir.join("cert.pem")).await.unwrap();
    identity.private_key().store_secret_pemfile(dir.join("key.pem")).await.unwrap();
    identity.certificate_chain().as_slice()[0].hash()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("0.0.0.0:0").unwrap().local_addr().unwrap().port()
}

async fn webtransport_server(cert: &Certificate) -> (TestServer, u16) {
    let port = free_udp_port();
    let (cert_path, key_path) = (cert.cert_path(), cert.key_path());
    let server = TestServer::start_with(move |config| {
        config.webtransport.enabled = true;
        config.webtransport.bind_address = ([0, 0, 0, 0], port).into();
        config.webtransport.protocol = Format::Bitcode;
        config.webtransport.cert_path = Some(cert_path);
        config.webtransport.key_path = Some(key_path);
        config.webtransport.cert_reload_interval_ms = 100;
    })
    .await;
    (server, port)
}

/// A WebTransport client with its reliable stream open.
struct Client {
    connection: wtransport::Connection,
    send: wtransport::SendStream,
    recv: wtransport::RecvStream,
    splitter: FrameSplitter,
    _endpoint: Endpoint<wtransport::endpoint::endpoint_side::Client>,
}

impl Client {
    async fn connect(port: u16, hash: &Sha256Digest) -> Result<Self, String> {
        let config = ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([hash.clone()])
            .build();
        let endpoint = Endpoint::client(config).map_err(|e| e.to_string())?;
        // The listener starts right after the WebSocket one: retry briefly.
        let mut last = String::new();
        for _ in 0..50 {
            match endpoint.connect(format!("https://127.0.0.1:{port}/")).await {
                Ok(connection) => {
                    let (send, recv) = connection.open_bi().await.map_err(|e| e.to_string())?.await.map_err(|e| e.to_string())?;
                    return Ok(Self { connection, send, recv, splitter: FrameSplitter::new(), _endpoint: endpoint });
                }
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(last)
    }

    async fn send(&mut self, frame: &ClientFrame) {
        let mut bytes = Vec::new();
        framing::write_frame(&mut bytes, &encode_client(frame));
        self.send.write_all(&bytes).await.unwrap();
    }

    async fn next_stream_frame(&mut self) -> ServerFrame {
        within(async {
            loop {
                if let Some(frame) = self.splitter.next_frame(1 << 20).unwrap() {
                    return decode_server(&frame, 0).unwrap();
                }
                let mut buffer = [0u8; 4096];
                let read = self.recv.read(&mut buffer).await.unwrap().expect("stream open");
                self.splitter.push(&buffer[..read]);
            }
        })
        .await
    }

    async fn hello(&mut self) {
        self.send(&ClientFrame::Hello { protocol_version: PROTOCOL_VERSION, client: "wt-test".into(), dictionary_id: None })
            .await;
        assert!(matches!(self.next_stream_frame().await, ServerFrame::HelloAck { .. }));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn webtransport_clients_handshake_and_ping_over_the_stream() {
    let cert = Certificate::new().await;
    let (mut server, port) = webtransport_server(&cert).await;
    let mut client = Client::connect(port, &cert.hash).await.unwrap();
    client.hello().await;
    let player = within(server.connected.recv()).await.unwrap();

    client.send(&ClientFrame::Ping { nonce: 11 }).await;
    assert_eq!(client.next_stream_frame().await, ServerFrame::Pong { nonce: 11 });

    // QUIC measures the round trip; plugins can read it.
    assert!(server.sender().connection_rtt(player).await.is_some());
    assert!(server.sender().max_datagram_size(player).await.unwrap() <= 1200);
}

#[tokio::test(flavor = "multi_thread")]
async fn unreliable_frames_travel_as_datagrams() {
    let cert = Certificate::new().await;
    let (mut server, port) = webtransport_server(&cert).await;
    let mut client = Client::connect(port, &cert.hash).await.unwrap();
    client.hello().await;
    let player = within(server.connected.recv()).await.unwrap();

    let frame = encode_server(&ServerFrame::Pong { nonce: 99 });
    server.sender().send_frame(player, Outbound::unreliable(frame)).await.unwrap();
    let datagram = within(client.connection.receive_datagram()).await.unwrap();
    let frame = decode_server(&datagram.payload(), 0).unwrap();
    let frames = match frame {
        ServerFrame::Batch { frames } => ServerFrame::unbatch(&frames).unwrap(),
        frame => vec![frame],
    };
    assert_eq!(frames, vec![ServerFrame::Pong { nonce: 99 }]);
    assert_eq!(server.server.delivery_counters().datagrams_sent, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn unreliable_frames_too_large_for_a_datagram_take_the_stream() {
    let cert = Certificate::new().await;
    let (mut server, port) = webtransport_server(&cert).await;
    let mut client = Client::connect(port, &cert.hash).await.unwrap();
    client.hello().await;
    let player = within(server.connected.recv()).await.unwrap();

    // Varied bytes: bitcode packs a run of identical ones small enough for a datagram.
    let payload: Vec<u8> = (0..8000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
    let big = ServerFrame::Event { key: EventKey::name("map:chunk"), payload };
    server.sender().send_frame(player, Outbound::unreliable(encode_server(&big))).await.unwrap();
    assert_eq!(client.next_stream_frame().await, big);
}

#[tokio::test(flavor = "multi_thread")]
async fn client_datagrams_are_routed() {
    let cert = Certificate::new().await;
    let (_server, port) = webtransport_server(&cert).await;
    let mut client = Client::connect(port, &cert.hash).await.unwrap();
    client.hello().await;
    client.connection.send_datagram(encode_client(&ClientFrame::Ping { nonce: 3 })).unwrap();
    assert_eq!(client.next_stream_frame().await, ServerFrame::Pong { nonce: 3 });
}

#[tokio::test(flavor = "multi_thread")]
async fn kicked_webtransport_players_are_told_why() {
    let cert = Certificate::new().await;
    let (mut server, port) = webtransport_server(&cert).await;
    let mut client = Client::connect(port, &cert.hash).await.unwrap();
    client.hello().await;
    let player = within(server.connected.recv()).await.unwrap();

    server.sender().kick(player, Some("bye".into())).await.unwrap();
    assert_eq!(
        client.next_stream_frame().await,
        ServerFrame::Disconnect { code: 4005, reason: "bye".into() }
    );
    let event = server.next_disconnect().await;
    assert_eq!(event.player_id, player);
    assert!(matches!(event.reason, horizon_event_system::DisconnectReason::Kicked(ref r) if r == "bye"));
    // The session itself is closed too.
    within(client.connection.closed()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn certificates_are_reloaded_without_a_restart() {
    let cert = Certificate::new().await;
    let (_server, port) = webtransport_server(&cert).await;
    let mut first = Client::connect(port, &cert.hash).await.unwrap();
    first.hello().await;

    // Files modification times have a granularity: make the change visible.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let new_hash = write_identity(cert.dir.path()).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut renewed = Client::connect(port, &new_hash).await.expect("new certificate served");
    renewed.hello().await;
    // Sessions opened before keep working.
    first.send(&ClientFrame::Ping { nonce: 1 }).await;
    assert_eq!(first.next_stream_frame().await, ServerFrame::Pong { nonce: 1 });
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_can_serve_tls() {
    let cert = Certificate::new().await;
    let (cert_path, key_path) = (cert.cert_path(), cert.key_path());
    let mut server = TestServer::start_with(move |config| {
        config.websocket.cert_path = Some(cert_path);
        config.websocket.key_path = Some(key_path);
    })
    .await;

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    let pem = std::fs::read(cert.cert_path()).unwrap();
    for der in rustls_pemfile::certs(&mut pem.as_slice()) {
        roots.add(der.unwrap()).unwrap();
    }
    let provider = std::sync::Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .unwrap();
    let (mut client, _) = tokio_tungstenite::client_async(format!("wss://localhost:{}/", server.addr.port()), tls)
        .await
        .unwrap();
    within(server.connected.recv()).await.unwrap();

    client
        .send(Message::Text(r#"{"namespace":"system","event":"ping","data":{}}"#.into()))
        .await
        .unwrap();
    let reply = loop {
        match within(client.next()).await.unwrap().unwrap() {
            Message::Text(text) => break text,
            _ => continue,
        }
    };
    assert!(reply.as_str().contains("pong"));
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_past_max_connections_are_refused() {
    let mut server = TestServer::start_with(|config| config.max_connections = 1).await;
    let (_first, _) = server.connect().await;
    let (mut second, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.addr)).await.unwrap();
    let code = loop {
        match within(second.next()).await {
            Some(Ok(Message::Close(Some(frame)))) => break u16::from(frame.code),
            Some(Ok(_)) => continue,
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    assert_eq!(code, 4007);
    let _ = next_data;
}

#[tokio::test(flavor = "multi_thread")]
async fn security_settings_load_from_partial_toml() {
    let security: crate::config::SecurityConfig = toml::from_str("max_message_size = 1234").unwrap();
    assert_eq!(security.max_message_size, 1234);
    assert_eq!(security.max_decode_errors, 5);
}
