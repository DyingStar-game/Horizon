//! End-to-end tests of the WebSocket listener: a real server on a free port and a real
//! `tokio-tungstenite` client.

use crate::{GameServer, ServerConfig};
use futures::{SinkExt, StreamExt};
use horizon_event_system::{
    ClientResponseSender, DisconnectReason, EventSystem, LogLevel, PlayerConnectedEvent,
    PlayerDisconnectedEvent, PlayerId,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{protocol::frame::coding::CloseCode as WsCloseCode, Bytes, Message},
    MaybeTlsStream, WebSocketStream,
};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const TIMEOUT: Duration = Duration::from_secs(10);

/// A running server, its address, and the players it sees connect and disconnect.
pub(crate) struct TestServer {
    pub server: Arc<GameServer>,
    pub addr: SocketAddr,
    pub connected: mpsc::UnboundedReceiver<PlayerId>,
    pub disconnected: mpsc::UnboundedReceiver<PlayerDisconnectedEvent>,
    _plugins: tempfile::TempDir,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let plugins = tempfile::tempdir().unwrap();
        let port = free_port();
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let mut config = ServerConfig {
            bind_address: ([0, 0, 0, 0], port).into(),
            plugin_directory: plugins.path().to_path_buf(),
            tick_interval_ms: 0,
            use_reuse_port: false,
            ..ServerConfig::default()
        };
        configure(&mut config);
        let server = Arc::new(GameServer::new(config, LogLevel::Warn));

        let events = server.get_horizon_event_system();
        let (connected_tx, connected) = mpsc::unbounded_channel();
        events
            .on_core("player_connected", move |event: PlayerConnectedEvent| {
                let _ = connected_tx.send(event.player_id);
                Ok(())
            })
            .await
            .unwrap();
        let (disconnected_tx, disconnected) = mpsc::unbounded_channel();
        events
            .on_core("player_disconnected", move |event: PlayerDisconnectedEvent| {
                let _ = disconnected_tx.send(event);
                Ok(())
            })
            .await
            .unwrap();

        let running = server.clone();
        tokio::spawn(async move {
            if let Err(e) = running.start().await {
                eprintln!("test server stopped: {e}");
            }
        });
        wait_until_listening(addr).await;
        Self { server, addr, connected, disconnected, _plugins: plugins }
    }

    pub fn events(&self) -> Arc<EventSystem> {
        self.server.get_horizon_event_system()
    }

    pub fn sender(&self) -> Arc<dyn ClientResponseSender + Send + Sync> {
        self.events().get_client_response_sender().unwrap()
    }

    /// Connects a client and returns it with the player ID the server gave it.
    pub async fn connect(&mut self) -> (Client, PlayerId) {
        let (client, _) = connect_async(format!("ws://{}", self.addr)).await.unwrap();
        let player = within(self.connected.recv()).await.unwrap();
        (client, player)
    }

    pub async fn next_disconnect(&mut self) -> PlayerDisconnectedEvent {
        within(self.disconnected.recv()).await.unwrap()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let server = self.server.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = server.shutdown().await;
            });
        }
    }
}

/// A free port. The server binds every interface (`horizon_sockets` cannot create a
/// listener on 127.0.0.1); clients reach it on 127.0.0.1.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_until_listening(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while TcpStream::connect(addr).await.is_err() {
        assert!(tokio::time::Instant::now() < deadline, "server never listened on {addr}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub(crate) async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(TIMEOUT, future).await.expect("timed out")
}

/// The next data message (text or binary), skipping pings and pongs.
pub(crate) async fn next_data(client: &mut Client) -> Message {
    loop {
        match within(client.next()).await.expect("connection ended").unwrap() {
            Message::Ping(_) | Message::Pong(_) => continue,
            other => return other,
        }
    }
}

fn ping_request() -> String {
    r#"{"namespace":"system","event":"ping","data":{}}"#.to_string()
}

fn is_pong(message: &Message) -> bool {
    let text = match message {
        Message::Text(text) => text.as_str().to_string(),
        Message::Binary(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        _ => return false,
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["message"] == "pong"
}

#[tokio::test(flavor = "multi_thread")]
async fn json_requests_are_answered_in_text_and_binary_frames() {
    let mut server = TestServer::start().await;
    let (mut client, _) = server.connect().await;

    client.send(Message::Text(ping_request().into())).await.unwrap();
    let reply = next_data(&mut client).await;
    assert!(matches!(reply, Message::Text(_)), "JSON replies stay text frames");
    assert!(is_pong(&reply));

    // Binary frames used to be dropped without a word.
    client
        .send(Message::Binary(Bytes::from(ping_request().into_bytes())))
        .await
        .unwrap();
    assert!(is_pong(&next_data(&mut client).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn transport_pings_are_answered() {
    let mut server = TestServer::start().await;
    let (mut client, _) = server.connect().await;
    client.send(Message::Ping(Bytes::from_static(b"hi"))).await.unwrap();
    loop {
        match within(client.next()).await.unwrap().unwrap() {
            Message::Pong(data) => {
                assert_eq!(&data[..], b"hi");
                break;
            }
            other => panic!("expected a pong, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_frames_do_not_end_the_connection() {
    let mut server = TestServer::start().await;
    let (mut client, _) = server.connect().await;
    client.send(Message::Binary(Bytes::from_static(&[0xff, 0xfe]))).await.unwrap();
    client.send(Message::Text("not json".into())).await.unwrap();
    client.send(Message::Text(ping_request().into())).await.unwrap();
    assert!(is_pong(&next_data(&mut client).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_client_gets_every_message_in_order() {
    let mut server = TestServer::start().await;
    let (mut client, player) = server.connect().await;
    let sender = server.sender();

    // Far more than the old shared channel held (1000): that client used to lag behind
    // it and be disconnected.
    const COUNT: usize = 5000;
    for i in 0..COUNT {
        let message = format!(r#"{{"seq":{i}}}"#).into_bytes();
        sender.send_to_client(player, message).await.unwrap();
    }
    // The client is not reading yet.
    tokio::time::sleep(Duration::from_millis(200)).await;

    for i in 0..COUNT {
        let Message::Text(text) = next_data(&mut client).await else {
            panic!("expected a text frame");
        };
        let value: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(value["seq"], i, "messages arrive in the order they were sent");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_reach_only_their_player() {
    let mut server = TestServer::start().await;
    let (mut alice, alice_id) = server.connect().await;
    let (mut bob, _) = server.connect().await;
    server.sender().send_to_client(alice_id, b"{\"to\":\"alice\"}".to_vec()).await.unwrap();
    client_send_ping(&mut bob).await;

    let Message::Text(text) = next_data(&mut alice).await else { panic!() };
    assert_eq!(text.as_str(), r#"{"to":"alice"}"#);
    // Bob's first message is his own pong, not Alice's message.
    assert!(is_pong(&next_data(&mut bob).await));
}

async fn client_send_ping(client: &mut Client) {
    client.send(Message::Text(ping_request().into())).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn kicked_players_get_a_close_code_and_a_disconnect_event() {
    let mut server = TestServer::start().await;
    let (mut client, player) = server.connect().await;

    server.sender().kick(player, Some("bye".into())).await.unwrap();

    let close = loop {
        match within(client.next()).await {
            Some(Ok(Message::Close(frame))) => break frame.expect("a close frame with a code"),
            Some(Ok(_)) => continue,
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    assert_eq!(close.code, WsCloseCode::from(4005));
    assert_eq!(close.reason.as_str(), "bye");

    // Before, kicked players were removed before their handler ended, so no
    // player_disconnected was ever emitted for them.
    let event = server.next_disconnect().await;
    assert_eq!(event.player_id, player);
    assert!(matches!(event.reason, DisconnectReason::Kicked(ref r) if r == "bye"));
    // The connection is removed right after the event.
    within(async {
        while server.sender().is_connection_active(player).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_closing_emits_player_disconnected() {
    let mut server = TestServer::start().await;
    let (mut client, player) = server.connect().await;
    client.close(None).await.unwrap();
    let event = server.next_disconnect().await;
    assert_eq!(event.player_id, player);
    assert!(matches!(event.reason, DisconnectReason::ClientDisconnect));
}

#[tokio::test(flavor = "multi_thread")]
async fn broadcast_reaches_every_client() {
    let mut server = TestServer::start().await;
    let (mut a, _) = server.connect().await;
    let (mut b, _) = server.connect().await;
    let reached = server.sender().broadcast_to_all(b"{\"all\":true}".to_vec()).await.unwrap();
    assert_eq!(reached, 2);
    for client in [&mut a, &mut b] {
        let Message::Text(text) = next_data(client).await else { panic!() };
        assert_eq!(text.as_str(), r#"{"all":true}"#);
    }
}
