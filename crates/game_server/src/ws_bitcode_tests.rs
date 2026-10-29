//! End-to-end tests of the listener formats: a bitcode WebSocket listener with its
//! handshake, typed handlers on both formats, and the close codes for clients that do
//! not speak the listener's protocol.

use crate::ws_tests::{next_data, within, TestServer};
use futures::{SinkExt, StreamExt};
use horizon_event_system::{
    protocol::{decode_server, encode_client, ClientFrame, EventKey, ServerFrame, PROTOCOL_VERSION},
    Format, PlayerId,
};
use std::sync::{Arc, Mutex};
use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode as WsCloseCode, Bytes, Message};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Chat {
    text: String,
    loud: bool,
}

async fn bitcode_server() -> TestServer {
    TestServer::start_with(|config| config.websocket.protocol = Format::Bitcode).await
}

/// Registers `chat:message`, which echoes the message back as `chat:echo` and records
/// who sent it in which format.
async fn echo_handler(server: &TestServer) -> Arc<Mutex<Vec<(PlayerId, Format)>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    server
        .events()
        .on_client_typed("chat", "message", move |msg: Chat, player, conn| {
            sink.lock().unwrap().push((player, conn.format()));
            tokio::spawn(async move {
                conn.respond_event("chat", "echo", &msg).await.unwrap();
            });
            Ok(())
        })
        .await
        .unwrap();
    seen
}

type Client = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn send_frame(client: &mut Client, frame: &ClientFrame) {
    client
        .send(Message::Binary(Bytes::from(encode_client(frame))))
        .await
        .unwrap();
}

async fn next_frame(client: &mut Client) -> ServerFrame {
    match next_data(client).await {
        Message::Binary(bytes) => decode_server(&bytes, 0).unwrap(),
        other => panic!("bitcode listeners send binary frames, got {other:?}"),
    }
}

async fn hello(client: &mut Client) -> Vec<(u16, String)> {
    send_frame(
        client,
        &ClientFrame::Hello { protocol_version: PROTOCOL_VERSION, client: "test".into(), dictionary_id: None },
    )
    .await;
    match next_frame(client).await {
        ServerFrame::HelloAck { protocol_version, event_table, .. } => {
            assert_eq!(protocol_version, PROTOCOL_VERSION);
            event_table
        }
        other => panic!("expected HelloAck, got {other:?}"),
    }
}

async fn close_code(client: &mut Client) -> u16 {
    loop {
        match within(client.next()).await {
            Some(Ok(Message::Close(Some(frame)))) => return u16::from(frame.code),
            Some(Ok(_)) => continue,
            other => panic!("expected a close frame, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bitcode_clients_handshake_and_reach_typed_handlers() {
    let mut server = bitcode_server().await;
    let seen = echo_handler(&server).await;
    let (mut client, player) = server.connect().await;

    let table = hello(&mut client).await;
    let id = table
        .iter()
        .find(|(_, name)| name == "chat:message")
        .map(|(id, _)| *id)
        .expect("registered handlers are in the event table");

    let chat = Chat { text: "salut".into(), loud: true };
    let payload = horizon_event_system::protocol::payload::encode_serde(Format::Bitcode, &chat).unwrap();
    send_frame(&mut client, &ClientFrame::Event { key: EventKey::Id(id), payload }).await;

    let ServerFrame::Event { key, payload } = next_frame(&mut client).await else {
        panic!("expected the echo event");
    };
    assert_eq!(key, EventKey::name("chat:echo"));
    let echoed: Chat = horizon_event_system::protocol::payload::decode_serde(Format::Bitcode, &payload).unwrap();
    assert_eq!(echoed, chat);
    // The handler got the sender's real id, not one recovered from the payload.
    assert_eq!(*seen.lock().unwrap(), vec![(player, Format::Bitcode)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bitcode_pings_are_answered() {
    let mut server = bitcode_server().await;
    let (mut client, _) = server.connect().await;
    hello(&mut client).await;
    send_frame(&mut client, &ClientFrame::Ping { nonce: 77 }).await;
    assert_eq!(next_frame(&mut client).await, ServerFrame::Pong { nonce: 77 });
}

#[tokio::test(flavor = "multi_thread")]
async fn a_json_client_on_a_bitcode_listener_is_told_why() {
    let mut server = bitcode_server().await;
    let (mut client, _) = server.connect().await;
    client
        .send(Message::Text(r#"{"namespace":"system","event":"ping","data":{}}"#.into()))
        .await
        .unwrap();
    assert_eq!(close_code(&mut client).await, 4001);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_protocol_version_is_refused() {
    let mut server = bitcode_server().await;
    let (mut client, _) = server.connect().await;
    send_frame(
        &mut client,
        &ClientFrame::Hello { protocol_version: PROTOCOL_VERSION + 1, client: "future".into(), dictionary_id: None },
    )
    .await;
    assert_eq!(close_code(&mut client).await, 4000);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_frame_must_be_hello() {
    let mut server = bitcode_server().await;
    let (mut client, _) = server.connect().await;
    send_frame(&mut client, &ClientFrame::Ping { nonce: 1 }).await;
    assert_eq!(close_code(&mut client).await, 4000);
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_garbage_closes_the_connection() {
    let mut server = bitcode_server().await;
    let (mut client, _) = server.connect().await;
    hello(&mut client).await;
    for _ in 0..5 {
        client.send(Message::Binary(Bytes::from_static(&[0xff, 0xff, 0xff]))).await.unwrap();
    }
    assert_eq!(close_code(&mut client).await, 4003);
    assert!(matches!(
        server.next_disconnect().await.reason,
        horizon_event_system::DisconnectReason::Error(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_frames_are_refused() {
    let mut server = TestServer::start_with(|config| config.security.max_message_size = 1024).await;
    let (mut client, _) = server.connect().await;
    client.send(Message::Text("x".repeat(2048).into())).await.unwrap();
    let code = close_code(&mut client).await;
    assert_eq!(code, 4004);
    let _ = WsCloseCode::from(code);
}

#[tokio::test(flavor = "multi_thread")]
async fn json_clients_reach_typed_handlers_and_may_say_hello() {
    let mut server = TestServer::start().await;
    let seen = echo_handler(&server).await;
    let (mut client, player) = server.connect().await;

    client
        .send(Message::Text(r#"{"type":"hello","protocol_version":1,"client":"web"}"#.into()))
        .await
        .unwrap();
    let Message::Text(ack) = next_data(&mut client).await else { panic!("JSON replies are text") };
    let ack: serde_json::Value = serde_json::from_str(ack.as_str()).unwrap();
    assert_eq!(ack["type"], "hello_ack");
    assert_eq!(ack["format"], "json");

    client
        .send(Message::Text(
            r#"{"namespace":"chat","event":"message","data":{"text":"hi","loud":false}}"#.into(),
        ))
        .await
        .unwrap();
    let Message::Text(echo) = next_data(&mut client).await else { panic!() };
    let echo: serde_json::Value = serde_json::from_str(echo.as_str()).unwrap();
    assert_eq!(
        echo,
        serde_json::json!({"namespace": "chat", "event": "echo", "data": {"text": "hi", "loud": false}})
    );
    assert_eq!(*seen.lock().unwrap(), vec![(player, Format::Json)]);
}
