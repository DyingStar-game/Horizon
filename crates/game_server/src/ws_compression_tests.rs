//! End-to-end tests of listeners with compression: header bytes, batches of small
//! frames, large frames compressed once, the decompression cap and dictionaries.

use crate::ws_tests::{next_data, within, TestServer};
use futures::{SinkExt, StreamExt};
use horizon_event_system::{
    protocol::{
        decode_server, encode_client, json, ClientFrame, Codec, Compression, Dictionary, ServerFrame,
        PROTOCOL_VERSION,
    },
    Format,
};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Bytes, Message};

type Client = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const MAX: usize = 1 << 20;

async fn server(format: Format, codec: Codec) -> TestServer {
    TestServer::start_with(move |config| {
        config.websocket.protocol = format;
        config.websocket.compression = codec;
    })
    .await
}

/// The bytes of the next server message (always binary on these listeners), unwrapped.
async fn next_body(client: &mut Client, compression: &Compression) -> Vec<u8> {
    match next_data(client).await {
        Message::Binary(bytes) => compression.unwrap(&bytes, MAX).unwrap().into_owned(),
        other => panic!("listeners with compression send binary frames, got {other:?}"),
    }
}

async fn send(client: &mut Client, compression: &Compression, frame: &[u8]) {
    client.send(Message::Binary(Bytes::from(compression.wrap(frame)))).await.unwrap();
}

async fn bitcode_hello(client: &mut Client, compression: &Compression) -> ServerFrame {
    let hello = ClientFrame::Hello {
        protocol_version: PROTOCOL_VERSION,
        client: "test".into(),
        dictionary_id: compression.dictionary_id(),
    };
    send(client, compression, &encode_client(&hello)).await;
    decode_server(&next_body(client, compression).await, 0).unwrap()
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
async fn bitcode_over_zstd_handshakes_and_pings() {
    let mut server = server(Format::Bitcode, Codec::Zstd).await;
    let (mut client, _) = server.connect().await;
    let compression = Compression::with_codec(Codec::Zstd);
    let ServerFrame::HelloAck { codec, .. } = bitcode_hello(&mut client, &compression).await else {
        panic!("expected HelloAck");
    };
    assert_eq!(Codec::from_bits(codec), Some(Codec::Zstd));
    send(&mut client, &compression, &encode_client(&ClientFrame::Ping { nonce: 5 })).await;
    assert_eq!(decode_server(&next_body(&mut client, &compression).await, 0).unwrap(), ServerFrame::Pong { nonce: 5 });
}

#[tokio::test(flavor = "multi_thread")]
async fn json_over_lz4_round_trips() {
    let mut server = server(Format::Json, Codec::Lz4).await;
    let (mut client, _) = server.connect().await;
    let compression = Compression::with_codec(Codec::Lz4);
    send(&mut client, &compression, br#"{"namespace":"system","event":"ping","data":{}}"#).await;
    let reply: serde_json::Value = serde_json::from_slice(&next_body(&mut client, &compression).await).unwrap();
    assert_eq!(reply["message"], "pong");
}

#[tokio::test(flavor = "multi_thread")]
async fn large_frames_arrive_compressed() {
    let mut server = server(Format::Json, Codec::Zstd).await;
    let (mut client, player) = server.connect().await;
    let big = serde_json::to_vec(&serde_json::json!({"cargo": vec!["crate of ore"; 400]})).unwrap();
    server.sender().send_to_client(player, big.clone()).await.unwrap();

    let Message::Binary(wire) = next_data(&mut client).await else { panic!() };
    assert_eq!(wire[0] & 0b11, Codec::Zstd.bits(), "compressed with zstd");
    assert!(wire.len() * 10 < big.len(), "{} bytes on the wire for {}", wire.len(), big.len());
    let compression = Compression::with_codec(Codec::Zstd);
    assert_eq!(compression.unwrap(&wire, MAX).unwrap().as_ref(), &big[..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn small_frames_to_a_busy_client_are_batched() {
    let mut server = server(Format::Json, Codec::Zstd).await;
    let (mut client, player) = server.connect().await;
    let sender = server.sender();
    const COUNT: usize = 300;
    for i in 0..COUNT {
        sender.send_to_client(player, format!(r#"{{"seq":{i}}}"#).into_bytes()).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let compression = Compression::with_codec(Codec::Zstd);
    let mut seqs = Vec::new();
    let mut messages = 0;
    while seqs.len() < COUNT {
        let body = next_body(&mut client, &compression).await;
        messages += 1;
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if value["type"] == json::Batch::TYPE {
            for frame in value["frames"].as_array().unwrap() {
                seqs.push(frame["seq"].as_u64().unwrap());
            }
        } else {
            seqs.push(value["seq"].as_u64().unwrap());
        }
    }
    assert_eq!(seqs, (0..COUNT as u64).collect::<Vec<_>>(), "every frame, in order");
    assert!(messages < COUNT / 10, "{messages} messages for {COUNT} frames");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_decompression_bomb_is_refused() {
    let mut server = TestServer::start_with(|config| {
        config.websocket.compression = Codec::Zstd;
        config.security.max_message_size = 64 * 1024;
    })
    .await;
    let (mut client, _) = server.connect().await;
    let bomb = Compression::with_codec(Codec::Zstd).wrap(&vec![b' '; 4 << 20]);
    assert!(bomb.len() < 64 * 1024, "small on the wire: {} bytes", bomb.len());
    client.send(Message::Binary(bomb)).await.unwrap();
    assert_eq!(close_code(&mut client).await, 4004);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_without_header_is_a_settings_mismatch() {
    let mut server = server(Format::Json, Codec::Zstd).await;
    let (mut client, _) = server.connect().await;
    // A JSON client unaware of compression.
    client.send(Message::Text(r#"{"namespace":"system","event":"ping","data":{}}"#.into())).await.unwrap();
    assert_eq!(close_code(&mut client).await, 4001);
}

#[tokio::test(flavor = "multi_thread")]
async fn dictionaries_must_match() {
    let dict_bytes: Vec<u8> = (0..4096).map(|i| b"ShipTransform position velocity"[i % 31]).collect();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("game.dict");
    std::fs::write(&path, &dict_bytes).unwrap();
    let mut server = TestServer::start_with(move |config| {
        config.websocket.protocol = Format::Bitcode;
        config.websocket.compression = Codec::Zstd;
        config.compression.dictionary_path = Some(path);
    })
    .await;

    // Without the dictionary: told why.
    let (mut stranger, _) = server.connect().await;
    let plain = Compression::with_codec(Codec::Zstd);
    send(
        &mut stranger,
        &plain,
        &encode_client(&ClientFrame::Hello { protocol_version: PROTOCOL_VERSION, client: "old".into(), dictionary_id: None }),
    )
    .await;
    assert_eq!(close_code(&mut stranger).await, 4002);

    // With it: accepted, and told the id.
    let (mut client, _) = server.connect().await;
    let dictionary = Dictionary::new(dict_bytes);
    let with_dict = Compression { dictionary: Some(dictionary.clone()), ..plain };
    let ServerFrame::HelloAck { dictionary_id, .. } = bitcode_hello(&mut client, &with_dict).await else {
        panic!("expected HelloAck");
    };
    assert_eq!(dictionary_id, Some(dictionary.id()));
}
