//! GORC frames for clients on different listener formats: every subscriber gets the
//! event in its own format, each format is encoded once, JSON stays byte-for-byte what
//! it was, and bitcode frames carry handles, sequence numbers and delivery classes.

use crate::gorc::instance::GorcInstanceManager;
use crate::protocol::{decode_server, EventKey, ServerFrame};
use crate::system::{ClientResponseSender, EventSystem, ListenerProfile, Outbound, Transport};
use crate::types::{AuthenticationStatus, PlayerId, Vec3};
use crate::{impl_gorc_object, Delivery, Dest, Format, GorcZoneData};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ShipTransform {
    position: Vec3,
    speed: f32,
}

impl GorcZoneData for ShipTransform {
    fn zone_type_name() -> &'static str {
        "ShipTransform"
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ShipStatus {
    hull: u32,
}

impl GorcZoneData for ShipStatus {
    fn zone_type_name() -> &'static str {
        "ShipStatus"
    }
}

#[derive(Debug, Clone)]
struct Ship {
    transform: ShipTransform,
    status: ShipStatus,
}

impl_gorc_object! {
    Ship {
        0 => transform: ShipTransform,
        1 => status: ShipStatus,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Move {
    x: f32,
    y: f32,
}

/// Records frames and answers with each player's listener format.
#[derive(Debug, Default)]
struct RecordingSender {
    formats: Mutex<HashMap<PlayerId, Format>>,
    frames: Mutex<Vec<(PlayerId, Outbound)>>,
}

impl RecordingSender {
    fn set_format(&self, player: PlayerId, format: Format) {
        self.formats.lock().unwrap().insert(player, format);
    }

    fn frames_for(&self, player: PlayerId) -> Vec<Outbound> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| *p == player)
            .map(|(_, f)| f.clone())
            .collect()
    }

    fn clear(&self) {
        self.frames.lock().unwrap().clear();
    }
}

impl ClientResponseSender for RecordingSender {
    fn send_to_client(&self, player_id: PlayerId, data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        self.frames.lock().unwrap().push((player_id, Outbound::reliable(data)));
        Box::pin(async { Ok(()) })
    }
    fn send_frame(&self, player_id: PlayerId, frame: Outbound) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        self.frames.lock().unwrap().push((player_id, frame));
        Box::pin(async { Ok(()) })
    }
    fn connection_profile(&self, player_id: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<ListenerProfile>> + Send + '_>> {
        let format = self.formats.lock().unwrap().get(&player_id).copied();
        Box::pin(async move { format.map(|f| ListenerProfile::new(Transport::WebSocket).with_format(f)) })
    }
    fn is_connection_active(&self, _: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { true })
    }
    fn get_auth_status(&self, _: PlayerId) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<AuthenticationStatus>> + Send + '_>> {
        Box::pin(async { None })
    }
    fn kick(&self, _: PlayerId, _: Option<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct Setup {
    events: EventSystem,
    gorc: Arc<GorcInstanceManager>,
    sender: Arc<RecordingSender>,
    ship: crate::GorcObjectId,
    json_player: PlayerId,
    bitcode_player: PlayerId,
}

async fn setup() -> Setup {
    let mut events = EventSystem::new();
    let gorc = Arc::new(GorcInstanceManager::new());
    let sender = Arc::new(RecordingSender::default());
    events.set_gorc_instances(gorc.clone());
    events.set_client_response_sender(sender.clone());
    events.declare_client_event("move");
    // The table is frozen by the first handshake.
    events.event_table().snapshot();

    let ship = Ship {
        transform: ShipTransform { position: Vec3::new(0.0, 0.0, 0.0), speed: 3.5 },
        status: ShipStatus { hull: 90 },
    };
    let ship = gorc.register_object(ship, Vec3::new(0.0, 0.0, 0.0)).await;

    let json_player = PlayerId::new();
    let bitcode_player = PlayerId::new();
    sender.set_format(json_player, Format::Json);
    sender.set_format(bitcode_player, Format::Bitcode);
    for player in [json_player, bitcode_player] {
        gorc.add_player(player, Vec3::new(1000.0, 1000.0, 0.0)).await;
        events.update_player_position(player, Vec3::new(1.0, 1.0, 0.0)).await.unwrap();
    }
    Setup { events, gorc, sender, ship, json_player, bitcode_player }
}

#[tokio::test]
async fn zone_entries_come_in_each_subscribers_format() {
    let s = setup().await;
    let handle = s.gorc.object_handle(s.ship).unwrap();

    let json = s.sender.frames_for(s.json_player);
    let enter: serde_json::Value = serde_json::from_slice(&json[0].bytes).unwrap();
    assert_eq!(enter["type"], "gorc_zone_enter");
    assert_eq!(enter["object_type"], "Ship");
    assert_eq!(enter["zone_data"]["speed"], 3.5);

    let frames: Vec<ServerFrame> = s
        .sender
        .frames_for(s.bitcode_player)
        .iter()
        .map(|f| decode_server(&f.bytes, 0).unwrap())
        .collect();
    let transform = frames
        .iter()
        .find_map(|f| match f {
            ServerFrame::ZoneEnter { handle: h, object_id, object_type, channel: 0, zone_data } => {
                assert_eq!(*h, handle);
                assert_eq!(object_id, s.ship.0.as_bytes());
                assert_eq!(object_type, "Ship");
                Some(crate::protocol::payload::decode_serde::<ShipTransform>(Format::Bitcode, zone_data).unwrap())
            }
            _ => None,
        })
        .expect("a zone entry for channel 0");
    // zone_data is bitcode, not JSON inside a bitcode frame.
    assert_eq!(transform.speed, 3.5);
}

#[tokio::test]
async fn events_are_encoded_once_per_format() {
    let s = setup().await;
    let second_json = PlayerId::new();
    s.sender.set_format(second_json, Format::Json);
    s.gorc.add_player(second_json, Vec3::new(1000.0, 0.0, 0.0)).await;
    s.events.update_player_position(second_json, Vec3::new(2.0, 2.0, 0.0)).await.unwrap();
    s.sender.clear();

    s.events
        .emit_gorc_instance(s.ship, 0, "move", &Move { x: 1.0, y: 2.0 }, Dest::Client)
        .await
        .unwrap();

    let a = &s.sender.frames_for(s.json_player)[0];
    let b = &s.sender.frames_for(second_json)[0];
    assert_eq!(a.bytes.as_ptr(), b.bytes.as_ptr(), "JSON subscribers share one encoding");
    let event: serde_json::Value = serde_json::from_slice(&a.bytes).unwrap();
    assert_eq!(event["event_type"], "move");
    assert_eq!(event["data"], serde_json::json!({"x": 1.0, "y": 2.0}));
    assert!(event.get("seq").is_none(), "reliable JSON events keep the historical envelope");

    let frame = decode_server(&s.sender.frames_for(s.bitcode_player)[0].bytes, 0).unwrap();
    let ServerFrame::GorcEvent { handle, channel, event, payload, .. } = frame else {
        panic!("expected a GORC event, got {frame:?}");
    };
    assert_eq!(handle, s.gorc.object_handle(s.ship).unwrap());
    assert_eq!(channel, 0);
    assert_eq!(event, EventKey::Id(0), "declared events travel as ids");
    let decoded: Move = crate::protocol::payload::decode_serde(Format::Bitcode, &payload).unwrap();
    assert_eq!(decoded, Move { x: 1.0, y: 2.0 });
}

#[tokio::test]
async fn unreliable_events_carry_a_growing_sequence() {
    let s = setup().await;
    s.sender.clear();
    for i in 0..3 {
        s.events
            .emit_gorc_instance_typed(s.ship, 0, "move", &Move { x: i as f32, y: 0.0 }, Dest::Client, Some(Delivery::Unreliable))
            .await
            .unwrap();
    }
    let json = s.sender.frames_for(s.json_player);
    assert!(json.iter().all(|f| f.delivery == Delivery::Unreliable));
    let seqs: Vec<u64> = json
        .iter()
        .map(|f| serde_json::from_slice::<serde_json::Value>(&f.bytes).unwrap()["seq"].as_u64().unwrap())
        .collect();
    assert!(seqs.windows(2).all(|w| w[1] == w[0] + 1), "seq grows: {seqs:?}");

    let bitcode_seqs: Vec<u16> = s
        .sender
        .frames_for(s.bitcode_player)
        .iter()
        .map(|f| match decode_server(&f.bytes, 0).unwrap() {
            ServerFrame::GorcEvent { seq, .. } => seq,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(bitcode_seqs.len(), 3);
    // Both formats share the object's counter.
    assert_eq!(bitcode_seqs.iter().map(|s| *s as u64).collect::<Vec<_>>(), seqs);
}

#[tokio::test]
async fn bytes_go_only_to_their_format_and_skips_are_counted() {
    let s = setup().await;
    s.sender.clear();
    let payload = bytes::Bytes::from(crate::protocol::payload::encode_serde(Format::Bitcode, &Move { x: 0.0, y: 0.0 }).unwrap());
    let sent = s
        .events
        .emit_gorc_instance_bytes(s.ship, 0, "move", payload, Format::Bitcode, None)
        .await
        .unwrap();
    assert_eq!(sent, 1);
    assert!(s.sender.frames_for(s.json_player).is_empty());
    assert_eq!(s.sender.frames_for(s.bitcode_player).len(), 1);
    assert_eq!(s.events.skipped_format_mismatches(), 1);
}

#[tokio::test]
async fn zone_exits_name_the_handle() {
    let s = setup().await;
    s.sender.clear();
    s.events
        .update_player_position(s.bitcode_player, Vec3::new(5000.0, 5000.0, 0.0))
        .await
        .unwrap();
    let handle = s.gorc.object_handle(s.ship).unwrap();
    let exits: Vec<ServerFrame> = s
        .sender
        .frames_for(s.bitcode_player)
        .iter()
        .map(|f| decode_server(&f.bytes, 0).unwrap())
        .collect();
    assert!(!exits.is_empty());
    assert!(exits
        .iter()
        .all(|f| matches!(f, ServerFrame::ZoneExit { handle: h, .. } if *h == handle)));
}

#[tokio::test]
async fn handles_are_unique_and_released_with_the_object() {
    let gorc = GorcInstanceManager::new();
    let make = || Ship {
        transform: ShipTransform { position: Vec3::new(0.0, 0.0, 0.0), speed: 0.0 },
        status: ShipStatus { hull: 1 },
    };
    let a = gorc.register_object(make(), Vec3::new(0.0, 0.0, 0.0)).await;
    let b = gorc.register_object(make(), Vec3::new(0.0, 0.0, 0.0)).await;
    let (ha, hb) = (gorc.object_handle(a).unwrap(), gorc.object_handle(b).unwrap());
    assert_ne!(ha, hb);
    assert_eq!(gorc.object_by_handle(ha), Some(a));
    gorc.unregister_object(a).await;
    assert_eq!(gorc.object_handle(a), None);
    assert_eq!(gorc.object_by_handle(ha), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn bitcode_client_events_reach_gorc_client_handlers_with_their_format() {
    let s = setup().await;
    let runtime = luminal::Runtime::new().expect("luminal runtime");
    let (tx, rx) = std::sync::mpsc::channel();
    s.events
        .on_gorc_client(runtime.handle().clone(), "Ship", 0, "move", move |event, player, conn, _instance| {
            let decoded: Move = event.decode_data()?;
            tx.send((decoded, event.format, player, conn.format())).unwrap();
            Ok(())
        })
        .await
        .unwrap();

    let payload = crate::protocol::payload::encode_serde(Format::Bitcode, &Move { x: 4.0, y: 5.0 }).unwrap();
    s.events
        .emit_gorc_client_payload(s.bitcode_player, s.ship, 0, "move", &payload, Format::Bitcode)
        .await
        .unwrap();

    let (decoded, format, player, conn_format) =
        rx.recv_timeout(std::time::Duration::from_secs(5)).expect("handler called");
    assert_eq!(decoded, Move { x: 4.0, y: 5.0 });
    assert_eq!(format, Format::Bitcode);
    assert_eq!(conn_format, Format::Bitcode);
    assert_eq!(player, s.bitcode_player);
}
