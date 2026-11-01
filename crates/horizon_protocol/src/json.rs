//! Today's JSON envelopes, as serde structs.
//!
//! The field names and values are exactly those the server has always sent, so JSON
//! clients see no change. Only two things are new, and both are absent unless used:
//! a `seq` field on unreliable GORC events, and the `batch` envelope (used only on
//! listeners with compression).

use serde::{Deserialize, Serialize};

/// A GORC event sent to the subscribers of an object.
///
/// Fields are in alphabetical order, the order `serde_json::json!` has always written
/// them in, so the bytes on the wire are unchanged.
#[derive(Debug, Serialize)]
pub struct GorcEvent<'a, T: Serialize + ?Sized> {
    pub channel: u8,
    pub data: &'a T,
    pub event_type: &'a str,
    pub object_id: String,
    pub object_type: &'a str,
    /// Historically the object id again (not a player id).
    pub player_id: String,
    /// Present only on unreliable events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u16>,
    pub timestamp: u64,
}

/// The recipient entered a replication zone of an object (fields in alphabetical
/// order, as historically written).
#[derive(Debug, Serialize)]
pub struct ZoneEnter<'a> {
    pub channel: u8,
    pub object_id: String,
    pub object_type: &'a str,
    pub player_id: String,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub zone_data: serde_json::Value,
}

impl<'a> ZoneEnter<'a> {
    /// The `type` value of a zone entry.
    pub const TYPE: &'static str = "gorc_zone_enter";
}

/// The recipient left a replication zone of an object (fields in alphabetical order,
/// as historically written).
#[derive(Debug, Serialize)]
pub struct ZoneExit<'a> {
    pub channel: u8,
    pub object_id: String,
    pub object_type: &'a str,
    pub player_id: String,
    pub timestamp: u64,
    #[serde(rename = "type")]
    pub kind: &'static str,
}

impl<'a> ZoneExit<'a> {
    /// The `type` value of a zone exit.
    pub const TYPE: &'static str = "gorc_zone_exit";
}

/// A client event sent by a plugin, in the shape clients send them.
#[derive(Debug, Serialize)]
pub struct Event<'a, T: Serialize + ?Sized> {
    pub namespace: &'a str,
    pub event: &'a str,
    pub data: &'a T,
}

/// Several frames to one client in one message (listeners with compression only).
#[derive(Debug, Serialize, Deserialize)]
pub struct Batch {
    #[serde(rename = "type")]
    pub kind: String,
    pub frames: Vec<Box<serde_json::value::RawValue>>,
}

impl Batch {
    /// The `type` value of a batch.
    pub const TYPE: &'static str = "batch";

    /// A batch of already encoded JSON frames.
    pub fn encode(frames: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(frames.iter().map(|f| f.len() + 1).sum::<usize>() + 32);
        out.extend_from_slice(br#"{"type":"batch","frames":["#);
        for (i, frame) in frames.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(frame);
        }
        out.extend_from_slice(b"]}");
        out
    }
}

/// The server is closing the connection (WebTransport, where the transport close
/// carries no reason a JSON client can read): `{"type":"disconnect","code":4005,...}`.
#[derive(Debug, Serialize)]
pub struct Disconnect<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub code: u16,
    pub reason: &'a str,
}

impl<'a> Disconnect<'a> {
    /// The `type` value of a disconnect notice.
    pub const TYPE: &'static str = "disconnect";
}

/// Optional handshake of a JSON client: `{"type":"hello","protocol_version":1,...}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u16,
    #[serde(default)]
    pub client: String,
    #[serde(default)]
    pub dictionary_id: Option<u32>,
}

/// Answer to a JSON [`Hello`].
#[derive(Debug, Serialize)]
pub struct HelloAck<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub protocol_version: u16,
    pub format: &'static str,
    pub compression: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary_id: Option<u32>,
}

impl<'a> HelloAck<'a> {
    /// The `type` value of a handshake answer.
    pub const TYPE: &'static str = "hello_ack";
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gorc_event_matches_the_historical_envelope() {
        let data = json!({"x": 1});
        let event = GorcEvent {
            event_type: "move",
            object_id: "GorcObjectId(1)".into(),
            object_type: "Ship",
            channel: 0,
            player_id: "GorcObjectId(1)".into(),
            data: &data,
            timestamp: 5,
            seq: None,
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({
                "event_type": "move",
                "object_id": "GorcObjectId(1)",
                "object_type": "Ship",
                "channel": 0,
                "player_id": "GorcObjectId(1)",
                "data": {"x": 1},
                "timestamp": 5
            })
        );
        let unreliable = GorcEvent { seq: Some(9), ..event };
        assert_eq!(serde_json::to_value(&unreliable).unwrap()["seq"], 9);
    }

    #[test]
    fn bytes_match_what_json_macro_wrote() {
        let data = json!({"x": 1});
        let event = GorcEvent {
            event_type: "move",
            object_id: "o".into(),
            object_type: "Ship",
            channel: 0,
            player_id: "o".into(),
            data: &data,
            timestamp: 5,
            seq: None,
        };
        let historical = json!({
            "event_type": "move", "object_id": "o", "object_type": "Ship", "channel": 0,
            "player_id": "o", "data": {"x": 1}, "timestamp": 5
        });
        assert_eq!(serde_json::to_vec(&event).unwrap(), serde_json::to_vec(&historical).unwrap());

        let enter = ZoneEnter {
            kind: ZoneEnter::TYPE,
            object_id: "o".into(),
            object_type: "Ship",
            channel: 1,
            player_id: "p".into(),
            zone_data: json!({"hp": 3}),
            timestamp: 2,
        };
        let historical = json!({"type": "gorc_zone_enter", "object_id": "o", "object_type": "Ship",
            "channel": 1, "player_id": "p", "zone_data": {"hp": 3}, "timestamp": 2});
        assert_eq!(serde_json::to_vec(&enter).unwrap(), serde_json::to_vec(&historical).unwrap());
    }

    #[test]
    fn zone_messages_match_the_historical_envelopes() {
        let enter = ZoneEnter {
            kind: ZoneEnter::TYPE,
            object_id: "o".into(),
            object_type: "Ship",
            channel: 1,
            player_id: "p".into(),
            zone_data: json!({"hp": 3}),
            timestamp: 2,
        };
        assert_eq!(
            serde_json::to_value(&enter).unwrap(),
            json!({"type": "gorc_zone_enter", "object_id": "o", "object_type": "Ship", "channel": 1,
                   "player_id": "p", "zone_data": {"hp": 3}, "timestamp": 2})
        );
        let exit = ZoneExit {
            kind: ZoneExit::TYPE,
            object_id: "o".into(),
            object_type: "Ship",
            channel: 1,
            player_id: "p".into(),
            timestamp: 2,
        };
        assert_eq!(
            serde_json::to_value(&exit).unwrap(),
            json!({"type": "gorc_zone_exit", "object_id": "o", "object_type": "Ship", "channel": 1,
                   "player_id": "p", "timestamp": 2})
        );
    }

    #[test]
    fn batches_embed_frames_without_reencoding() {
        let bytes = Batch::encode(&[br#"{"a":1}"#, br#"{"b":2}"#]);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value, json!({"type": "batch", "frames": [{"a": 1}, {"b": 2}]}));
        let batch: Batch = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(batch.frames.len(), 2);
    }

    #[test]
    fn hello_accepts_missing_optional_fields() {
        let hello: Hello = serde_json::from_str(r#"{"type":"hello","protocol_version":1}"#).unwrap();
        assert_eq!(hello, Hello { protocol_version: 1, client: String::new(), dictionary_id: None });
    }
}
