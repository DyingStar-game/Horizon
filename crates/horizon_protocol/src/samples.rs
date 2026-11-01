//! Synthetic frames for benchmarks and size reports: position updates of ships, the
//! most frequent frame of a space game, in both formats.

use crate::{encode_server, json, EventKey, ServerFrame};
use serde::{Deserialize, Serialize};

/// A transform update, the hot payload of a game.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct Transform {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub vx: f32,
    pub vy: f32,
    pub vz: f32,
    pub heading: f32,
}

/// The `i`-th transform of a fleet moving together, with close, varying values.
pub fn transform(i: usize) -> Transform {
    let t = i as f32;
    Transform {
        x: 1200.0 + t * 0.37,
        y: -350.0 + (t * 0.11).sin() * 4.0,
        z: 88.0 + t * 0.05,
        vx: 3.2,
        vy: 0.0,
        vz: -1.1 + (t * 0.01),
        heading: (t * 0.02) % 6.28,
    }
}

/// A bitcode GORC event frame carrying `transform(i)` (derive payload, event id).
pub fn bitcode_frame(i: usize) -> Vec<u8> {
    encode_server(&ServerFrame::GorcEvent {
        handle: 1000 + (i % 64) as u32,
        channel: 0,
        seq: i as u16,
        event: EventKey::Id(3),
        payload: bitcode::encode(&transform(i)),
    })
    .to_vec()
}

/// The JSON GORC event envelope carrying `transform(i)`, as the server sends it.
pub fn json_frame(i: usize) -> Vec<u8> {
    let object_id = format!("GorcObjectId({:08x}-1c2d-4e5f-8a9b-0c1d2e3f4a5b)", 1000 + i % 64);
    let data = transform(i);
    serde_json::to_vec(&json::GorcEvent {
        event_type: "move",
        object_id: object_id.clone(),
        object_type: "Ship",
        channel: 0,
        player_id: object_id,
        data: &data,
        timestamp: 1_790_000_000 + i as u64,
        seq: None,
    })
    .unwrap_or_default()
}

/// `count` frames of one format as one batch frame.
pub fn batch(bitcode_format: bool, count: usize) -> Vec<u8> {
    if bitcode_format {
        encode_server(&ServerFrame::Batch { frames: (0..count).map(bitcode_frame).collect() }).to_vec()
    } else {
        let frames: Vec<Vec<u8>> = (0..count).map(json_frame).collect();
        let slices: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
        json::Batch::encode(&slices)
    }
}
