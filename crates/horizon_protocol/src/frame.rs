//! The bitcode frames.
//!
//! Every frame names its object by a [`handle`](ServerFrame::ZoneEnter): a `u32` the
//! server gives each object when it registers it, the same for every client. A client
//! learns the handle of an object in its `ZoneEnter`, then later frames carry only the
//! handle, not the 16-byte id and the type name. Because the handle is per server and not
//! per client, the bytes of a frame are the same for every recipient and are encoded once.

use crate::{ProtocolError, DEFAULT_MAX_FRAME_SIZE};
use bitcode::{Decode, Encode};

/// How a frame names an event: a number from the table sent in
/// [`ServerFrame::HelloAck`], or its full name (`"namespace:event"` for client events,
/// the event name for GORC events) when it is not in the table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
#[cfg_attr(feature = "debug-json", derive(serde::Serialize))]
pub enum EventKey {
    /// Index in the event table.
    Id(u16),
    /// Full event name.
    Name(String),
}

impl EventKey {
    /// A key by name.
    pub fn name(name: impl Into<String>) -> Self {
        EventKey::Name(name.into())
    }
}

/// A frame from a client.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "debug-json", derive(serde::Serialize))]
pub enum ClientFrame {
    /// First frame of a session: the protocol version the client speaks.
    Hello {
        protocol_version: u16,
        /// Free-form client name and version, for logs.
        client: String,
        /// The compression dictionary the client holds, if any.
        dictionary_id: Option<u32>,
    },
    /// A client event (`namespace:event`), with its payload in bitcode.
    Event { key: EventKey, payload: Vec<u8> },
    /// An event targeting a GORC object the client knows by its handle.
    GorcEvent {
        handle: u32,
        channel: u8,
        event: EventKey,
        payload: Vec<u8>,
    },
    /// Application-level ping; answered with [`ServerFrame::Pong`].
    Ping { nonce: u32 },
}

/// A frame from the server.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "debug-json", derive(serde::Serialize))]
pub enum ServerFrame {
    /// Answer to [`ClientFrame::Hello`].
    HelloAck {
        protocol_version: u16,
        /// Compression codec of the listener (0: none).
        codec: u8,
        /// The compression dictionary the server uses, if any.
        dictionary_id: Option<u32>,
        /// Event ids: `(id, name)`.
        event_table: Vec<(u16, String)>,
    },
    /// A client event (`namespace:event`) sent by a plugin.
    Event { key: EventKey, payload: Vec<u8> },
    /// The client entered a replication zone of an object: its handle, identity and
    /// current state for that channel.
    ZoneEnter {
        handle: u32,
        object_id: [u8; 16],
        object_type: String,
        channel: u8,
        zone_data: Vec<u8>,
    },
    /// An event of an object the client is subscribed to.
    GorcEvent {
        handle: u32,
        channel: u8,
        /// Wrapping counter per object and channel, for the client to drop unreliable
        /// frames that arrive late or out of order.
        seq: u16,
        event: EventKey,
        payload: Vec<u8>,
    },
    /// The client left a replication zone of an object.
    ZoneExit { handle: u32, channel: u8 },
    /// Several frames to one client in one write or one datagram, each already encoded
    /// as a [`ServerFrame`] (never a `Batch`).
    Batch { frames: Vec<Vec<u8>> },
    /// Answer to [`ClientFrame::Ping`].
    Pong { nonce: u32 },
    /// The server is closing the connection: a close code and a reason.
    Disconnect { code: u16, reason: String },
}

impl ServerFrame {
    /// The frames inside a [`ServerFrame::Batch`], decoded.
    pub fn unbatch(frames: &[Vec<u8>]) -> Result<Vec<ServerFrame>, ProtocolError> {
        frames
            .iter()
            .map(|bytes| decode_server(bytes, usize::MAX))
            .collect()
    }
}

/// Encodes a server frame.
pub fn encode_server(frame: &ServerFrame) -> bytes::Bytes {
    bytes::Bytes::from(crate::bitcode_encode(frame))
}

/// Encodes a client frame.
pub fn encode_client(frame: &ClientFrame) -> Vec<u8> {
    crate::bitcode_encode(frame)
}

/// Decodes a frame from an untrusted client, rejecting frames over `max_size` bytes
/// before decoding them.
pub fn decode_client(bytes: &[u8], max_size: usize) -> Result<ClientFrame, ProtocolError> {
    check_size(bytes, max_size)?;
    Ok(bitcode::decode(bytes)?)
}

/// Decodes a frame from the server (used by clients and tests).
pub fn decode_server(bytes: &[u8], max_size: usize) -> Result<ServerFrame, ProtocolError> {
    check_size(bytes, max_size)?;
    Ok(bitcode::decode(bytes)?)
}

fn check_size(bytes: &[u8], max: usize) -> Result<(), ProtocolError> {
    let max = if max == 0 { DEFAULT_MAX_FRAME_SIZE } else { max };
    if bytes.len() > max {
        return Err(ProtocolError::TooLarge { size: bytes.len(), max });
    }
    Ok(())
}

/// The frame as JSON, for logs and captures of bitcode traffic.
#[cfg(feature = "debug-json")]
pub fn frame_to_json(frame: &ServerFrame) -> serde_json::Value {
    serde_json::to_value(frame).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_frames() -> Vec<ServerFrame> {
        vec![
            ServerFrame::HelloAck {
                protocol_version: 1,
                codec: 0,
                dictionary_id: None,
                event_table: vec![(0, "chat:message".into())],
            },
            ServerFrame::Event { key: EventKey::Id(0), payload: vec![1, 2, 3] },
            ServerFrame::ZoneEnter {
                handle: 7,
                object_id: [9; 16],
                object_type: "Ship".into(),
                channel: 0,
                zone_data: vec![4, 5],
            },
            ServerFrame::GorcEvent {
                handle: 7,
                channel: 0,
                seq: u16::MAX,
                event: EventKey::name("move"),
                payload: vec![6],
            },
            ServerFrame::ZoneExit { handle: 7, channel: 0 },
            ServerFrame::Pong { nonce: 42 },
            ServerFrame::Disconnect { code: 4005, reason: "bye".into() },
        ]
    }

    #[test]
    fn server_frames_round_trip() {
        for frame in server_frames() {
            let bytes = encode_server(&frame);
            assert_eq!(decode_server(&bytes, 0).unwrap(), frame);
        }
    }

    #[test]
    fn batches_hold_encoded_frames() {
        let inner: Vec<Vec<u8>> = server_frames().iter().map(|f| encode_server(f).to_vec()).collect();
        let batch = ServerFrame::Batch { frames: inner.clone() };
        let ServerFrame::Batch { frames } = decode_server(&encode_server(&batch), 0).unwrap() else {
            panic!("expected a batch");
        };
        assert_eq!(ServerFrame::unbatch(&frames).unwrap(), server_frames());
    }

    #[test]
    fn client_frames_round_trip() {
        let frames = [
            ClientFrame::Hello { protocol_version: 1, client: "test/1".into(), dictionary_id: Some(3) },
            ClientFrame::Event { key: EventKey::name("chat:message"), payload: vec![1] },
            ClientFrame::GorcEvent { handle: 1, channel: 2, event: EventKey::Id(4), payload: vec![] },
            ClientFrame::Ping { nonce: 1 },
        ];
        for frame in frames {
            assert_eq!(decode_client(&encode_client(&frame), 0).unwrap(), frame);
        }
    }

    #[test]
    fn untrusted_input_is_rejected_not_trusted() {
        assert!(matches!(
            decode_client(&vec![0u8; 100], 10),
            Err(ProtocolError::TooLarge { size: 100, max: 10 })
        ));
        assert!(decode_client(&[0xff, 0xff, 0xff], 0).is_err());
        // A JSON client talking to a bitcode listener.
        assert!(decode_client(br#"{"namespace":"a","event":"b","data":{}}"#, 0).is_err());
        assert!(decode_client(&[], 0).is_err());
    }

    #[test]
    fn a_position_frame_is_small() {
        // Handle + channel + seq + event id + a 12-byte payload (three f32).
        let frame = ServerFrame::GorcEvent {
            handle: 123_456,
            channel: 0,
            seq: 1000,
            event: EventKey::Id(3),
            payload: vec![0; 12],
        };
        let size = encode_server(&frame).len();
        assert!(size <= 30, "position frame is {size} bytes");
    }
}
