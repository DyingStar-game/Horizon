//! Wire protocol between Horizon and its clients.
//!
//! Horizon owns the envelope: the small set of frames a client and the server exchange
//! (events, GORC events, zone enter and exit, ping, disconnect). The payload inside a
//! frame belongs to the game and is opaque to this crate.
//!
//! Two formats carry the same frames:
//!
//! * [`Format::Json`]: today's JSON envelopes, unchanged on the wire (see [`json`]).
//! * [`Format::Bitcode`]: [`ClientFrame`] and [`ServerFrame`] encoded with `bitcode`,
//!   with numeric event keys and per-server object handles to keep frames small.
//!
//! bitcode is not self-describing: a client and a server must agree on
//! [`PROTOCOL_VERSION`] (and on the exact `bitcode` version, pinned in this crate's
//! manifest). The client says which version it speaks in [`ClientFrame::Hello`].

pub mod close;
pub mod compression;
pub mod event_table;
pub mod frame;
pub mod json;
pub mod payload;
#[doc(hidden)]
pub mod samples;
pub mod stream;

pub use bitcode;
pub use close::CloseCode;
pub use compression::{Codec, Compression, Dictionary};
pub use event_table::EventTable;
pub use frame::{decode_client, decode_server, encode_client, encode_server, ClientFrame, EventKey, ServerFrame};

use serde::{Deserialize, Serialize};

/// Version of the frame layout. Bumped whenever a frame changes, or `bitcode` is
/// upgraded.
pub const PROTOCOL_VERSION: u16 = 1;

/// Frames larger than this are rejected before decoding, unless the server is
/// configured otherwise.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024;

/// The message format of a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// Today's JSON envelopes.
    #[default]
    Json,
    /// `bitcode`-encoded [`ClientFrame`] / [`ServerFrame`].
    Bitcode,
}

impl Format {
    /// Both formats, for code that encodes a frame once per format in use.
    pub const ALL: [Format; 2] = [Format::Json, Format::Bitcode];

    /// The format's name as it appears in configuration files.
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Bitcode => "bitcode",
        }
    }

    /// Index into a `[_; 2]` cache keyed by format.
    pub fn index(self) -> usize {
        match self {
            Format::Json => 0,
            Format::Bitcode => 1,
        }
    }
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

thread_local! {
    /// bitcode sets up per-type encoder state on every call; a buffer reused per
    /// thread keeps it, which makes encoding small frames several times cheaper.
    static BITCODE_BUFFER: std::cell::RefCell<bitcode::Buffer> = std::cell::RefCell::new(bitcode::Buffer::new());
}

/// Encodes with the bitcode derive, reusing this thread's buffer.
pub(crate) fn bitcode_encode<T: bitcode::Encode + ?Sized>(value: &T) -> Vec<u8> {
    BITCODE_BUFFER.with(|buffer| match buffer.try_borrow_mut() {
        Ok(mut buffer) => buffer.encode(value).to_vec(),
        // Re-entered (an encoder calling back into us): no reuse this time.
        Err(_) => bitcode::encode(value),
    })
}

/// Why a frame could not be read or written.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// The frame is larger than the receiver accepts.
    #[error("frame of {size} bytes exceeds the {max} byte limit")]
    TooLarge { size: usize, max: usize },
    /// The bytes are not a valid bitcode frame.
    #[error("invalid bitcode frame: {0}")]
    Bitcode(#[from] bitcode::Error),
    /// The bytes are not a valid JSON frame.
    #[error("invalid JSON frame: {0}")]
    Json(#[from] serde_json::Error),
    /// The frame names an event the server does not know.
    #[error("unknown event id {0}")]
    UnknownEventId(u16),
    /// The frame names an object handle the receiver does not know.
    #[error("unknown object handle {0}")]
    UnknownHandle(u32),
    /// The payload type cannot go through bitcode's serde mode (it changes shape:
    /// `serde_json::Value`, untagged enums, flattened fields...): it is JSON-only.
    #[error("{0} cannot be encoded with bitcode's serde mode; send it to JSON clients or derive bitcode")]
    NotBitcodeCompatible(&'static str),
    /// The frame's header byte is missing or invalid (unknown codec, reserved bits, a
    /// dictionary the receiver does not have).
    #[error("invalid frame header {0:?}")]
    BadHeader(Option<u8>),
    /// The compressed body could not be decompressed.
    #[error("decompression failed: {0}")]
    Decompression(String),
}
