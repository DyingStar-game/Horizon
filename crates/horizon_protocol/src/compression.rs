//! Frame compression: zstd or lz4, chosen per listener.
//!
//! On a listener with compression, every frame starts with one header byte:
//!
//! | bits | meaning                                          |
//! |------|--------------------------------------------------|
//! | 0-1  | codec of the body: `00` none, `01` zstd, `10` lz4 |
//! | 2    | the body was compressed with the dictionary      |
//! | 3-7  | reserved, zero                                   |
//!
//! A listener without compression has no header byte at all, so its frames are exactly
//! those of `json` or `bitcode`.
//!
//! Small frames gain nothing on their own (zstd's own frame header is several bytes),
//! so frames under `min_size` go uncompressed with the codec bits cleared. The gain
//! comes from frames compressed together: the server's writer groups a player's small
//! frames into one batch and compresses the batch.
//!
//! Decompression is capped: a small compressed frame from a client cannot expand past
//! the listener's maximum frame size.

use crate::ProtocolError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

/// The compression codec of a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    /// No compression, and no header byte.
    #[default]
    None,
    /// zstd: the better ratio, especially on bitcode and with a dictionary.
    Zstd,
    /// lz4: faster, lower ratio.
    Lz4,
}

impl Codec {
    /// The codec's number in the header byte and in `HelloAck`.
    pub fn bits(self) -> u8 {
        match self {
            Codec::None => 0,
            Codec::Zstd => 1,
            Codec::Lz4 => 2,
        }
    }

    /// The codec with this number.
    pub fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0 => Some(Codec::None),
            1 => Some(Codec::Zstd),
            2 => Some(Codec::Lz4),
            _ => None,
        }
    }

    /// The codec's name as it appears in configuration files.
    pub fn as_str(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Zstd => "zstd",
            Codec::Lz4 => "lz4",
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

const CODEC_MASK: u8 = 0b011;
const DICTIONARY_BIT: u8 = 0b100;
const RESERVED_MASK: u8 = !0b111;

/// A compression dictionary, trained on the game's own payloads.
#[derive(Clone)]
pub struct Dictionary {
    id: u32,
    bytes: Arc<[u8]>,
}

impl std::fmt::Debug for Dictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dictionary").field("id", &self.id).field("len", &self.bytes.len()).finish()
    }
}

impl Dictionary {
    /// A dictionary from its bytes. Its id, which client and server compare during the
    /// handshake, is a hash of the bytes.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        Self { id: fnv1a(&bytes), bytes }
    }

    /// The dictionary's id.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The dictionary's bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in bytes {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// The compression settings of a listener.
#[derive(Debug, Clone)]
pub struct Compression {
    /// The codec; [`Codec::None`] means no header byte.
    pub codec: Codec,
    /// zstd level (1 to 3 for the hot path; negative is faster). Ignored by lz4.
    pub level: i32,
    /// Frames shorter than this are sent uncompressed.
    pub min_size: usize,
    /// Optional dictionary.
    pub dictionary: Option<Dictionary>,
}

impl Default for Compression {
    fn default() -> Self {
        Self::none()
    }
}

impl Compression {
    /// No compression.
    pub fn none() -> Self {
        Self { codec: Codec::None, level: 1, min_size: 128, dictionary: None }
    }

    /// `codec` with the default level and threshold.
    pub fn with_codec(codec: Codec) -> Self {
        Self { codec, ..Self::none() }
    }

    /// Whether frames carry a header byte.
    pub fn has_header(&self) -> bool {
        self.codec != Codec::None
    }

    /// The dictionary's id, if any.
    pub fn dictionary_id(&self) -> Option<u32> {
        self.dictionary.as_ref().map(Dictionary::id)
    }

    /// Wraps a frame for the wire: as is without compression; otherwise the header byte
    /// and the body, compressed when the frame is at least `min_size` bytes and
    /// compressing makes it smaller.
    pub fn wrap(&self, frame: &[u8]) -> Bytes {
        if !self.has_header() {
            return Bytes::copy_from_slice(frame);
        }
        if frame.len() >= self.min_size {
            if let Some(compressed) = self.compress(frame) {
                if compressed.len() < frame.len() {
                    let mut out = Vec::with_capacity(compressed.len() + 1);
                    out.push(self.header());
                    out.extend_from_slice(&compressed);
                    return Bytes::from(out);
                }
            }
        }
        Self::raw(frame)
    }

    /// A frame with a header byte saying it is not compressed.
    pub fn raw(frame: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(frame.len() + 1);
        out.push(0);
        out.extend_from_slice(frame);
        Bytes::from(out)
    }

    /// Reads a frame from the wire: without compression, `bytes` as is; otherwise the
    /// header byte then the body, decompressed to at most `max_size` bytes.
    pub fn unwrap<'a>(&self, bytes: &'a [u8], max_size: usize) -> Result<Cow<'a, [u8]>, ProtocolError> {
        if !self.has_header() {
            return Ok(Cow::Borrowed(bytes));
        }
        let (&header, body) = bytes.split_first().ok_or(ProtocolError::BadHeader(None))?;
        if header & RESERVED_MASK != 0 {
            return Err(ProtocolError::BadHeader(Some(header)));
        }
        let codec = Codec::from_bits(header & CODEC_MASK).ok_or(ProtocolError::BadHeader(Some(header)))?;
        let with_dictionary = header & DICTIONARY_BIT != 0;
        let dictionary = match (with_dictionary, &self.dictionary) {
            (false, _) => None,
            (true, Some(dictionary)) => Some(dictionary.bytes()),
            (true, None) => return Err(ProtocolError::BadHeader(Some(header))),
        };
        match codec {
            Codec::None => Ok(Cow::Borrowed(body)),
            Codec::Zstd => decompress_zstd(body, dictionary, max_size).map(Cow::Owned),
            Codec::Lz4 => decompress_lz4(body, dictionary, max_size).map(Cow::Owned),
        }
    }

    fn header(&self) -> u8 {
        let dictionary = if self.dictionary.is_some() { DICTIONARY_BIT } else { 0 };
        self.codec.bits() | dictionary
    }

    fn compress(&self, frame: &[u8]) -> Option<Vec<u8>> {
        let dictionary = self.dictionary.as_ref().map(Dictionary::bytes);
        match self.codec {
            Codec::None => None,
            Codec::Zstd => ZSTD_COMPRESSOR.with(|cell| {
                // A zstd context is costly to create: one per thread, kept while the
                // level and dictionary stay the same.
                let key = (self.level, self.dictionary_id());
                let mut cached = cell.borrow_mut();
                if cached.as_ref().map(|(k, _)| *k) != Some(key) {
                    let mut compressor = match dictionary {
                        Some(dict) => zstd::bulk::Compressor::with_dictionary(self.level, dict).ok()?,
                        None => zstd::bulk::Compressor::new(self.level).ok()?,
                    };
                    // Saves bytes on frames the receiver can only read whole anyway.
                    let _ = compressor.include_checksum(false);
                    let _ = compressor.include_dictid(false);
                    *cached = Some((key, compressor));
                }
                cached.as_mut()?.1.compress(frame).ok()
            }),
            Codec::Lz4 => Some(match dictionary {
                Some(dict) => lz4_flex::block::compress_prepend_size_with_dict(frame, dict),
                None => lz4_flex::block::compress_prepend_size(frame),
            }),
        }
    }
}

thread_local! {
    static ZSTD_COMPRESSOR: std::cell::RefCell<Option<((i32, Option<u32>), zstd::bulk::Compressor<'static>)>> =
        const { std::cell::RefCell::new(None) };
}

fn too_large(size: usize, max: usize) -> ProtocolError {
    ProtocolError::TooLarge { size, max }
}

fn decompress_zstd(body: &[u8], dictionary: Option<&[u8]>, max_size: usize) -> Result<Vec<u8>, ProtocolError> {
    // The declared size is checked before anything is allocated.
    if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(body) {
        if size as usize > max_size {
            return Err(too_large(size as usize, max_size));
        }
    }
    let mut decompressor = match dictionary {
        Some(dict) => zstd::bulk::Decompressor::with_dictionary(dict),
        None => zstd::bulk::Decompressor::new(),
    }
    .map_err(|e| ProtocolError::Decompression(e.to_string()))?;
    // Fails rather than grow past max_size.
    decompressor
        .decompress(body, max_size)
        .map_err(|e| ProtocolError::Decompression(e.to_string()))
}

fn decompress_lz4(body: &[u8], dictionary: Option<&[u8]>, max_size: usize) -> Result<Vec<u8>, ProtocolError> {
    let (size, _) = lz4_flex::block::uncompressed_size(body).map_err(|e| ProtocolError::Decompression(e.to_string()))?;
    if size > max_size {
        return Err(too_large(size, max_size));
    }
    match dictionary {
        Some(dict) => lz4_flex::block::decompress_size_prepended_with_dict(body, dict),
        None => lz4_flex::block::decompress_size_prepended(body),
    }
    .map_err(|e| ProtocolError::Decompression(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(len: usize) -> Vec<u8> {
        // Repetitive like real frames: field layouts and close values repeat.
        (0..len).map(|i| b"position:{x:1.0,y:2.0}"[i % 22]).collect()
    }

    #[test]
    fn without_compression_frames_are_untouched() {
        let none = Compression::none();
        assert_eq!(&none.wrap(b"abc")[..], b"abc");
        assert_eq!(none.unwrap(b"abc", 10).unwrap(), Cow::Borrowed(&b"abc"[..]));
    }

    #[test]
    fn small_frames_go_raw_with_a_header() {
        for codec in [Codec::Zstd, Codec::Lz4] {
            let compression = Compression::with_codec(codec);
            let wrapped = compression.wrap(b"tiny");
            assert_eq!(&wrapped[..], b"\0tiny");
            assert_eq!(&compression.unwrap(&wrapped, 100).unwrap()[..], b"tiny");
        }
    }

    #[test]
    fn large_frames_are_compressed_and_round_trip() {
        let frame = sample(4096);
        for codec in [Codec::Zstd, Codec::Lz4] {
            let compression = Compression::with_codec(codec);
            let wrapped = compression.wrap(&frame);
            assert_eq!(wrapped[0] & CODEC_MASK, codec.bits());
            assert!(wrapped.len() < frame.len() / 4, "{codec}: {} bytes", wrapped.len());
            assert_eq!(compression.unwrap(&wrapped, 8192).unwrap().as_ref(), &frame[..]);
        }
    }

    #[test]
    fn dictionaries_help_small_frames_and_must_match() {
        let dictionary = Dictionary::new(sample(1024));
        let frame = sample(200);
        for codec in [Codec::Zstd, Codec::Lz4] {
            let plain = Compression { min_size: 0, ..Compression::with_codec(codec) };
            let with_dict = Compression { dictionary: Some(dictionary.clone()), ..plain.clone() };
            let small = with_dict.wrap(&frame);
            assert!(small[0] & DICTIONARY_BIT != 0);
            assert!(small.len() < plain.wrap(&frame).len(), "{codec}: the dictionary helps");
            assert_eq!(with_dict.unwrap(&small, 1024).unwrap().as_ref(), &frame[..]);
            // A receiver without the dictionary refuses the frame.
            assert!(plain.unwrap(&small, 1024).is_err());
        }
        assert_eq!(dictionary.id(), Dictionary::new(sample(1024)).id());
        assert_ne!(dictionary.id(), Dictionary::new(sample(1000)).id());
    }

    #[test]
    fn decompression_is_capped() {
        // 1 MiB of zeros compresses to a few bytes: it must not expand past the cap.
        let bomb = vec![0u8; 1 << 20];
        for codec in [Codec::Zstd, Codec::Lz4] {
            let compression = Compression::with_codec(codec);
            let wrapped = compression.wrap(&bomb);
            assert!(wrapped.len() < 8 * 1024);
            assert!(matches!(compression.unwrap(&wrapped, 64 * 1024), Err(ProtocolError::TooLarge { .. })));
        }
    }

    #[test]
    fn bad_headers_are_rejected() {
        let compression = Compression::with_codec(Codec::Zstd);
        assert!(compression.unwrap(&[], 10).is_err());
        assert!(compression.unwrap(&[0b1000_0000, 1], 10).is_err(), "reserved bits");
        assert!(compression.unwrap(&[0b11, 1], 10).is_err(), "unknown codec");
        assert!(compression.unwrap(&[0b101, 1], 10).is_err(), "dictionary the receiver lacks");
        assert!(compression.unwrap(&[0b01, 1, 2, 3], 10).is_err(), "not zstd");
    }
}
