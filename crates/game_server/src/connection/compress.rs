//! Compression of a listener's outgoing frames.
//!
//! Large frames (JSON frames, zone entries) are compressed once, as they are queued,
//! and the compressed bytes are shared by every recipient: the GORC emitters hand the
//! same frame to all subscribers one after the other, so a one-entry cache keyed by the
//! frame's allocation catches the whole fan-out.
//!
//! Small frames gain nothing compressed alone. They are queued as they are, and each
//! connection's writer groups those waiting into one batch, then compresses the batch.

use bytes::Bytes;
use horizon_event_system::{
    protocol::{encode_server, json, Compression, ServerFrame},
    Format,
};
use std::sync::Mutex;

/// Batches stop growing past this many uncompressed bytes.
const MAX_BATCH_BYTES: usize = 32 * 1024;

/// A listener's compression settings, shared by its connections.
#[derive(Debug, Default)]
pub struct ListenerCompression {
    compression: Compression,
    /// The last large frame compressed, and its compressed form.
    last: Mutex<Option<(Bytes, Bytes)>>,
}

impl ListenerCompression {
    /// Compression with these settings.
    pub fn new(compression: Compression) -> Self {
        Self { compression, last: Mutex::new(None) }
    }

    /// No compression.
    pub fn none() -> Self {
        Self::default()
    }

    /// The settings.
    pub fn settings(&self) -> &Compression {
        &self.compression
    }

    /// Whether frames on this listener carry a header byte.
    pub fn has_header(&self) -> bool {
        self.compression.has_header()
    }

    /// The wire form of a frame large enough to be compressed on its own, compressed
    /// once for all its recipients. `None` for frames the writer should batch (and for
    /// every frame on a listener without compression).
    pub fn wrap_large(&self, frame: &Bytes) -> Option<Bytes> {
        if !self.has_header() || frame.len() < self.compression.min_size {
            return None;
        }
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((original, wrapped)) = last.as_ref() {
            // The cache holds the original, so its allocation cannot be reused for
            // another frame while it is cached: same pointer and length, same frame.
            if original.as_ptr() == frame.as_ptr() && original.len() == frame.len() {
                return Some(wrapped.clone());
            }
        }
        let wrapped = self.compression.wrap(frame);
        *last = Some((frame.clone(), wrapped.clone()));
        Some(wrapped)
    }

    /// The wire forms of a writer's waiting frames: unwrapped frames are grouped into
    /// batches (one frame alone stays itself) and wrapped; wrapped frames go as they
    /// are, in order.
    pub fn wire_frames(&self, format: Format, frames: Vec<(Bytes, bool)>) -> Vec<Bytes> {
        if !self.has_header() {
            return frames.into_iter().map(|(bytes, _)| bytes).collect();
        }
        let mut out = Vec::with_capacity(frames.len());
        let mut group: Vec<Bytes> = Vec::new();
        let mut group_bytes = 0;
        for (bytes, wrapped) in frames {
            if wrapped {
                self.flush_group(format, &mut group, &mut group_bytes, &mut out);
                out.push(bytes);
                continue;
            }
            group_bytes += bytes.len();
            group.push(bytes);
            if group_bytes >= MAX_BATCH_BYTES {
                self.flush_group(format, &mut group, &mut group_bytes, &mut out);
            }
        }
        self.flush_group(format, &mut group, &mut group_bytes, &mut out);
        out
    }

    fn flush_group(&self, format: Format, group: &mut Vec<Bytes>, group_bytes: &mut usize, out: &mut Vec<Bytes>) {
        *group_bytes = 0;
        match group.len() {
            0 => {}
            1 => out.push(self.compression.wrap(&group.pop().unwrap_or_default())),
            _ => {
                let batch = batch(format, group);
                group.clear();
                out.push(self.compression.wrap(&batch));
            }
        }
    }
}

/// Several encoded frames as one batch frame.
fn batch(format: Format, frames: &[Bytes]) -> Vec<u8> {
    match format {
        Format::Json => {
            let slices: Vec<&[u8]> = frames.iter().map(|f| f.as_ref()).collect();
            json::Batch::encode(&slices)
        }
        Format::Bitcode => encode_server(&ServerFrame::Batch {
            frames: frames.iter().map(|f| f.to_vec()).collect(),
        })
        .to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use horizon_event_system::protocol::{decode_server, Codec};

    fn zstd() -> ListenerCompression {
        ListenerCompression::new(Compression { min_size: 64, ..Compression::with_codec(Codec::Zstd) })
    }

    #[test]
    fn large_frames_are_compressed_once_for_every_recipient() {
        let compression = zstd();
        let frame = Bytes::from(vec![b'a'; 1000]);
        let first = compression.wrap_large(&frame).unwrap();
        let second = compression.wrap_large(&frame.clone()).unwrap();
        assert_eq!(first.as_ptr(), second.as_ptr(), "the second recipient reuses the first compression");
        assert!(first.len() < 100);
        // Small frames are left for the writer.
        assert_eq!(compression.wrap_large(&Bytes::from_static(b"small")), None);
        assert_eq!(ListenerCompression::none().wrap_large(&frame), None);
    }

    #[test]
    fn small_frames_are_batched_then_compressed() {
        let compression = zstd();
        let frame = |i: u8| encode_server(&ServerFrame::Pong { nonce: i as u32 });
        let waiting: Vec<(Bytes, bool)> = (0..50).map(|i| (frame(i), false)).collect();
        let wire = compression.wire_frames(Format::Bitcode, waiting);
        assert_eq!(wire.len(), 1, "50 small frames go as one batch");
        let unwrapped = compression.settings().unwrap(&wire[0], 1 << 20).unwrap();
        let ServerFrame::Batch { frames } = decode_server(&unwrapped, 0).unwrap() else {
            panic!("expected a batch");
        };
        assert_eq!(ServerFrame::unbatch(&frames).unwrap()[49], ServerFrame::Pong { nonce: 49 });
    }

    #[test]
    fn wrapped_frames_keep_their_place() {
        let compression = zstd();
        let small = Bytes::from_static(br#"{"a":1}"#);
        let large = compression.wrap_large(&Bytes::from(vec![b'x'; 500])).unwrap();
        let wire = compression.wire_frames(
            Format::Json,
            vec![(small.clone(), false), (small.clone(), false), (large.clone(), true), (small, false)],
        );
        assert_eq!(wire.len(), 3);
        assert_eq!(wire[1], large);
        let batch = compression.settings().unwrap(&wire[0], 4096).unwrap();
        let batch: serde_json::Value = serde_json::from_slice(&batch).unwrap();
        assert_eq!(batch["type"], "batch");
        assert_eq!(batch["frames"].as_array().unwrap().len(), 2);
        assert_eq!(&compression.settings().unwrap(&wire[2], 4096).unwrap()[..], br#"{"a":1}"#);
    }

    #[test]
    fn without_compression_frames_go_one_by_one() {
        let none = ListenerCompression::none();
        let frames = vec![(Bytes::from_static(b"a"), false), (Bytes::from_static(b"b"), false)];
        assert_eq!(none.wire_frames(Format::Json, frames), vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    }
}
