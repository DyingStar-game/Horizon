//! Framing of the reliable WebTransport stream: each frame is preceded by its length,
//! as an unsigned LEB128 varint (1 byte up to 127, 2 bytes up to 16383...).
//!
//! [`FrameSplitter`] reads that framing back from bytes as they arrive, whatever the
//! read sizes, and refuses lengths above a limit before buffering them.

use crate::ProtocolError;

/// The length prefix of a `len`-byte frame.
pub fn length_prefix(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    let mut value = len as u64;
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// Appends `frame`, prefixed by its length, to `out`.
pub fn write_frame(out: &mut Vec<u8>, frame: &[u8]) {
    out.extend_from_slice(&length_prefix(frame.len()));
    out.extend_from_slice(frame);
}

/// Splits a byte stream back into frames.
#[derive(Debug, Default)]
pub struct FrameSplitter {
    buffer: Vec<u8>,
}

impl FrameSplitter {
    /// An empty splitter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds bytes read from the stream.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// The next complete frame, if the buffer holds one. A declared length above
    /// `max_size` is an error, raised as soon as the prefix is read.
    pub fn next_frame(&mut self, max_size: usize) -> Result<Option<Vec<u8>>, ProtocolError> {
        let mut len: u64 = 0;
        let mut prefix = 0;
        loop {
            let Some(&byte) = self.buffer.get(prefix) else {
                return Ok(None);
            };
            if prefix == 9 {
                return Err(ProtocolError::TooLarge { size: usize::MAX, max: max_size });
            }
            len |= ((byte & 0x7f) as u64) << (7 * prefix);
            prefix += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if len > max_size {
            return Err(ProtocolError::TooLarge { size: len, max: max_size });
        }
        if self.buffer.len() < prefix + len {
            return Ok(None);
        }
        let frame = self.buffer[prefix..prefix + len].to_vec();
        self.buffer.drain(..prefix + len);
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_are_leb128() {
        assert_eq!(length_prefix(0), vec![0]);
        assert_eq!(length_prefix(127), vec![127]);
        assert_eq!(length_prefix(128), vec![0x80, 1]);
        assert_eq!(length_prefix(300), vec![0xac, 0x02]);
    }

    #[test]
    fn frames_come_back_whatever_the_read_sizes() {
        let frames: Vec<Vec<u8>> = vec![vec![1; 3], vec![], vec![2; 200], vec![3; 70_000]];
        let mut stream = Vec::new();
        for frame in &frames {
            write_frame(&mut stream, frame);
        }
        for chunk in [1usize, 7, 1000, stream.len()] {
            let mut splitter = FrameSplitter::new();
            let mut out = Vec::new();
            for piece in stream.chunks(chunk) {
                splitter.push(piece);
                while let Some(frame) = splitter.next_frame(1 << 20).unwrap() {
                    out.push(frame);
                }
            }
            assert_eq!(out, frames, "chunks of {chunk}");
        }
    }

    #[test]
    fn oversized_lengths_are_refused_before_buffering() {
        let mut splitter = FrameSplitter::new();
        splitter.push(&length_prefix(1 << 30));
        assert!(matches!(splitter.next_frame(1024), Err(ProtocolError::TooLarge { .. })));
        let mut endless = FrameSplitter::new();
        endless.push(&[0xff; 12]);
        assert!(endless.next_frame(1024).is_err());
    }
}
