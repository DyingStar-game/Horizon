//! Prints the wire size of position updates per format and codec, alone and batched,
//! with and without a dictionary trained on similar frames.
//!
//! `cargo run -p horizon_protocol --release --example frame_sizes`

use horizon_protocol::samples::{batch, bitcode_frame, json_frame};
use horizon_protocol::{Codec, Compression, Dictionary};

fn dictionary(bitcode: bool) -> Dictionary {
    let samples: Vec<Vec<u8>> = (10_000..12_000)
        .map(|i| if bitcode { bitcode_frame(i) } else { json_frame(i) })
        .collect();
    Dictionary::new(zstd::dict::from_samples(&samples, 4096).expect("train dictionary"))
}

fn main() {
    println!("| Format | Frames | Uncompressed | zstd -1 | zstd 1 | zstd 3 | lz4 | zstd 1 + dict | lz4 + dict |");
    println!("|---|---|---|---|---|---|---|---|---|");
    for bitcode in [false, true] {
        let dict = dictionary(bitcode);
        for count in [1usize, 10, 50] {
            let frame = if count == 1 {
                if bitcode { bitcode_frame(0) } else { json_frame(0) }
            } else {
                batch(bitcode, count)
            };
            let size = |codec: Codec, level: i32, dictionary: Option<&Dictionary>| {
                let compression = Compression { codec, level, min_size: 0, dictionary: dictionary.cloned() };
                compression.wrap(&frame).len()
            };
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                if bitcode { "bitcode" } else { "JSON" },
                count,
                frame.len(),
                size(Codec::Zstd, -1, None),
                size(Codec::Zstd, 1, None),
                size(Codec::Zstd, 3, None),
                size(Codec::Lz4, 0, None),
                size(Codec::Zstd, 1, Some(&dict)),
                size(Codec::Lz4, 0, Some(&dict)),
            );
        }
    }
}
