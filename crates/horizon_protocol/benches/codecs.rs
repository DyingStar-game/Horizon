//! Encode, compress and decompress costs per format and codec, on position updates.
//!
//! `cargo bench -p horizon_protocol`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use horizon_protocol::payload::{decode_packed, decode_serde, encode_packed, encode_serde};
use horizon_protocol::samples::{batch, bitcode_frame, json_frame, transform, Transform};
use horizon_protocol::{Codec, Compression, Format};

fn payload_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload");
    let value = transform(7);
    group.bench_function("json encode", |b| b.iter(|| encode_serde(Format::Json, black_box(&value)).unwrap()));
    group.bench_function("bitcode serde encode", |b| b.iter(|| encode_serde(Format::Bitcode, black_box(&value)).unwrap()));
    group.bench_function("bitcode derive encode", |b| b.iter(|| encode_packed(Format::Bitcode, black_box(&value)).unwrap()));
    let json = encode_serde(Format::Json, &value).unwrap();
    let serde_bits = encode_serde(Format::Bitcode, &value).unwrap();
    let derive_bits = encode_packed(Format::Bitcode, &value).unwrap();
    group.bench_function("json decode", |b| b.iter(|| decode_serde::<Transform>(Format::Json, black_box(&json)).unwrap()));
    group.bench_function("bitcode serde decode", |b| b.iter(|| decode_serde::<Transform>(Format::Bitcode, black_box(&serde_bits)).unwrap()));
    group.bench_function("bitcode derive decode", |b| b.iter(|| decode_packed::<Transform>(Format::Bitcode, black_box(&derive_bits)).unwrap()));
    group.finish();
}

fn compression(c: &mut Criterion) {
    let codecs = [
        ("zstd -1", Compression { codec: Codec::Zstd, level: -1, min_size: 0, dictionary: None }),
        ("zstd 1", Compression { codec: Codec::Zstd, level: 1, min_size: 0, dictionary: None }),
        ("lz4", Compression { codec: Codec::Lz4, level: 0, min_size: 0, dictionary: None }),
    ];
    for (format, bitcode) in [("json", false), ("bitcode", true)] {
        let frames = [
            ("1 frame", if bitcode { bitcode_frame(0) } else { json_frame(0) }),
            ("batch of 50", batch(bitcode, 50)),
        ];
        let mut group = c.benchmark_group(format!("compress {format}"));
        for (frames_label, frame) in &frames {
            group.throughput(Throughput::Bytes(frame.len() as u64));
            for (codec_label, compression) in &codecs {
                group.bench_with_input(BenchmarkId::new(*codec_label, frames_label), frame, |b, frame| {
                    b.iter(|| compression.wrap(black_box(frame)))
                });
            }
        }
        group.finish();

        let mut group = c.benchmark_group(format!("decompress {format}"));
        for (frames_label, frame) in &frames {
            group.throughput(Throughput::Bytes(frame.len() as u64));
            for (codec_label, compression) in &codecs {
                let wrapped = compression.wrap(frame);
                group.bench_with_input(BenchmarkId::new(*codec_label, frames_label), &wrapped, |b, wrapped| {
                    b.iter(|| compression.unwrap(black_box(wrapped), 1 << 20).unwrap())
                });
            }
        }
        group.finish();
    }
}

criterion_group!(benches, payload_encoding, compression);
criterion_main!(benches);
