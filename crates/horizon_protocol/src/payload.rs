//! Encoding of game payloads.
//!
//! A payload can be encoded two ways in bitcode:
//!
//! * Through serde ([`encode_serde`] / [`decode_serde`]): works with any type that
//!   implements serde, which is every payload type that works with JSON today. It is
//!   slower and somewhat larger than the derives, and cannot handle types that need a
//!   self-describing format: `serde_json::Value`, `#[serde(untagged)]`,
//!   `#[serde(flatten)]`. Such payloads stay JSON-only.
//! * Through the bitcode derives ([`encode_packed`] / [`decode_packed`]): fastest and
//!   smallest, for hot types such as transforms.
//!
//! In JSON, payloads are always serde values.

use crate::{Format, ProtocolError};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

/// Types bitcode's serde mode cannot encode, by type name. bitcode panics ("type
/// changed") instead of returning an error for them; once a type has panicked, it is
/// refused at once so the panic (and its log line) does not repeat on every frame.
static UNSUPPORTED: Mutex<Option<HashSet<&'static str>>> = Mutex::new(None);

fn is_unsupported(type_name: &'static str) -> bool {
    UNSUPPORTED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|set| set.contains(type_name))
}

fn mark_unsupported(type_name: &'static str) -> ProtocolError {
    UNSUPPORTED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashSet::new)
        .insert(type_name);
    ProtocolError::NotBitcodeCompatible(type_name)
}

/// Encodes a payload in `format`, through serde.
///
/// A type bitcode's serde mode cannot encode (see the module docs) gives
/// [`ProtocolError::NotBitcodeCompatible`], never a panic.
pub fn encode_serde<T: Serialize + ?Sized>(format: Format, payload: &T) -> Result<Vec<u8>, ProtocolError> {
    Ok(match format {
        Format::Json => serde_json::to_vec(payload)?,
        Format::Bitcode => {
            let type_name = std::any::type_name::<T>();
            if is_unsupported(type_name) {
                return Err(ProtocolError::NotBitcodeCompatible(type_name));
            }
            catch_unwind(AssertUnwindSafe(|| bitcode::serialize(payload))).map_err(|_| mark_unsupported(type_name))??
        }
    })
}

/// Decodes a payload in `format`, through serde. Like [`encode_serde`], never panics.
pub fn decode_serde<T: DeserializeOwned>(format: Format, bytes: &[u8]) -> Result<T, ProtocolError> {
    Ok(match format {
        Format::Json => serde_json::from_slice(bytes)?,
        Format::Bitcode => {
            let type_name = std::any::type_name::<T>();
            if is_unsupported(type_name) {
                return Err(ProtocolError::NotBitcodeCompatible(type_name));
            }
            catch_unwind(AssertUnwindSafe(|| bitcode::deserialize(bytes))).map_err(|_| mark_unsupported(type_name))??
        }
    })
}

/// Encodes a payload in `format`: serde for JSON, the bitcode derive for bitcode.
pub fn encode_packed<T>(format: Format, payload: &T) -> Result<Vec<u8>, ProtocolError>
where
    T: Serialize + bitcode::Encode + ?Sized,
{
    Ok(match format {
        Format::Json => serde_json::to_vec(payload)?,
        Format::Bitcode => bitcode::encode(payload),
    })
}

/// Decodes a payload in `format`: serde for JSON, the bitcode derive for bitcode.
pub fn decode_packed<T>(format: Format, bytes: &[u8]) -> Result<T, ProtocolError>
where
    T: DeserializeOwned + bitcode::DecodeOwned,
{
    Ok(match format {
        Format::Json => serde_json::from_slice(bytes)?,
        Format::Bitcode => bitcode::decode(bytes)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, bitcode::Encode, bitcode::Decode)]
    struct Move {
        x: f32,
        y: f32,
        z: f32,
        sprint: bool,
    }

    fn sample() -> Move {
        Move { x: 1.5, y: -2.0, z: 3.25, sprint: true }
    }

    #[test]
    fn serde_mode_round_trips_in_both_formats() {
        for format in Format::ALL {
            let bytes = encode_serde(format, &sample()).unwrap();
            assert_eq!(decode_serde::<Move>(format, &bytes).unwrap(), sample());
        }
    }

    #[test]
    fn packed_mode_round_trips_in_both_formats() {
        for format in Format::ALL {
            let bytes = encode_packed(format, &sample()).unwrap();
            assert_eq!(decode_packed::<Move>(format, &bytes).unwrap(), sample());
        }
    }

    #[test]
    fn bitcode_is_much_smaller_than_json() {
        let json = encode_serde(Format::Json, &sample()).unwrap().len();
        let bitcode = encode_packed(Format::Bitcode, &sample()).unwrap().len();
        assert!(bitcode * 3 < json, "bitcode {bitcode} bytes, json {json} bytes");
    }

    #[test]
    fn types_bitcode_cannot_encode_are_errors_not_panics() {
        let value = serde_json::json!({"a": 1, "b": "x", "c": [true, null]});
        for _ in 0..2 {
            assert!(matches!(
                encode_serde(Format::Bitcode, &value),
                Err(ProtocolError::NotBitcodeCompatible(_))
            ));
        }
        assert!(encode_serde(Format::Json, &value).is_ok());
    }

    #[test]
    fn self_describing_types_need_json() {
        let value = serde_json::json!({"any": "shape"});
        assert!(encode_serde(Format::Json, &value).is_ok());
        let bytes = encode_serde(Format::Json, &value).unwrap();
        assert!(decode_serde::<serde_json::Value>(Format::Bitcode, &bytes).is_err());
    }
}
