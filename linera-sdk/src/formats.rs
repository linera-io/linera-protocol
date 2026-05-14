// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support the declaration of the binary formats used by an application.

/// Re-exports the derive macros for the stable-enum tag scheme.
pub use linera_sdk_derive::{StableEnumDeserialize, StableEnumSerialize, StableEnumTrace};
use serde::{Deserialize, Serialize};
use serde_reflection::{
    json_converter::{DeserializationContext, EmptyEnvironment},
    Format, Registry, Samples, Tracer,
};

/// The serde formats used by an application. The exact serde encoding in use must be
/// known separately.
#[derive(Serialize, Deserialize, Debug, Eq, Clone, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub struct Formats {
    /// The registry of container definitions.
    pub registry: Registry,
    /// The format of operations.
    pub operation: Format,
    /// The format of operation responses.
    pub response: Format,
    /// The format of messages.
    pub message: Format,
    /// The format of events.
    pub event_value: Format,
}

/// An application using BCS as binary encoding.
pub trait BcsApplication {
    /// Link the public Abi of application for good measure.
    type Abi;

    /// Returns the serde formats for this application's ABI types.
    fn formats() -> serde_reflection::Result<Formats>;
}

/// An enum whose variant tags are computed at compile time from
/// `Keccak-256(variant_name)` (see
/// [`linera_sdk_derive::StableEnumSerialize`] /
/// [`linera_sdk_derive::StableEnumDeserialize`]).
///
/// Derive this trait with `#[derive(StableEnumTrace)]` to make the enum
/// usable with [`TracerExt::trace_stable_enum_type`].
///
/// The derive auto-generates `trace_all_variants` by calling
/// [`Tracer::trace_type_once`] for each field type to obtain a sample value,
/// then [`Tracer::trace_value`] to record the variant. As a result, every
/// field type must implement [`serde::de::DeserializeOwned`] (or otherwise be
/// traceable by [`Tracer::trace_type_once`]). Nested `StableEnum` fields are
/// not supported automatically — pre-trace them with
/// `trace_stable_enum_type::<NestedEnum>` first.
pub trait StableEnumTrace: Sized + Serialize {
    /// The `(variant_name, variant_tag)` pairs in declaration order.
    const STABLE_VARIANTS: &'static [(&'static str, u32)];

    /// Trace each variant of `Self` into `tracer`'s registry. The default
    /// derive implementation is sufficient for most cases.
    fn trace_all_variants(
        tracer: &mut Tracer,
        samples: &Samples,
    ) -> serde_reflection::Result<Format>;
}

/// Extension methods on [`Tracer`] for tracing enums whose variant tags are
/// not contiguous starting at zero.
///
/// The standard [`Tracer::trace_type`] discovers an enum's variants by probing
/// `0, 1, 2, …` until each `u32` index has been seen. With Keccak-derived
/// stable tags those indices are not consecutive (they live in `[2^27, 2^28)`),
/// so probing never terminates. This trait delegates to the enum's
/// [`StableEnumTrace`] impl, which drives tracing variant-by-variant via the
/// enum's [`Serialize`] impl.
pub trait TracerExt {
    /// Trace every variant of a stable-tagged enum, returning the enum's
    /// [`Format`] for use in [`Formats::operation`], [`Formats::response`],
    /// etc.
    fn trace_stable_enum_type<T>(&mut self, samples: &Samples) -> serde_reflection::Result<Format>
    where
        T: StableEnumTrace;
}

impl TracerExt for Tracer {
    fn trace_stable_enum_type<T>(&mut self, samples: &Samples) -> serde_reflection::Result<Format>
    where
        T: StableEnumTrace,
    {
        T::trace_all_variants(self, samples)
    }
}

/// Decode BCS-serialized `bytes` into a [`serde_json::Value`], guided by `format`
/// and the container `registry`.
pub fn bcs_to_json(
    bytes: &[u8],
    format: &Format,
    registry: &Registry,
) -> bcs::Result<serde_json::Value> {
    let context = DeserializationContext {
        format: format.clone(),
        registry,
        environment: &EmptyEnvironment,
    };
    bcs::from_bytes_seed(context, bytes)
}

impl Formats {
    /// Decode BCS-encoded operation bytes into a JSON value.
    pub fn decode_operation(&self, bytes: &[u8]) -> bcs::Result<serde_json::Value> {
        bcs_to_json(bytes, &self.operation, &self.registry)
    }

    /// Decode BCS-encoded operation response bytes into a JSON value.
    pub fn decode_response(&self, bytes: &[u8]) -> bcs::Result<serde_json::Value> {
        bcs_to_json(bytes, &self.response, &self.registry)
    }

    /// Decode BCS-encoded message bytes into a JSON value.
    pub fn decode_message(&self, bytes: &[u8]) -> bcs::Result<serde_json::Value> {
        bcs_to_json(bytes, &self.message, &self.registry)
    }

    /// Decode BCS-encoded event value bytes into a JSON value.
    pub fn decode_event_value(&self, bytes: &[u8]) -> bcs::Result<serde_json::Value> {
        bcs_to_json(bytes, &self.event_value, &self.registry)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use serde_reflection::{ContainerFormat, Samples, Tracer, TracerConfig};

    use super::*;

    fn trace_format<T>() -> (Format, Registry)
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let mut tracer = Tracer::new(
            TracerConfig::default()
                .record_samples_for_newtype_structs(true)
                .record_samples_for_tuple_structs(true),
        );
        let samples = Samples::new();
        let (format, _) = tracer.trace_type::<T>(&samples).unwrap();
        let registry = tracer.registry().unwrap();
        (format, registry)
    }

    #[test]
    fn primitive_round_trip() {
        let (format, registry) = trace_format::<u64>();
        let bytes = bcs::to_bytes(&42u64).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!(42));
    }

    #[test]
    fn struct_round_trip() {
        #[derive(Serialize, Deserialize)]
        struct Point {
            x: i32,
            y: i32,
        }

        let (format, registry) = trace_format::<Point>();
        let bytes = bcs::to_bytes(&Point { x: 10, y: -7 }).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!({ "x": 10, "y": -7 }));
    }

    #[test]
    fn enum_unit_and_struct_variants() {
        #[derive(Serialize, Deserialize)]
        enum Op {
            Increment,
            Set { value: u64 },
            Add(i64, i64),
        }

        let (format, registry) = trace_format::<Op>();

        let bytes = bcs::to_bytes(&Op::Increment).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!({ "Increment": null }));

        let bytes = bcs::to_bytes(&Op::Set { value: 99 }).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!({ "Set": { "value": 99 } }));

        let bytes = bcs::to_bytes(&Op::Add(2, 3)).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!({ "Add": [2, 3] }));
    }

    #[test]
    fn nested_with_option_and_seq() {
        #[derive(Serialize, Deserialize)]
        struct Outer {
            tag: String,
            items: Vec<u32>,
            note: Option<String>,
        }

        let (format, registry) = trace_format::<Outer>();
        let value = Outer {
            tag: "hello".to_string(),
            items: vec![1, 2, 3],
            note: None,
        };
        let bytes = bcs::to_bytes(&value).unwrap();
        let json_value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(
            json_value,
            json!({ "tag": "hello", "items": [1, 2, 3], "note": null })
        );
    }

    #[test]
    fn formats_decode_helpers() {
        #[derive(Serialize, Deserialize)]
        enum Operation {
            Ping,
            Echo(String),
        }
        #[derive(Serialize, Deserialize)]
        struct Response {
            ok: bool,
        }

        let (operation, op_registry) = trace_format::<Operation>();
        let (response, resp_registry) = trace_format::<Response>();

        let mut registry = op_registry;
        registry.extend(resp_registry);

        let (message, _) = trace_format::<()>();
        let (event_value, _) = trace_format::<()>();

        let formats = Formats {
            registry,
            operation,
            response,
            message,
            event_value,
        };

        let op_bytes = bcs::to_bytes(&Operation::Echo("hi".to_string())).unwrap();
        assert_eq!(
            formats.decode_operation(&op_bytes).unwrap(),
            json!({ "Echo": "hi" })
        );

        let resp_bytes = bcs::to_bytes(&Response { ok: true }).unwrap();
        assert_eq!(
            formats.decode_response(&resp_bytes).unwrap(),
            json!({ "ok": true })
        );

        let unit_bytes = bcs::to_bytes(&()).unwrap();
        assert_eq!(formats.decode_message(&unit_bytes).unwrap(), json!(null));
        assert_eq!(
            formats.decode_event_value(&unit_bytes).unwrap(),
            json!(null)
        );
    }

    #[test]
    fn malformed_bytes_return_error() {
        let (format, registry) = trace_format::<u64>();
        assert!(bcs_to_json(&[1, 2, 3], &format, &registry).is_err());
    }

    #[test]
    fn stable_enum_round_trip() {
        use linera_sdk_derive::StableEnumTraceInCrate;

        #[derive(
            Debug, PartialEq, StableEnumSerialize, StableEnumDeserialize, StableEnumTraceInCrate,
        )]
        enum Op {
            Increment,
            Set { value: u64 },
            Add(i64, i64),
            Echo(String),
        }

        // Wire format: each variant tag is exactly 4 ULEB128 bytes.
        for c in [
            Op::Increment,
            Op::Set { value: 99 },
            Op::Add(2, 3),
            Op::Echo("hi".into()),
        ] {
            let bytes = bcs::to_bytes(&c).unwrap();
            assert!(bytes.len() >= 4, "tag must be at least 4 bytes: {:?}", c);
            // 4-byte ULEB128: first 3 bytes have continuation bit, 4th doesn't.
            assert_eq!(bytes[0] & 0x80, 0x80, "byte 0 has continuation");
            assert_eq!(bytes[1] & 0x80, 0x80, "byte 1 has continuation");
            assert_eq!(bytes[2] & 0x80, 0x80, "byte 2 has continuation");
            assert_eq!(bytes[3] & 0x80, 0x00, "byte 3 terminates");

            let back: Op = bcs::from_bytes(&bytes).unwrap();
            assert_eq!(back, c);
        }

        // Unknown tags must be rejected.
        let bogus = bcs::to_bytes(&0u32).unwrap();
        assert!(bcs::from_bytes::<Op>(&bogus).is_err());

        // Tags published in the trait const match what BCS actually emits.
        for &(name, tag) in <Op as StableEnumTrace>::STABLE_VARIANTS {
            let sample = match name {
                "Increment" => bcs::to_bytes(&Op::Increment).unwrap(),
                "Set" => bcs::to_bytes(&Op::Set { value: 0 }).unwrap(),
                "Add" => bcs::to_bytes(&Op::Add(0, 0)).unwrap(),
                "Echo" => bcs::to_bytes(&Op::Echo(String::new())).unwrap(),
                _ => unreachable!(),
            };
            let decoded = decode_uleb_u32(&sample[..4]);
            assert_eq!(decoded, tag, "variant {name} tag mismatch");
        }

        // Tracing via the extension trait records the Keccak tags in the registry.
        let mut tracer = Tracer::new(TracerConfig::default());
        let samples = Samples::new();
        let format = tracer.trace_stable_enum_type::<Op>(&samples).unwrap();
        let registry = tracer.registry().unwrap();
        match registry.get("Op").unwrap() {
            ContainerFormat::Enum(variants) => {
                let mut keys: Vec<_> = variants.keys().copied().collect();
                keys.sort();
                let mut expected: Vec<u32> = <Op as StableEnumTrace>::STABLE_VARIANTS
                    .iter()
                    .map(|(_, t)| *t)
                    .collect();
                expected.sort();
                assert_eq!(keys, expected);
            }
            _ => panic!("expected enum"),
        }

        // End-to-end: bcs_to_json works against the reflected registry.
        let bytes = bcs::to_bytes(&Op::Set { value: 99 }).unwrap();
        let value = bcs_to_json(&bytes, &format, &registry).unwrap();
        assert_eq!(value, json!({ "Set": { "value": 99 } }));
    }

    /// Decode a 4-byte (exactly) ULEB128 sequence into a `u32`.
    fn decode_uleb_u32(bytes: &[u8]) -> u32 {
        let b0 = (bytes[0] & 0x7f) as u32;
        let b1 = (bytes[1] & 0x7f) as u32;
        let b2 = (bytes[2] & 0x7f) as u32;
        let b3 = bytes[3] as u32;
        b0 | (b1 << 7) | (b2 << 14) | (b3 << 21)
    }
}
