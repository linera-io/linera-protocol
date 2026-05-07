// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support the declaration of the binary formats used by an application.

use serde::{Deserialize, Serialize};
use serde_reflection::{
    json_converter::{DeserializationContext, EmptyEnvironment},
    Format, Registry,
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
    use serde_reflection::{Samples, Tracer, TracerConfig};

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

        // Combine the two registries so the same `Formats` can decode both types.
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

        // Empty BCS for the `()` unit type.
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
        // u64 needs 8 bytes; only provide 3.
        assert!(bcs_to_json(&[1, 2, 3], &format, &registry).is_err());
    }
}
