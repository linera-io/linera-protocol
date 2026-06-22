// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Support the declaration of the binary formats used by an application.

use serde::{Deserialize, Serialize};
use serde_reflection::{
    json_converter::{
        DeserializationContext, DeserializationEnvironment, SerializationContext,
        SerializationEnvironment, SymbolTableEnvironment,
    },
    Format, Registry,
};
#[cfg(not(target_arch = "wasm32"))]
use serde_reflection::{Samples, Tracer, TracerConfig};

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

    /// Returns the serde formats for this application's ABI types. The returned
    /// registry is self-contained: it describes every type structurally, including
    /// the well-known linera-base primitives.
    fn formats() -> serde_reflection::Result<Formats>;

    /// Like [`formats`](Self::formats), but with the well-known linera-base primitives
    /// pruned from the registry (see [`Formats::prune_known_primitives`]) so that they
    /// decode to their human-readable form via [`LineraEnvironment`]. This is the form
    /// meant to be published to a formats registry.
    #[cfg(not(target_arch = "wasm32"))]
    fn pruned_formats() -> Result<Formats, PruneError> {
        let mut formats = Self::formats()?;
        formats.prune_known_primitives()?;
        Ok(formats)
    }
}

/// Decodes BCS-serialized `bytes` into a [`serde_json::Value`], guided by `format`
/// and the container `registry`.
///
/// Well-known linera-base primitives that are absent from `registry` (because they
/// were removed by [`Formats::prune_known_primitives`]) are decoded into their
/// human-readable representation by [`LineraEnvironment`].
fn bcs_to_json(
    bytes: &[u8],
    format: &Format,
    registry: &Registry,
) -> bcs::Result<serde_json::Value> {
    let context = DeserializationContext {
        format: format.clone(),
        registry,
        environment: &LineraEnvironment,
    };
    bcs::from_bytes_seed(context, bytes)
}

/// Encodes a [`serde_json::Value`] into BCS `bytes`, guided by `format` and the
/// container `registry`. This is the inverse of [`bcs_to_json`].
///
/// Well-known linera-base primitives that are absent from `registry` are read from
/// their human-readable representation by [`LineraEnvironment`].
fn json_to_bcs(
    value: &serde_json::Value,
    format: &Format,
    registry: &Registry,
) -> bcs::Result<Vec<u8>> {
    let context = SerializationContext {
        value,
        format,
        registry,
        environment: &LineraEnvironment,
    };
    bcs::to_bytes(&context)
}

/// Decodes the BCS form of a value as the concrete type `T`, then re-encodes it as
/// JSON using `T`'s human-readable serialization.
fn primitive_to_json<'de, T, D>(deserializer: D) -> Result<serde_json::Value, String>
where
    T: serde::Deserialize<'de> + serde::Serialize,
    D: serde::Deserializer<'de>,
{
    let value = T::deserialize(deserializer).map_err(|error| error.to_string())?;
    serde_json::to_value(&value).map_err(|error| error.to_string())
}

/// Reads a value's human-readable JSON representation into the concrete type `T`, then
/// serializes it with `serializer` (its BCS form). Inverse of [`primitive_to_json`].
fn primitive_from_json<T, S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    S: serde::Serializer,
{
    let value: T = T::deserialize(value).map_err(serde::ser::Error::custom)?;
    value.serialize(serializer)
}

/// Declares the linera-base primitives whose human-readable serde representation
/// differs from their BCS one. This single list drives both [`LineraEnvironment`]
/// (decoding) and the canonical-format check used by
/// [`Formats::prune_known_primitives`], so the two can never drift apart.
macro_rules! known_human_readable_primitives {
    ($($name:literal => $ty:ty),* $(,)?) => {
        /// The registry names of the linera-base primitives handled by
        /// [`LineraEnvironment`]. These are exactly the types whose human-readable
        /// serde representation differs from their BCS form *and* whose BCS form is a
        /// named container (so it can be matched by name in a traced registry).
        pub const KNOWN_PRIMITIVE_NAMES: &[&str] = &[$($name),*];

        /// A [`json_converter`](serde_reflection::json_converter) environment that
        /// decodes the BCS form of well-known linera-base primitives into their
        /// human-readable JSON representation (e.g. a `CryptoHash` as a hex string, an
        /// `Amount` as a decimal string, an `AccountOwner` as its canonical address).
        ///
        /// It only takes effect for names that are *absent* from the registry being
        /// decoded; see [`Formats::prune_known_primitives`].
        #[derive(Clone, Copy, Debug, Default)]
        pub struct LineraEnvironment;

        impl SymbolTableEnvironment for LineraEnvironment {}

        impl<'de> DeserializationEnvironment<'de> for LineraEnvironment {
            fn deserialize<D>(
                &self,
                name: String,
                deserializer: D,
            ) -> Result<serde_json::Value, String>
            where
                D: serde::Deserializer<'de>,
            {
                match name.as_str() {
                    $( $name => primitive_to_json::<$ty, D>(deserializer), )*
                    _ => Err(format!("No external definition available for {name}")),
                }
            }
        }

        impl SerializationEnvironment for LineraEnvironment {
            fn serialize<S>(
                &self,
                name: &str,
                value: &serde_json::Value,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                match name {
                    $( $name => primitive_from_json::<$ty, S>(value, serializer), )*
                    _ => Err(serde::ser::Error::custom(format!(
                        "No external serializer available for {name}"
                    ))),
                }
            }
        }

        /// Traces the canonical BCS format of every known primitive, used to verify
        /// the correspondence before pruning.
        #[cfg(not(target_arch = "wasm32"))]
        fn expected_primitive_registry() -> serde_reflection::Result<Registry> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();
            $( tracer.trace_type::<$ty>(&samples)?; )*
            // Supporting enums reached only through the primitives above; they must be
            // traced explicitly so all of their variants are recorded.
            tracer.trace_type::<crate::linera_base_types::VmRuntime>(&samples)?;
            tracer.trace_type::<crate::linera_base_types::BlobType>(&samples)?;
            tracer.trace_type::<crate::linera_base_types::GenericApplicationId>(&samples)?;
            tracer.registry()
        }
    };
}

// NOTE: The cryptographic key and signature types (`Ed25519PublicKey`,
// `Secp256k1PublicKey`, `EvmPublicKey`, and their `*Signature` counterparts) also have
// a customized human-readable serde representation, but their `Deserialize` validates
// the bytes (e.g. that a compressed point is on the curve), so `serde_reflection`
// cannot trace them from dummy bytes and we cannot verify their format before pruning.
// Supporting them would require feeding valid samples to the tracer; deferred for now.
// They also do not currently appear in any application's operation/message ABI.
known_human_readable_primitives! {
    "CryptoHash" => crate::linera_base_types::CryptoHash,
    "AccountOwner" => crate::linera_base_types::AccountOwner,
    "Amount" => crate::linera_base_types::Amount,
    "Epoch" => crate::linera_base_types::Epoch,
    "BlobId" => crate::linera_base_types::BlobId,
    "StreamId" => crate::linera_base_types::StreamId,
    "ModuleId" => crate::linera_base_types::ModuleId,
    "ApplicationId" => crate::linera_base_types::ApplicationId,
}

/// An error raised while pruning known primitives from a [`Formats`] registry.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum PruneError {
    /// The canonical formats of the known primitives could not be computed.
    #[error("failed to compute the canonical primitive formats: {0}")]
    Reflection(#[from] serde_reflection::Error),
    /// A known primitive name is present in the registry but with a format that does
    /// not match the canonical linera-base one (e.g. a name collision or a layout
    /// change). Nothing is pruned in that case.
    #[error(
        "registry entry for `{name}` does not match the canonical linera-base format; \
         refusing to prune"
    )]
    Mismatch {
        /// The name of the offending registry entry.
        name: String,
    },
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

    /// Encodes a JSON operation value into its BCS bytes. Inverse of
    /// [`decode_operation`](Self::decode_operation).
    pub fn encode_operation(&self, value: &serde_json::Value) -> bcs::Result<Vec<u8>> {
        json_to_bcs(value, &self.operation, &self.registry)
    }

    /// Encode a JSON operation response value into its BCS bytes. Inverse of
    /// [`decode_response`](Self::decode_response).
    pub fn encode_response(&self, value: &serde_json::Value) -> bcs::Result<Vec<u8>> {
        json_to_bcs(value, &self.response, &self.registry)
    }

    /// Encode a JSON message value into its BCS bytes. Inverse of
    /// [`decode_message`](Self::decode_message).
    pub fn encode_message(&self, value: &serde_json::Value) -> bcs::Result<Vec<u8>> {
        json_to_bcs(value, &self.message, &self.registry)
    }

    /// Encode a JSON event value into its BCS bytes. Inverse of
    /// [`decode_event_value`](Self::decode_event_value).
    pub fn encode_event_value(&self, value: &serde_json::Value) -> bcs::Result<Vec<u8>> {
        json_to_bcs(value, &self.event_value, &self.registry)
    }

    /// Removes the registry entries for the well-known linera-base primitives (see
    /// [`KNOWN_PRIMITIVE_NAMES`]) so that decoding falls back to [`LineraEnvironment`],
    /// which renders them in their human-readable form.
    ///
    /// Before removing anything, this verifies that each such entry actually matches
    /// the canonical BCS format of the corresponding linera-base type. If any entry is
    /// present with a different format — for instance because an application defined a
    /// distinct type with a colliding name — this returns [`PruneError::Mismatch`] and
    /// leaves the registry untouched.
    ///
    /// This is meant to be called from snapshot tests (and any other code that
    /// generates the stored [`Formats`]) so that the human-readable rendering is baked
    /// into the published formats.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prune_known_primitives(&mut self) -> Result<(), PruneError> {
        let expected = expected_primitive_registry()?;
        // Verify everything first so a mismatch never leaves the registry half-pruned.
        for name in KNOWN_PRIMITIVE_NAMES {
            let (Some(actual), Some(expected_format)) =
                (self.registry.get(*name), expected.get(*name))
            else {
                continue;
            };
            if actual != expected_format {
                return Err(PruneError::Mismatch {
                    name: (*name).to_string(),
                });
            }
        }
        for name in KNOWN_PRIMITIVE_NAMES {
            self.registry.remove(*name);
        }
        Ok(())
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

        // The encode helpers are the inverse of the decode helpers.
        assert_eq!(
            formats.encode_operation(&json!({ "Echo": "hi" })).unwrap(),
            op_bytes
        );
        assert_eq!(
            formats.encode_response(&json!({ "ok": true })).unwrap(),
            resp_bytes
        );
        assert_eq!(formats.encode_message(&json!(null)).unwrap(), unit_bytes);
        assert_eq!(
            formats.encode_event_value(&json!(null)).unwrap(),
            unit_bytes
        );
    }

    #[test]
    fn malformed_bytes_return_error() {
        let (format, registry) = trace_format::<u64>();
        // u64 needs 8 bytes; only provide 3.
        assert!(bcs_to_json(&[1, 2, 3], &format, &registry).is_err());
    }

    #[test]
    fn expected_registry_builds() {
        let registry = expected_primitive_registry().unwrap();
        for name in KNOWN_PRIMITIVE_NAMES {
            assert!(registry.contains_key(*name), "missing {name}");
        }
    }

    #[test]
    fn known_primitives_decode_as_human_readable() {
        use std::str::FromStr as _;

        use crate::linera_base_types::{AccountOwner, Amount, CryptoHash, ModuleId, VmRuntime};

        #[derive(Serialize, Deserialize)]
        struct Sample {
            owner: AccountOwner,
            amount: Amount,
            hash: CryptoHash,
            module: Option<ModuleId>,
        }

        let hash = CryptoHash::from_str(&"ab".repeat(32)).unwrap();
        let value = Sample {
            owner: AccountOwner::Address32(hash),
            amount: Amount::from_tokens(5),
            hash,
            module: Some(ModuleId::new(hash, hash, VmRuntime::Wasm)),
        };

        // Trace `Sample` plus the nested multi-variant enums, so every variant of
        // `AccountOwner` and `VmRuntime` is recorded (a single `trace_type` pass over a
        // struct only samples one variant per nested enum).
        let mut tracer = Tracer::new(
            TracerConfig::default()
                .record_samples_for_newtype_structs(true)
                .record_samples_for_tuple_structs(true),
        );
        let samples = Samples::new();
        let (operation, _) = tracer.trace_type::<Sample>(&samples).unwrap();
        tracer.trace_type::<AccountOwner>(&samples).unwrap();
        tracer.trace_type::<VmRuntime>(&samples).unwrap();
        let registry = tracer.registry().unwrap();

        let unit = Format::Unit;
        let mut formats = Formats {
            registry,
            operation,
            response: unit.clone(),
            message: unit.clone(),
            event_value: unit,
        };

        // Tracing this value embeds CryptoHash/AccountOwner/Amount/ModuleId structurally.
        let bytes = bcs::to_bytes(&value).unwrap();
        assert!(formats.registry.contains_key("CryptoHash"));

        // After pruning, those primitives are decoded by `LineraEnvironment` into the
        // exact human-readable JSON their own `Serialize` would produce.
        formats.prune_known_primitives().unwrap();
        assert!(!formats.registry.contains_key("CryptoHash"));
        assert!(!formats.registry.contains_key("AccountOwner"));

        let decoded = formats.decode_operation(&bytes).unwrap();
        let expected = serde_json::to_value(&value).unwrap();
        assert_eq!(decoded, expected);
        // Sanity-check the human-readable shape: a hex hash and a decimal amount string.
        assert_eq!(decoded["hash"], json!("ab".repeat(32)));
        assert_eq!(decoded["amount"], json!(value.amount.to_string()));

        // Encoding is the inverse of decoding: the human-readable JSON re-encodes to
        // exactly the original BCS bytes.
        let reencoded = formats.encode_operation(&decoded).unwrap();
        assert_eq!(reencoded, bytes);
    }

    #[test]
    fn prune_rejects_colliding_format() {
        use serde_reflection::ContainerFormat;

        // A registry where `CryptoHash` is (wrongly) bound to a different format.
        let mut registry = Registry::new();
        registry.insert(
            "CryptoHash".to_string(),
            ContainerFormat::NewTypeStruct(Box::new(Format::U64)),
        );
        let unit = Format::Unit;
        let mut formats = Formats {
            registry,
            operation: Format::TypeName("CryptoHash".to_string()),
            response: unit.clone(),
            message: unit.clone(),
            event_value: unit,
        };

        let error = formats.prune_known_primitives().unwrap_err();
        assert!(matches!(error, PruneError::Mismatch { name } if name == "CryptoHash"));
        // The registry is left untouched on mismatch.
        assert!(formats.registry.contains_key("CryptoHash"));
    }
}
