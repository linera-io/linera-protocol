// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The BCS formats of the types that storage keys and values are encoded with.
//!
//! `linera-rpc` does the same for the wire surface. Together with
//! [`linera_storage::format::StorageFormat`], which says *where* each type appears, this registry
//! is what a decoder needs to turn a raw key-value pair into JSON.

use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::{BlockHeight, NetworkDescription, OracleResponse, Round},
    identifiers::{AccountOwner, BlobType, GenericApplicationId},
    vm::VmRuntime,
};
use linera_chain::{
    data_types::{MessageAction, Transaction},
    types::{CertificateKind, ConfirmedBlock, LiteCertificate},
};
use linera_execution::{
    system::{AdminOperation, SystemMessage, SystemOperation},
    BlobOrigin, BlobState, Message, MessageKind, Operation,
};
use linera_storage::{format::StorageFormat, EntryKey, RestrictedEventId, RootKey};
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let mut samples = Samples::new();

    // Signature types have custom deserializers that reject the values serde-reflection would
    // invent, so record real ones first. Same treatment as in `linera-rpc/tests/format.rs`.
    let validator_keypair = linera_base::crypto::ValidatorKeypair::generate();
    let validator_signature = linera_base::crypto::ValidatorSignature::new(
        &TestString::new("signature".to_string()),
        &validator_keypair.secret_key,
    );
    tracer.trace_value(&mut samples, &validator_keypair.public_key)?;
    tracer.trace_value(&mut samples, &validator_signature)?;

    let evm_secret_key = linera_base::crypto::EvmSecretKey::generate();
    let evm_public_key = evm_secret_key.public();
    tracer.trace_value(&mut samples, &evm_public_key)?;
    let evm_signature = linera_base::crypto::EvmSignature::new(
        CryptoHash::new(&TestString::new("signature".to_string())),
        &evm_secret_key,
    );
    tracer.trace_value(&mut samples, &evm_signature)?;

    // The keys.
    tracer.trace_type::<RootKey>(&samples)?;
    tracer.trace_type::<EntryKey>(&samples)?;
    tracer.trace_type::<RestrictedEventId>(&samples)?;
    tracer.trace_type::<BlockHeight>(&samples)?;
    tracer.trace_type::<CryptoHash>(&samples)?;

    // Enums must be traced individually, or serde-reflection only discovers the variants that
    // happen to be reachable from the values above.
    tracer.trace_type::<AccountOwner>(&samples)?;
    tracer.trace_type::<AdminOperation>(&samples)?;
    tracer.trace_type::<BlobOrigin>(&samples)?;
    tracer.trace_type::<BlobType>(&samples)?;
    tracer.trace_type::<CertificateKind>(&samples)?;
    tracer.trace_type::<GenericApplicationId>(&samples)?;
    tracer.trace_type::<Message>(&samples)?;
    tracer.trace_type::<MessageAction>(&samples)?;
    tracer.trace_type::<MessageKind>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<OracleResponse>(&samples)?;
    tracer.trace_type::<Round>(&samples)?;
    tracer.trace_type::<SystemMessage>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<Transaction>(&samples)?;
    tracer.trace_type::<VmRuntime>(&samples)?;

    // The values.
    tracer.trace_type::<NetworkDescription>(&samples)?;
    tracer.trace_type::<LiteCertificate>(&samples)?;
    tracer.trace_type::<ConfirmedBlock>(&samples)?;
    tracer.trace_type::<BlobState>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("storage_types.yaml", get_registry().unwrap());
}

/// Every type the layout description names must be one this registry defines. Without this the
/// names in `StorageFormat` are just strings, and nothing catches a rename or a stale entry.
#[test]
fn schema_type_names_resolve() {
    let registry = get_registry().unwrap();
    for name in StorageFormat::current().serialized_type_names() {
        assert!(
            registry.contains_key(name),
            "the layout description names `{name}`, which the storage type registry does not \
             define; either the name is wrong or the type needs tracing in this file"
        );
    }
}
