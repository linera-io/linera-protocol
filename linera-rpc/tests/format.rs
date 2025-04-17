// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, TestString},
    data_types::{BlobContent, OracleResponse, Round},
    identifiers::{AccountOwner, BlobType, ChainDescription, GenericApplicationId},
    ownership::ChainOwnership,
    vm::VmRuntime,
};
use linera_chain::{
    data_types::MessageAction,
    manager::{ChainManagerInfo, LockingBlock},
    types::{Certificate, CertificateKind, ConfirmedBlock, Timeout, ValidatedBlock},
};
use linera_core::{data_types::CrossChainRequest, node::NodeError};
use linera_execution::{
    system::{AdminOperation, Recipient, SystemMessage, SystemOperation},
    Message, MessageKind, Operation,
};
use linera_rpc::RpcMessage;
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let mut samples = Samples::new();
    // 1. Record samples for types with custom deserializers.
    {
        // Record sample values for Secp256k1PublicKey and Secp256k1Signature
        // as the ones generated by serde-reflection are not valid and will fail.
        let validator_keypair = linera_base::crypto::ValidatorKeypair::generate();
        let validator_signature = linera_base::crypto::ValidatorSignature::new(
            &TestString::new("signature".to_string()),
            &validator_keypair.secret_key,
        );
        tracer.trace_value(&mut samples, &validator_keypair.public_key)?;
        tracer.trace_value(&mut samples, &validator_signature)?;

        // We also record separate samples for EVM-compatible keys,
        // as the generated ones are not valid.
        let evm_secret_key = linera_base::crypto::EvmSecretKey::generate();
        let evm_public_key = evm_secret_key.public();
        tracer.trace_value(&mut samples, &evm_public_key)?;
        let evm_signature = linera_base::crypto::EvmSignature::new(
            &TestString::new("signature".to_string()),
            &evm_secret_key,
        );
        tracer.trace_value(&mut samples, &evm_signature)?;
    }
    // 2. Trace the main entry point(s) + every enum separately.
    tracer.trace_type::<AccountPublicKey>(&samples)?;
    tracer.trace_type::<AccountSignature>(&samples)?;
    tracer.trace_type::<Round>(&samples)?;
    tracer.trace_type::<OracleResponse>(&samples)?;
    tracer.trace_type::<Recipient>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<AdminOperation>(&samples)?;
    tracer.trace_type::<SystemMessage>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<Message>(&samples)?;
    tracer.trace_type::<VmRuntime>(&samples)?;
    tracer.trace_type::<MessageAction>(&samples)?;
    tracer.trace_type::<MessageKind>(&samples)?;
    tracer.trace_type::<CertificateKind>(&samples)?;
    tracer.trace_type::<Certificate>(&samples)?;
    tracer.trace_type::<ConfirmedBlock>(&samples)?;
    tracer.trace_type::<ValidatedBlock>(&samples)?;
    tracer.trace_type::<Timeout>(&samples)?;
    tracer.trace_type::<ChainDescription>(&samples)?;
    tracer.trace_type::<ChainOwnership>(&samples)?;
    tracer.trace_type::<GenericApplicationId>(&samples)?;
    tracer.trace_type::<LockingBlock>(&samples)?;
    tracer.trace_type::<ChainManagerInfo>(&samples)?;
    tracer.trace_type::<CrossChainRequest>(&samples)?;
    tracer.trace_type::<NodeError>(&samples)?;
    tracer.trace_type::<RpcMessage>(&samples)?;
    tracer.trace_type::<BlobType>(&samples)?;
    tracer.trace_type::<BlobContent>(&samples)?;
    tracer.trace_type::<AccountOwner>(&samples)?;
    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format.yaml", get_registry().unwrap());
}
