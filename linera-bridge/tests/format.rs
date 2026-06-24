// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::{OracleResponse, Round},
    identifiers::{AccountOwner, BlobType, GenericApplicationId},
    vm::VmRuntime,
};
use linera_chain::{
    data_types::{MessageAction, Transaction, VoteValue},
    types::{CertificateKind, ConfirmedBlockCertificate},
};
use linera_execution::{
    system::AdminOperation, Message, MessageKind, Operation, SystemMessage, SystemOperation,
};
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let mut samples = Samples::new();
    // Record samples for types with custom deserializers.
    {
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
    }
    // Trace enums that appear in the ConfirmedBlockCertificate type graph.
    tracer.trace_type::<AccountOwner>(&samples)?;
    tracer.trace_type::<BlobType>(&samples)?;
    tracer.trace_type::<GenericApplicationId>(&samples)?;
    tracer.trace_type::<Message>(&samples)?;
    tracer.trace_type::<MessageAction>(&samples)?;
    tracer.trace_type::<MessageKind>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<OracleResponse>(&samples)?;
    tracer.trace_type::<Round>(&samples)?;
    tracer.trace_type::<SystemMessage>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<AdminOperation>(&samples)?;
    tracer.trace_type::<VmRuntime>(&samples)?;
    tracer.trace_type::<CertificateKind>(&samples)?;
    tracer.trace_type::<Transaction>(&samples)?;
    tracer.trace_type::<VoteValue>(&samples)?;
    tracer.trace_type::<ConfirmedBlockCertificate>(&samples)?;
    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format.yaml", get_registry().unwrap());
}
