// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{BlobContent, OracleResponse, Round},
    hashed::Hashed,
    identifiers::{AccountOwner, BlobType, ChainDescription, Destination, GenericApplicationId},
    ownership::ChainOwnership,
};
use linera_chain::{
    data_types::{Medium, MessageAction},
    manager::{ChainManagerInfo, LockedBlock},
    types::{Certificate, CertificateKind, ConfirmedBlock, Timeout, ValidatedBlock},
};
use linera_core::{data_types::CrossChainRequest, node::NodeError};
use linera_execution::{
    system::{AdminOperation, Recipient, SystemChannel, SystemMessage, SystemOperation},
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
    let samples = Samples::new();
    // 1. Record samples for types with custom deserializers.
    // 2. Trace the main entry point(s) + every enum separately.
    tracer.trace_type::<Round>(&samples)?;
    tracer.trace_type::<OracleResponse>(&samples)?;
    tracer.trace_type::<Recipient>(&samples)?;
    tracer.trace_type::<SystemChannel>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<AdminOperation>(&samples)?;
    tracer.trace_type::<SystemMessage>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<Message>(&samples)?;
    tracer.trace_type::<MessageAction>(&samples)?;
    tracer.trace_type::<MessageKind>(&samples)?;
    tracer.trace_type::<CertificateKind>(&samples)?;
    tracer.trace_type::<Certificate>(&samples)?;
    tracer.trace_type::<Hashed<ConfirmedBlock>>(&samples)?;
    tracer.trace_type::<ConfirmedBlock>(&samples)?;
    tracer.trace_type::<ValidatedBlock>(&samples)?;
    tracer.trace_type::<Timeout>(&samples)?;
    tracer.trace_type::<Medium>(&samples)?;
    tracer.trace_type::<Destination>(&samples)?;
    tracer.trace_type::<ChainDescription>(&samples)?;
    tracer.trace_type::<ChainOwnership>(&samples)?;
    tracer.trace_type::<GenericApplicationId>(&samples)?;
    tracer.trace_type::<LockedBlock>(&samples)?;
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
