// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Needed for arg_enum!
#![allow(clippy::useless_vec)]

use linera_base::{
    data_types::Round,
    identifiers::{ChainDescription, Destination},
};
use linera_chain::{
    data_types::{CertificateValue, HashedValue, Medium, MessageAction},
    ChainManagerInfo,
};
use linera_core::{data_types::CrossChainRequest, node::NodeError};
use linera_execution::{
    system::{AdminOperation, Recipient, SystemChannel, SystemMessage, SystemOperation},
    ChainOwnership, GenericApplicationId, Message, MessageKind, Operation,
};
use linera_rpc::RpcMessage;
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};
use std::{fs::File, io::Write};

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
    tracer.trace_type::<Recipient>(&samples)?;
    tracer.trace_type::<SystemChannel>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<AdminOperation>(&samples)?;
    tracer.trace_type::<SystemMessage>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<Message>(&samples)?;
    tracer.trace_type::<MessageAction>(&samples)?;
    tracer.trace_type::<MessageKind>(&samples)?;
    tracer.trace_type::<HashedValue>(&samples)?;
    tracer.trace_type::<CertificateValue>(&samples)?;
    tracer.trace_type::<Medium>(&samples)?;
    tracer.trace_type::<Destination>(&samples)?;
    tracer.trace_type::<ChainDescription>(&samples)?;
    tracer.trace_type::<ChainOwnership>(&samples)?;
    tracer.trace_type::<GenericApplicationId>(&samples)?;
    tracer.trace_type::<ChainManagerInfo>(&samples)?;
    tracer.trace_type::<CrossChainRequest>(&samples)?;
    tracer.trace_type::<NodeError>(&samples)?;
    tracer.trace_type::<RpcMessage>(&samples)?;
    tracer.registry()
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum Action {
    Print,
    Test,
    Record,
}

#[derive(clap::Parser, Debug)]
#[command(
    name = "Format generator",
    about = "Trace serde (de)serialization to generate format descriptions",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct Options {
    #[arg(value_enum, default_value_t = Action::Print, ignore_case = true)]
    action: Action,
}

const FILE_PATH: &str = "linera-rpc/tests/staged/formats.yaml";

fn main() {
    let options = <Options as clap::Parser>::parse();
    let registry = get_registry().unwrap();
    match options.action {
        Action::Print => {
            let content = serde_yaml::to_string(&registry).unwrap();
            println!("{}", content);
        }
        Action::Record => {
            let content = serde_yaml::to_string(&registry).unwrap();
            let mut f = File::create(FILE_PATH).unwrap();
            writeln!(f, "{}", content).unwrap();
        }
        Action::Test => {
            let reference = std::fs::read_to_string(FILE_PATH).unwrap();
            let content = serde_yaml::to_string(&registry).unwrap() + "\n";
            similar_asserts::assert_eq!(&reference, &content);
        }
    }
}
