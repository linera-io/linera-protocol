// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::ChainDescription;
use linera_chain::{
    data_types::{HashedValue, Medium, ValueKind},
    ChainManagerInfo,
};
use linera_core::{data_types::CrossChainRequest, node::NodeError};
use linera_execution::{
    system::{Recipient, SystemChannel, SystemEffect, SystemOperation},
    ApplicationDescription, ApplicationId, Destination, Effect, Operation,
};
use linera_rpc::RpcMessage;
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};
use std::{fs::File, io::Write};
use structopt::{clap::arg_enum, StructOpt};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let samples = Samples::new();
    // 1. Record samples for types with custom deserializers.
    // 2. Trace the main entry point(s) + every enum separately.
    tracer.trace_type::<Recipient>(&samples)?;
    tracer.trace_type::<SystemChannel>(&samples)?;
    tracer.trace_type::<SystemOperation>(&samples)?;
    tracer.trace_type::<SystemEffect>(&samples)?;
    tracer.trace_type::<Operation>(&samples)?;
    tracer.trace_type::<Effect>(&samples)?;
    tracer.trace_type::<HashedValue>(&samples)?;
    tracer.trace_type::<ValueKind>(&samples)?;
    tracer.trace_type::<Medium>(&samples)?;
    tracer.trace_type::<Destination>(&samples)?;
    tracer.trace_type::<ChainDescription>(&samples)?;
    tracer.trace_type::<ApplicationId>(&samples)?;
    tracer.trace_type::<ApplicationDescription>(&samples)?;
    tracer.trace_type::<ChainManagerInfo>(&samples)?;
    tracer.trace_type::<CrossChainRequest>(&samples)?;
    tracer.trace_type::<NodeError>(&samples)?;
    tracer.trace_type::<RpcMessage>(&samples)?;
    tracer.registry()
}

arg_enum! {
#[derive(Debug, StructOpt, Clone, Copy)]
enum Action {
    Print,
    Test,
    Record,
}
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Format generator",
    about = "Trace serde (de)serialization to generate format descriptions"
)]
struct Options {
    #[structopt(possible_values = &Action::variants(), default_value = "Print", case_insensitive = true)]
    action: Action,
}

const FILE_PATH: &str = "linera-rpc/tests/staged/formats.yaml";

fn main() {
    let options = Options::from_args();
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
