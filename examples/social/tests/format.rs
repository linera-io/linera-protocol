// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the social application.

#![cfg(not(target_arch = "wasm32"))]

use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};
use social::{Comment, Key, Message, Operation, OwnPost, Post};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let samples = Samples::new();

    // ContractAbi types
    tracer.trace_type::<Operation>(&samples)?;
    // Response is () - skipped

    // Contract types
    tracer.trace_type::<Message>(&samples)?;
    // Parameters is () - skipped
    // InstantiationArgument is () - skipped
    // EventValue is () - skipped

    // Supporting types
    tracer.trace_type::<Key>(&samples)?;
    tracer.trace_type::<OwnPost>(&samples)?;
    tracer.trace_type::<Post>(&samples)?;
    tracer.trace_type::<Comment>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", get_registry().unwrap());
}
