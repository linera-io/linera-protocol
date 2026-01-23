// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the call-evm-counter application.

#![cfg(not(target_arch = "wasm32"))]

use call_evm_counter::{CallCounterOperation, CallCounterRequest};
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let samples = Samples::new();

    // ContractAbi types
    tracer.trace_type::<CallCounterOperation>(&samples)?;
    // Response is u64 - primitive

    // ServiceAbi types
    tracer.trace_type::<CallCounterRequest>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", get_registry().unwrap());
}
