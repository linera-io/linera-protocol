// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the rfq application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::linera_base_types::AccountOwner;
use rfq::{Message, Operation, RequestId, TokenPair, Tokens};
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};

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

    // Supporting types
    tracer.trace_type::<TokenPair>(&samples)?;
    tracer.trace_type::<RequestId>(&samples)?;
    tracer.trace_type::<Tokens>(&samples)?;
    tracer.trace_type::<AccountOwner>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", get_registry().unwrap());
}
