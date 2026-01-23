// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the gen-nft application.

#![cfg(not(target_arch = "wasm32"))]

use fungible::Account;
use gen_nft::{Message, Nft, NftOutput, Operation, TokenId};
use linera_sdk::linera_base_types::AccountOwner;
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
    tracer.trace_type::<TokenId>(&samples)?;
    tracer.trace_type::<Nft>(&samples)?;
    tracer.trace_type::<NftOutput>(&samples)?;
    tracer.trace_type::<Account>(&samples)?;
    tracer.trace_type::<AccountOwner>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", get_registry().unwrap());
}
