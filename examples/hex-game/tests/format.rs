// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the hex-game application.

#![cfg(not(target_arch = "wasm32"))]

use hex_game::{Board, Cell, Clock, HexOutcome, Operation, Player, Timeouts};
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
    tracer.trace_type::<HexOutcome>(&samples)?;

    // Supporting types
    tracer.trace_type::<Timeouts>(&samples)?;
    tracer.trace_type::<Clock>(&samples)?;
    tracer.trace_type::<Board>(&samples)?;
    tracer.trace_type::<Cell>(&samples)?;
    tracer.trace_type::<Player>(&samples)?;
    tracer.trace_type::<AccountOwner>(&samples)?;

    tracer.registry()
}

#[test]
fn test_format() {
    insta::assert_yaml_snapshot!("format", get_registry().unwrap());
}
