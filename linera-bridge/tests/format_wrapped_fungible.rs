// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::AccountOwner;
use linera_sdk::formats::TracerExt;
use serde_reflection::{Samples, Tracer, TracerConfig};
use wrapped_fungible::{BurnEvent, Message, WrappedFungibleOperation};

#[test]
fn test_format_wrapped_fungible() {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let samples = Samples::new();
    // Trace AccountOwner explicitly so all variants get proper indices.
    tracer.trace_type::<AccountOwner>(&samples).unwrap();
    // `WrappedFungibleOperation` has `#[derive(StableEnum)]`; its variant tags
    // are non-contiguous Keccak-derived u32s, so `trace_type` cannot enumerate
    // them — drive tracing via the `StableEnumTrace` impl instead.
    tracer
        .trace_stable_enum_type::<WrappedFungibleOperation>(&samples)
        .unwrap();
    tracer.trace_type::<Message>(&samples).unwrap();
    tracer.trace_type::<BurnEvent>(&samples).unwrap();
    let registry = tracer.registry_unchecked();
    insta::assert_yaml_snapshot!("format_wrapped_fungible.yaml", registry);
}
