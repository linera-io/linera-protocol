// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fungible::{FungibleOperation, Message};
use linera_base::identifiers::AccountOwner;
use serde_reflection::{Samples, Tracer, TracerConfig};

#[test]
fn test_format_fungible() {
    let mut tracer = Tracer::new(
        TracerConfig::default()
            .record_samples_for_newtype_structs(true)
            .record_samples_for_tuple_structs(true),
    );
    let samples = Samples::new();
    // Trace AccountOwner explicitly so all variants get proper indices.
    tracer.trace_type::<AccountOwner>(&samples).unwrap();
    tracer.trace_type::<FungibleOperation>(&samples).unwrap();
    tracer.trace_type::<Message>(&samples).unwrap();
    let registry = tracer.registry_unchecked();
    insta::assert_yaml_snapshot!("format_fungible.yaml", registry);
}
