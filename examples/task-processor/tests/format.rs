// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the task-processor application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::formats::BcsApplication;
use task_processor::formats::TaskProcessorApplication;

#[test]
fn test_format() {
    let mut formats = TaskProcessorApplication::formats().unwrap();
    formats.prune_known_primitives().unwrap();
    insta::assert_yaml_snapshot!("format", formats);
}
