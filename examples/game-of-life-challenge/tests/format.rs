// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI format test for the game-of-life-challenge application.

#![cfg(not(target_arch = "wasm32"))]

use gol_challenge::formats::GameOfLifeApplication;
use linera_sdk::formats::BcsApplication;

#[test]
fn test_format() {
    let mut formats = GameOfLifeApplication::formats().unwrap();
    formats.prune_known_primitives().unwrap();
    insta::assert_yaml_snapshot!("format", formats);
}
