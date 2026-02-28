// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared types and helpers for linera-views benchmarks.

use serde::{Deserialize, Serialize};

/// Index type with non-trivial serialization cost, used by collection benchmarks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ComplexIndex {
    UselessVariant,
    NestedVariant(Box<ComplexIndex>),
    Leaf(String),
}
