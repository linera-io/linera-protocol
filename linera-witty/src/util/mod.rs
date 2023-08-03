// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types and functions that aren't specific to WIT or WebAssembly.

mod merge;
mod split;
mod zero_extend;

pub use self::{merge::Merge, split::Split, zero_extend::ZeroExtend};
