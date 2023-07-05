// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primitive WebAssembly and WIT types.

mod flat_type;
mod simple_type;

pub use self::{flat_type::FlatType, simple_type::SimpleType};
