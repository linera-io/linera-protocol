// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Primitive WebAssembly and WIT types.

mod flat_type;
mod maybe_flat_type;
mod simple_type;

pub use self::{flat_type::FlatType, maybe_flat_type::MaybeFlatType, simple_type::SimpleType};
