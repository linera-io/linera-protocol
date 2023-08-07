// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Memory layout of non-fundamental WIT types.
//!
//! Complex WIT types are stored in memory as a sequence of fundamental types. The [`Layout`] type
//! allows representing the memory layout as a type, a heterogeneous list ([`frunk::hlist::HList`])
//! of fundamental types.

mod element;
mod flat_layout;
mod join_flat_layouts;
mod layout;

pub use self::{flat_layout::FlatLayout, join_flat_layouts::JoinFlatLayouts, layout::Layout};
