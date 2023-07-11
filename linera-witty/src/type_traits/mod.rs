// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Traits used to allow complex types to be sent and received between hosts and guests using WIT.

mod implementations;

use crate::memory_layout::Layout;

/// A type that is representable by fundamental WIT types.
pub trait WitType {
    /// The size of the type when laid out in memory.
    const SIZE: u32;

    /// The layout of the type as fundamental types.
    type Layout: Layout;
}
