// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper traits for exporting host functions to guest Wasm instances.
//!
//! These help determining the function signature the guest expects based on the host function
//! signature.

use crate::RuntimeError;

/// A type that can register some functions as exports for the target `Instance`.
pub trait ExportTo<Instance> {
    /// Registers some host functions as exports to the provided guest Wasm `instance`.
    fn export_to(instance: &mut Instance) -> Result<(), RuntimeError>;
}
