// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper traits for exporting host functions to guest Wasm instances.
//!
//! These help determining the function signature the guest expects based on the host function
//! signature.

mod result_storage;

use self::result_storage::ResultStorage;
use crate::RuntimeError;

/// A type that can register some functions as exports for the target `Instance`.
pub trait ExportTo<Instance> {
    /// Registers some host functions as exports to the provided guest Wasm `instance`.
    fn export_to(instance: &mut Instance) -> Result<(), RuntimeError>;
}

/// A type that accepts registering a host function as an export for a guest Wasm instance.
///
/// The `Handler` represents the closure type required for the host function, and `Parameters` and
/// `Results` are the input and output types of the closure, respectively.
pub trait ExportFunction<Handler, Parameters, Results> {
    /// Registers a host function executed by the `handler` with the provided `module_name` and
    /// `function_name` as an export for a guest Wasm instance.
    fn export(
        &mut self,
        module_name: &str,
        function_name: &str,
        handler: Handler,
    ) -> Result<(), RuntimeError>;
}
