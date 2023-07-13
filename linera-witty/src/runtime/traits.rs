// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstractions over different Wasm runtime implementations.

use super::RuntimeError;
use crate::memory_layout::FlatLayout;

/// A Wasm runtime.
///
/// Shared types between different guest instances that use the same runtime.
pub trait Runtime: Sized {
    /// A handle to something exported from a guest Wasm module.
    type Export;
}

/// An active guest Wasm module.
pub trait Instance: Sized {
    /// The runtime this instance is running in.
    type Runtime: Runtime;

    /// Loads an export from the guest module.
    fn load_export(&mut self, name: &str) -> Option<<Self::Runtime as Runtime>::Export>;
}

/// How a runtime supports a function signature.
pub trait InstanceWithFunction<Parameters, Results>: Instance
where
    Parameters: FlatLayout,
    Results: FlatLayout,
{
    /// The runtime-specific type to represent the function.
    type Function;

    /// Converts an export into a function, if it is one.
    fn function_from_export(
        &mut self,
        export: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError>;

    /// Calls the `function` from this instance using the specified `parameters`.
    fn call(
        &mut self,
        function: &Self::Function,
        parameters: Parameters,
    ) -> Result<Results, RuntimeError>;

    /// Loads a function from the guest Wasm instance.
    fn load_function(&mut self, name: &str) -> Result<Self::Function, RuntimeError> {
        let export = self
            .load_export(name)
            .ok_or_else(|| RuntimeError::FunctionNotFound(name.to_string()))?;

        self.function_from_export(export)?
            .ok_or_else(|| RuntimeError::NotAFunction(name.to_string()))
    }
}
