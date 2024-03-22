// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstractions over different Wasm runtime implementations.

use std::ops::{Deref, DerefMut};

use frunk::HList;

use super::{memory::Memory, RuntimeError};
use crate::memory_layout::FlatLayout;

/// A Wasm runtime.
///
/// Shared types between different guest instances that use the same runtime.
pub trait Runtime: Sized {
    /// A handle to something exported from a guest Wasm module.
    type Export;

    /// A handle to the guest Wasm module's memory.
    type Memory;
}

/// An active guest Wasm module.
pub trait Instance: Sized {
    /// The runtime this instance is running in.
    type Runtime: Runtime;

    /// Custom user data stored in the instance.
    type UserData;

    /// A reference to the custom user data stored in the instance.
    type UserDataReference<'a>: Deref<Target = Self::UserData>
    where
        Self::UserData: 'a,
        Self: 'a;

    /// A mutable reference to the custom user data stored in the instance.
    type UserDataMutReference<'a>: DerefMut<Target = Self::UserData>
    where
        Self::UserData: 'a,
        Self: 'a;

    /// Loads an export from the guest module.
    fn load_export(&mut self, name: &str) -> Option<<Self::Runtime as Runtime>::Export>;

    /// Returns a reference to the custom user data stored in this instance.
    fn user_data(&self) -> Self::UserDataReference<'_>;

    /// Returns a mutable reference to the custom user data stored in this instance.
    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_>;
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

/// Trait alias for a Wasm module instance with the WIT Canonical ABI `cabi_realloc` function.
pub trait CabiReallocAlias: InstanceWithFunction<HList![i32, i32, i32, i32], HList![i32]> {}

impl<AnyInstance> CabiReallocAlias for AnyInstance where
    AnyInstance: InstanceWithFunction<HList![i32, i32, i32, i32], HList![i32]>
{
}

/// Trait alias for a Wasm module instance with the WIT Canonical ABI `cabi_free` function.
pub trait CabiFreeAlias: InstanceWithFunction<HList![i32], HList![]> {}

impl<AnyInstance> CabiFreeAlias for AnyInstance where
    AnyInstance: InstanceWithFunction<HList![i32], HList![]>
{
}

/// Trait alias for a Wasm module instance with the WIT Canonical ABI functions.
pub trait InstanceWithMemory: CabiReallocAlias + CabiFreeAlias {
    /// Converts an `export` into the runtime's specific memory type.
    fn memory_from_export(
        &self,
        export: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<<Self::Runtime as Runtime>::Memory>, RuntimeError>;

    /// Returns the memory export from the current Wasm module instance.
    fn memory(&mut self) -> Result<Memory<'_, Self>, RuntimeError> {
        let export = self
            .load_export("memory")
            .ok_or(RuntimeError::MissingMemory)?;

        let memory = self
            .memory_from_export(export)?
            .ok_or(RuntimeError::NotMemory)?;

        Ok(Memory::new(self, memory))
    }
}
