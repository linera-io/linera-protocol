// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of Wasm instance related traits to mutable borrows of instances.
//!
//! This allows using the same traits without having to move the type implementation around, for
//! example as parameters in reentrant functions.

use std::borrow::Cow;

use super::{
    traits::{CabiFreeAlias, CabiReallocAlias},
    Instance, InstanceWithFunction, InstanceWithMemory, Runtime, RuntimeError, RuntimeMemory,
};
use crate::{memory_layout::FlatLayout, GuestPointer};

impl<I> Instance for &mut I
where
    I: Instance,
{
    type Runtime = I::Runtime;
    type UserData = I::UserData;
    type UserDataReference<'a> = I::UserDataReference<'a>
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a> = I::UserDataMutReference<'a>
    where
        Self::UserData: 'a,
        Self: 'a;

    fn load_export(&mut self, name: &str) -> Option<<Self::Runtime as Runtime>::Export> {
        I::load_export(*self, name)
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        I::user_data(*self)
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        I::user_data_mut(*self)
    }
}

impl<Parameters, Results, I> InstanceWithFunction<Parameters, Results> for &mut I
where
    I: InstanceWithFunction<Parameters, Results>,
    Parameters: FlatLayout,
    Results: FlatLayout,
{
    type Function = I::Function;

    fn function_from_export(
        &mut self,
        export: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError> {
        I::function_from_export(*self, export)
    }

    fn call(
        &mut self,
        function: &Self::Function,
        parameters: Parameters,
    ) -> Result<Results, RuntimeError> {
        I::call(*self, function, parameters)
    }
}

impl<'a, I> InstanceWithMemory for &'a mut I
where
    I: InstanceWithMemory,
    &'a mut I: Instance<Runtime = I::Runtime> + CabiReallocAlias + CabiFreeAlias,
{
    fn memory_from_export(
        &self,
        export: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<<Self::Runtime as Runtime>::Memory>, RuntimeError> {
        I::memory_from_export(&**self, export)
    }
}

impl<M, I> RuntimeMemory<&mut I> for M
where
    M: RuntimeMemory<I>,
{
    /// Reads `length` bytes from memory from the provided `location`.
    fn read<'instance>(
        &self,
        instance: &'instance &mut I,
        location: GuestPointer,
        length: u32,
    ) -> Result<Cow<'instance, [u8]>, RuntimeError> {
        self.read(&**instance, location, length)
    }

    /// Writes the `bytes` to memory at the provided `location`.
    fn write(
        &mut self,
        instance: &mut &mut I,
        location: GuestPointer,
        bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        self.write(&mut **instance, location, bytes)
    }
}
