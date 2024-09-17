// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains a [`StubInstance`] type and the code it requires to implement the necessary
//! trait to be used as a Wasm instance type during compile time.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    memory_layout::FlatLayout, GuestPointer, Instance, InstanceWithFunction, InstanceWithMemory,
    Runtime, RuntimeError, RuntimeMemory,
};

/// A stub Wasm instance.
///
/// This is when a Wasm instance type is needed for type-checking but is not needed during
/// runtime.
pub struct StubInstance<UserData = ()> {
    _user_data: PhantomData<UserData>,
}

impl<UserData> Default for StubInstance<UserData> {
    fn default() -> Self {
        StubInstance {
            _user_data: PhantomData,
        }
    }
}

impl<UserData> Instance for StubInstance<UserData> {
    type Runtime = StubRuntime;
    type UserData = UserData;
    type UserDataReference<'a> = &'a UserData
    where
        Self::UserData: 'a,
        Self: 'a;
    type UserDataMutReference<'a> = &'a mut UserData
    where
        Self::UserData: 'a,
        Self: 'a;

    fn load_export(&mut self, _name: &str) -> Option<()> {
        unimplemented!("`StubInstance` can not be used as a real `Instance`");
    }

    fn user_data(&self) -> Self::UserDataReference<'_> {
        unimplemented!("`StubInstance` can not be used as a real `Instance`");
    }

    fn user_data_mut(&mut self) -> Self::UserDataMutReference<'_> {
        unimplemented!("`StubInstance` can not be used as a real `Instance`");
    }
}

impl<Parameters, Results, UserData> InstanceWithFunction<Parameters, Results>
    for StubInstance<UserData>
where
    Parameters: FlatLayout + 'static,
    Results: FlatLayout + 'static,
{
    type Function = ();

    fn function_from_export(
        &mut self,
        _name: <Self::Runtime as Runtime>::Export,
    ) -> Result<Option<Self::Function>, RuntimeError> {
        unimplemented!("`StubInstance` can not be used as a real `InstanceWithFunction`");
    }

    fn call(
        &mut self,
        _function: &Self::Function,
        _parameters: Parameters,
    ) -> Result<Results, RuntimeError> {
        unimplemented!("`StubInstance` can not be used as a real `InstanceWithFunction`");
    }
}

impl<UserData> InstanceWithMemory for StubInstance<UserData> {
    fn memory_from_export(&self, _export: ()) -> Result<Option<StubMemory>, RuntimeError> {
        unimplemented!("`StubInstance` can not be used as a real `InstanceWithMemory`");
    }
}

/// A stub Wasm runtime.
pub struct StubRuntime;

impl Runtime for StubRuntime {
    type Export = ();
    type Memory = StubMemory;
}

/// A stub Wasm runtime memory.
pub struct StubMemory;

impl<UserData> RuntimeMemory<StubInstance<UserData>> for StubMemory {
    fn read<'instance>(
        &self,
        _instance: &'instance StubInstance<UserData>,
        _location: GuestPointer,
        _length: u32,
    ) -> Result<Cow<'instance, [u8]>, RuntimeError> {
        unimplemented!("`StubMemory` can not be used as a real `RuntimeMemory`");
    }

    fn write(
        &mut self,
        _instance: &mut StubInstance<UserData>,
        _location: GuestPointer,
        _bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        unimplemented!("`StubMemory` can not be used as a real `RuntimeMemory`");
    }
}
