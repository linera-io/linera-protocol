// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of where a function's results should be stored.
//!
//! The [canonical ABI][flattening] describes a `MAX_FLAT_RESULTS` limit on the number of flat
//! types that are returned from a function. The limit is currently one. That means that any type
//! returned from a host function that needs more than one flat type to represent it after lowering
//! needs to be placed in memory.
//!
//! The [`ResultStorage`] trait below allows representing in compile time if the results should be
//! returned as a single flat value (scenario represented by the `()` unit type) or placed in
//! memory and not have any return value (scenario represented by a [`GuestPointer`] instance with
//! the address where the results should be stored).
//!
//! [flattening]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening

use frunk::HNil;

use crate::{
    memory_layout::FlatLayout, GuestPointer, InstanceWithMemory, Layout, Memory, Runtime,
    RuntimeError, RuntimeMemory, WitStore,
};

/// Representation of where a function's results should be stored.
///
/// See the module level documentation for details.
pub trait ResultStorage {
    /// A helper associated type that maps the flat layout of a `HostResults` type into the flat
    /// layout of the returned value.
    ///
    /// This is either an empty layout representing a function that has no return values or a
    /// layout with a single flat type representing a function that returns one flat value.
    type OutputFor<HostResults>: FlatLayout
    where
        HostResults: WitStore;

    /// Lowers the `HostResults` and prepares the output for the function, storing it in memory if
    /// needed.
    fn lower_result<HostResults, Instance>(
        self,
        result: HostResults,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::OutputFor<HostResults>, RuntimeError>
    where
        HostResults: WitStore,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

impl ResultStorage for () {
    type OutputFor<HostResults> = <HostResults::Layout as Layout>::Flat
    where
        HostResults: WitStore;

    fn lower_result<HostResults, Instance>(
        self,
        result: HostResults,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::OutputFor<HostResults>, RuntimeError>
    where
        HostResults: WitStore,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        result.lower(memory)
    }
}

impl ResultStorage for GuestPointer {
    type OutputFor<HostResults> = HNil
    where
        HostResults: WitStore;

    fn lower_result<HostResults, Instance>(
        self,
        result: HostResults,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::OutputFor<HostResults>, RuntimeError>
    where
        HostResults: WitStore,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        result.store(memory, self)?;

        Ok(HNil)
    }
}
