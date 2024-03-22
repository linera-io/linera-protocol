// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the results of an imported function.
//!
//! The results returned from an imported guest function may be empty, a single flat value or a
//! more complex type that's stored in the heap. That depends on the flat layout of the type
//! representing the host results, or more specifically, how many elements that flat layout has. If
//! the flat layout is empty, nothing should be returned from the guest. If there's only one
//! element, it should be returned as a value from the function. If there is more than one element,
//! then the results type is stored in a heap allocated memory region instead, and the address to
//! that region is returned as a value from the function.
//!
//! This is determined by the `MAX_FLAT_RESULTS` in the [canonical ABI's section on
//! flattening](https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening)
//! and has the value of `1`. There is no equivalent constant in Witty. Instead, the
//! [`FlatHostResults`] implementations automatically handle flat layouts with more or less
//! elements than the limit.

use frunk::HList;

use crate::{
    memory_layout::FlatLayout, primitive_types::FlatType, GuestPointer, InstanceWithMemory, Layout,
    Memory, Runtime, RuntimeError, RuntimeMemory, WitLoad, WitType,
};

/// Helper trait for converting from the guest results to the host results.
///
/// This trait is implemented for the intermediate flat layout of the host results type. This
/// allows converting from the guest results type into this flat layout and finally into the host
/// results type.
pub trait FlatHostResults: FlatLayout {
    /// The results received from the guest function.
    type GuestResults: FlatLayout;

    /// Converts the received guest `results` into the `HostResults` type.
    fn lift_results<HostResults, Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<HostResults, RuntimeError>
    where
        HostResults: WitLoad,
        <HostResults as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

/// Implementation for host results that are zero-sized types.
impl FlatHostResults for HList![] {
    type GuestResults = HList![];

    fn lift_results<Results, Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<Results, RuntimeError>
    where
        Results: WitLoad,
        <Results as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Results::lift_from(results, memory)
    }
}

/// Implementation for host results that are flattened into a single value.
impl<FlatResult> FlatHostResults for HList![FlatResult]
where
    FlatResult: FlatType,
{
    type GuestResults = HList![FlatResult];

    fn lift_results<Results, Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<Results, RuntimeError>
    where
        Results: WitLoad,
        <Results as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Results::lift_from(results, memory)
    }
}

/// Implementation for host results that are flattened into multiple values.
///
/// The value received from the guest is an address to a heap allocated region of memory containing
/// the results. The host results type is loaded from that region.
impl<FirstResult, SecondResult, Tail> FlatHostResults for HList![FirstResult, SecondResult, ...Tail]
where
    FirstResult: FlatType,
    SecondResult: FlatType,
    Tail: FlatLayout,
{
    type GuestResults = HList![i32];

    fn lift_results<Results, Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<Results, RuntimeError>
    where
        Results: WitLoad,
        <Results as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let location = GuestPointer::lift_from(results, memory)?;

        Results::load(memory, location)
    }
}
