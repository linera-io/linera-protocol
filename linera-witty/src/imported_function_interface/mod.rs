// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic representation of a function the host imports from a guest Wasm instance.
//! This helps determining the actual signature of the imported guest Wasm function based on host
//! types for the parameters and the results.
//! The signature depends on the number of flat types used to represent the parameters and the
//! results, as specified in the [canonical ABI](https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening).

mod parameters;
mod results;

use self::{parameters::FlatHostParameters, results::FlatHostResults};
use crate::{
    memory_layout::FlatLayout, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError,
    RuntimeMemory, WitLoad, WitStore,
};

/// Representation of an imported function's interface.
/// Implemented for a tuple pair of the host parameters type and the host results type, and then
/// allows converting to the guest function's signature.
pub trait ImportedFunctionInterface {
    /// The type representing the host-side parameters.
    type HostParameters: WitStore;

    /// The type representing the host-side results.
    type HostResults: WitLoad;

    /// The flat layout representing the guest-side parameters.
    type GuestParameters: FlatLayout;

    /// The flat layout representing the guest-side results.
    type GuestResults: FlatLayout;

    /// Converts the host-side parameters into the guest-side parameters.
    fn lower_parameters<Instance>(
        parameters: Self::HostParameters,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestParameters, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;

    /// Converts the guest-side results into the host-side results.
    fn lift_results<Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self::HostResults, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

impl<Parameters, Results> ImportedFunctionInterface for (Parameters, Results)
where
    Parameters: WitStore,
    Results: WitLoad,
    <Parameters::Layout as Layout>::Flat: FlatHostParameters,
    <Results::Layout as Layout>::Flat: FlatHostResults,
{
    type HostParameters = Parameters;
    type HostResults = Results;
    type GuestParameters =
        <<Parameters::Layout as Layout>::Flat as FlatHostParameters>::GuestParameters;
    type GuestResults = <<Results::Layout as Layout>::Flat as FlatHostResults>::GuestResults;

    fn lower_parameters<Instance>(
        parameters: Self::HostParameters,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestParameters, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        <<Parameters::Layout as Layout>::Flat as FlatHostParameters>::lower_parameters(
            parameters, memory,
        )
    }

    fn lift_results<Instance>(
        results: Self::GuestResults,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self::HostResults, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        <<Results::Layout as Layout>::Flat as FlatHostResults>::lift_results(results, memory)
    }
}
