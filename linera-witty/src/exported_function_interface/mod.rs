// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper traits for exporting host functions to guest Wasm instances.
//!
//! These help determining the function signature the guest expects based on the host function
//! signature.

mod guest_interface;
mod result_storage;

use self::{guest_interface::GuestInterface, result_storage::ResultStorage};
use crate::{
    InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory, WitLoad, WitStore,
    WitType,
};

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

/// Representation of an exported host function's interface.
///
/// Implemented for a tuple pair of the host parameters type and the host results type, and allows
/// converting to the signature the guest Wasm instance uses for that host function.
pub trait ExportedFunctionInterface {
    /// The type representing the host-side parameters.
    type HostParameters: WitType;

    /// The type representing the host-side results.
    type HostResults: WitStore;

    /// The representation of the guest-side function interface.
    type GuestInterface: GuestInterface<
        FlatHostParameters = <<Self::HostParameters as WitType>::Layout as Layout>::Flat,
        ResultStorage = Self::ResultStorage,
    >;

    /// The type representing the guest-side parameters.
    type GuestParameters;

    /// The type representing the guest-side results.
    type GuestResults;

    /// How the results from the exported host function should be sent back to the guest.
    type ResultStorage: ResultStorage<OutputFor<Self::HostResults> = Self::GuestResults>;

    /// Converts the guest-side parameters into the host-side parameters.
    fn lift_parameters<Instance>(
        guest_parameters: Self::GuestParameters,
        memory: &Memory<'_, Instance>,
    ) -> Result<(Self::HostParameters, Self::ResultStorage), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;

    /// Converts the host-side results into the guest-side results.
    fn lower_results<Instance>(
        results: Self::HostResults,
        result_storage: Self::ResultStorage,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestResults, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

impl<Parameters, Results> ExportedFunctionInterface for (Parameters, Results)
where
    Parameters: WitLoad,
    Results: WitStore,
    (
        <Parameters::Layout as Layout>::Flat,
        <Results::Layout as Layout>::Flat,
    ): GuestInterface<FlatHostParameters = <Parameters::Layout as Layout>::Flat>,
    <() as WitType>::Layout: Layout<Flat = frunk::HNil>,
{
    type HostParameters = Parameters;
    type HostResults = Results;
    type GuestInterface = (
        <Parameters::Layout as Layout>::Flat,
        <Results::Layout as Layout>::Flat,
    );
    type GuestParameters = <Self::GuestInterface as GuestInterface>::FlatGuestParameters;
    type GuestResults =
        <<Self::GuestInterface as GuestInterface>::ResultStorage as ResultStorage>::OutputFor<
            Self::HostResults,
        >;
    type ResultStorage = <Self::GuestInterface as GuestInterface>::ResultStorage;

    fn lift_parameters<Instance>(
        guest_parameters: Self::GuestParameters,
        memory: &Memory<'_, Instance>,
    ) -> Result<(Self::HostParameters, Self::ResultStorage), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        Self::GuestInterface::lift_parameters(guest_parameters, memory)
    }

    fn lower_results<Instance>(
        results: Self::HostResults,
        result_storage: Self::ResultStorage,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestResults, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        result_storage.lower_result(results, memory)
    }
}
