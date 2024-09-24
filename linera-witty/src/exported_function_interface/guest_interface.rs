// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the interface a guest Wasm instance uses for a host function.
//!
//! The [`GuestInterface`] trait is implemented for the tuple pair of the flattened representation
//! of the host function's parameters type and results type. The trait then maps the flattened host
//! function signature into the interface guest Wasm instances expect. This involves selecting
//! where the host function's parameters and results are stored and if the guest Wasm instance
//! needs to provide additional parameters to the function with the addresses to store the
//! parameters and/or results in memory.
//!
//! The [canonical ABI][flattening] describes a `MAX_FLAT_PARAMS` limit on the number of flat types
//! that can be sent to the function through Wasm parameters. If more flat parameters are needed,
//! then all of them are stored in memory instead, and the guest Wasm instance must provide a
//! single parameter with the address in memory of where the parameters are stored. The same must
//! be done if more flat result types than the `MAX_FLAT_RESULTS` limit defined by the [canonical
//! ABI][flattening] need to be returned from the function. In that case, the guest Wasm instance
//! is responsible for sending an additional parameter with an address in memory where the results
//! can be stored. See [`ResultStorage`] for more information.
//!
//! [flattening]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening

use std::ops::Add;

use frunk::HList;

use super::result_storage::ResultStorage;
use crate::{
    memory_layout::FlatLayout, primitive_types::FlatType, util::Split, GuestPointer,
    InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory, WitLoad,
};

/// Representation of the interface a guest Wasm instance uses for a host function.
///
/// See the module level documentation for more information.
pub trait GuestInterface {
    /// The flattened layout of the host function's parameters type.
    ///
    /// This is used as a generic input type in all implementations, but is needed here so it can
    /// be used in the constraints of the [`Self::lift_parameters`] method.
    type FlatHostParameters: FlatLayout;

    /// The flat types the guest Wasm instances use as parameters for the host function.
    type FlatGuestParameters: FlatLayout;

    /// How the host function's results type is sent back to the guest Wasm instance.
    type ResultStorage: ResultStorage;

    /// Lifts the parameters received from the guest Wasm instance into the parameters type the
    /// host function expects.
    ///
    /// Returns the lifted host function's parameters together with a [`ResultStorage`]
    /// implementation which is later used to lower the host function's results to where the guest
    /// Wasm instance expects them to be. The [`ResultStorage`] is either the unit type `()`
    /// indicating that the results should just be lowered and sent to the guest as the function's
    /// return value, or a [`GuestPointer`] with the address of where the results should be stored
    /// in memory.
    fn lift_parameters<Instance, HostParameters>(
        guest_parameters: Self::FlatGuestParameters,
        memory: &Memory<'_, Instance>,
    ) -> Result<(HostParameters, Self::ResultStorage), RuntimeError>
    where
        HostParameters: WitLoad,
        HostParameters::Layout: Layout<Flat = Self::FlatHostParameters>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

/// Implements [`GuestInterface`] for the cases where the parameters and the results are sent
/// directly as the function's parameters and results, without requiring any of them to be moved to
/// memory.
macro_rules! direct_interface {
    ($( $types:ident ),* $(,)*) => {
        direct_interface_with_result!($( $types ),* =>);
        direct_interface_with_result!($( $types ),* => FlatResult);
    };
}

/// Helper macro to [`direct_interface`] so that it can handle the two possible return signatures
/// with all parameter combinations.
macro_rules! direct_interface_with_result {
    ($( $types:ident ),* => $( $flat_result:ident )?) => {
        impl<$( $types, )* $( $flat_result )*> GuestInterface
            for (HList![$( $types, )*], HList![$( $flat_result )*])
        where
            HList![$( $types, )*]: FlatLayout,
            $( $flat_result: FlatType, )*
        {
            type FlatHostParameters = HList![$( $types, )*];
            type FlatGuestParameters = HList![$( $types, )*];
            type ResultStorage = ();

            fn lift_parameters<Instance, HostParameters>(
                guest_parameters: Self::FlatGuestParameters,
                memory: &Memory<'_, Instance>,
            ) -> Result<(HostParameters, Self::ResultStorage), RuntimeError>
            where
                HostParameters: WitLoad,
                HostParameters::Layout: Layout<Flat = Self::FlatHostParameters>,
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let parameters = HostParameters::lift_from(guest_parameters, memory)?;

                Ok((parameters, ()))
            }
        }
    };
}

repeat_macro!(direct_interface => A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Implements [`GuestInterface`] for the cases where the parameters are sent directly as the
/// function's parameters but the results are stored in memory instead.
macro_rules! indirect_results {
    ($( $types:ident ),*) => {
        impl<$( $types, )* Y, Z, Tail> GuestInterface
            for (HList![$( $types, )*], HList![Y, Z, ...Tail])
        where
            HList![$( $types, )*]: FlatLayout + Add<HList![i32]>,
            <HList![$( $types, )*] as Add<HList![i32]>>::Output:
                FlatLayout + Split<HList![$( $types, )*], Remainder = HList![i32]>,
        {
            type FlatHostParameters = HList![$( $types, )*];
            type FlatGuestParameters = <Self::FlatHostParameters as Add<HList![i32]>>::Output;
            type ResultStorage = GuestPointer;

            fn lift_parameters<Instance, Parameters>(
                guest_parameters: Self::FlatGuestParameters,
                memory: &Memory<'_, Instance>,
            ) -> Result<(Parameters, Self::ResultStorage), RuntimeError>
            where
                Parameters: WitLoad,
                Parameters::Layout: Layout<Flat = Self::FlatHostParameters>,
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let (parameters_layout, result_storage_layout) = guest_parameters.split();
                let parameters = Parameters::lift_from(parameters_layout, memory)?;
                let result_storage = Self::ResultStorage::lift_from(result_storage_layout, memory)?;

                Ok((parameters, result_storage))
            }
        }
    };
}

repeat_macro!(indirect_results => A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Implements [`GuestInterface`] for the cases where the results are sent directly as the
/// function's return value but the parameters are stored in memory instead.
macro_rules! indirect_parameters {
    (=> $( $flat_result:ident )? ) => {
        impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Tail $(, $flat_result )*>
            GuestInterface
            for (
                HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...Tail],
                HList![$( $flat_result )*],
            )
        where
            HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...Tail]: FlatLayout,
            $( $flat_result: FlatType, )*
        {
            type FlatHostParameters =
                HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...Tail];
            type FlatGuestParameters = HList![i32];
            type ResultStorage = ();

            fn lift_parameters<Instance, Parameters>(
                guest_parameters: Self::FlatGuestParameters,
                memory: &Memory<'_, Instance>,
            ) -> Result<(Parameters, Self::ResultStorage), RuntimeError>
            where
                Parameters: WitLoad,
                Parameters::Layout: Layout<Flat = Self::FlatHostParameters>,
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                let parameters_location = GuestPointer::lift_from(guest_parameters, memory)?;
                let parameters = Parameters::load(memory, parameters_location)?;

                Ok((parameters, ()))
            }
        }
    };
}

indirect_parameters!(=>);
indirect_parameters!(=> Z);

/// Implements [`GuestInterface`] for the cases where the parameters and the results need to be
/// stored in memory.
impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, OtherParameters, Y, Z, OtherResults>
    GuestInterface
    for (
        HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...OtherParameters],
        HList![Y, Z, ...OtherResults],
    )
where
    HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...OtherParameters]: FlatLayout,
    HList![Y, Z, ...OtherResults]: FlatLayout,
{
    type FlatHostParameters =
        HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...OtherParameters];
    type FlatGuestParameters = HList![i32, i32];
    type ResultStorage = GuestPointer;

    fn lift_parameters<Instance, Parameters>(
        guest_parameters: Self::FlatGuestParameters,
        memory: &Memory<'_, Instance>,
    ) -> Result<(Parameters, Self::ResultStorage), RuntimeError>
    where
        Parameters: WitLoad,
        Parameters::Layout: Layout<Flat = Self::FlatHostParameters>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let (parameters_layout, result_storage_layout) = guest_parameters.split();
        let parameters_location = GuestPointer::lift_from(parameters_layout, memory)?;
        let parameters = Parameters::load(memory, parameters_location)?;
        let result_storage = Self::ResultStorage::lift_from(result_storage_layout, memory)?;

        Ok((parameters, result_storage))
    }
}
