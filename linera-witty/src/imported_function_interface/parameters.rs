// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the parameters of an imported function.
//!
//! The maximum number of parameters that can be used in a WIT function is defined by the
//! [canonical ABI][flattening] as the `MAX_FLAT_PARAMS` constant (16). There is no equivalent
//! constant defined in Witty. Instead, any attempt to use more than the limit should lead to a
//! compiler error.
//!
//! The host parameters type is flattened and if it's made up of `MAX_FLAT_PARAMS` or less flat
//! types, they are sent directly as the guest's function parameters. If there are more than
//! `MAX_FLAT_PARAMS` flat types, then the host type is instead stored in a heap allocated region
//! of memory, and the address to that region is sent as a parameter instead.
//!
//! [flattening]: https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening

use frunk::HList;

use crate::{
    memory_layout::FlatLayout, primitive_types::FlatType, InstanceWithMemory, Layout, Memory,
    Runtime, RuntimeError, RuntimeMemory, WitStore, WitType,
};

/// Helper trait for converting from the host parameters to the guest parameters.
///
/// This trait is implemented for the intermediate flat layout of the host parameters type. This
/// allows converting from the host parameters type into this flat layout and finally into the guest
/// parameters type.
pub trait FlatHostParameters: FlatLayout {
    /// The actual parameters sent to the guest function.
    type GuestParameters: FlatLayout;

    /// Converts the `HostParameters` into the guest parameters that can be sent to the guest
    /// function.
    fn lower_parameters<HostParameters, Instance>(
        parameters: HostParameters,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestParameters, RuntimeError>
    where
        HostParameters: WitStore,
        <HostParameters as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

/// Macro to implement [`FlatHostParameters`] for a parameters that are sent directly to the guest.
///
/// Each element in the flat layout type is sent as a separate parameter to the function.
macro_rules! direct_parameters {
    ($( $types:ident ),*) => {
        impl<$( $types ),*> FlatHostParameters for HList![$( $types ),*]
        where
            $( $types: FlatType, )*
        {
            type GuestParameters = HList![$( $types ),*];

            fn lower_parameters<Parameters, Instance>(
                parameters: Parameters,
                memory: &mut Memory<'_, Instance>,
            ) -> Result<Self::GuestParameters, RuntimeError>
            where
                Parameters: WitStore,
                <Parameters as WitType>::Layout: Layout<Flat = Self>,
                Instance: InstanceWithMemory,
                <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
            {
                parameters.lower(memory)
            }
        }
    };
}

repeat_macro!(direct_parameters => A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Implementation of [`FlatHostParameters`] for parameters that are sent to the guest through the
/// heap.
///
/// The maximum number of parameters is defined by the [canonical
/// ABI](https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md#flattening)
/// as the `MAX_FLAT_PARAMS` constant. When more than `MAX_FLAT_PARAMS` (16) function parameters
/// are needed, they are spilled over into a heap memory allocation and the only parameter sent as
/// a function parameter is the address to that allocation.
impl<A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Tail> FlatHostParameters for HList![A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, ...Tail]
where
    A: FlatType,
    B: FlatType,
    C: FlatType,
    D: FlatType,
    E: FlatType,
    F: FlatType,
    G: FlatType,
    H: FlatType,
    I: FlatType,
    J: FlatType,
    K: FlatType,
    L: FlatType,
    M: FlatType,
    N: FlatType,
    O: FlatType,
    P: FlatType,
    Q: FlatType,
    Tail: FlatLayout,
{
    type GuestParameters = HList![i32];

    fn lower_parameters<Parameters, Instance>(
        parameters: Parameters,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<Self::GuestParameters, RuntimeError>
    where
        Parameters: WitStore,
        <Parameters as WitType>::Layout: Layout<Flat = Self>,
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>,
    {
        let location =
            memory.allocate(Parameters::SIZE, <Parameters::Layout as Layout>::ALIGNMENT)?;

        parameters.store(memory, location)?;
        location.lower(memory)
    }
}
