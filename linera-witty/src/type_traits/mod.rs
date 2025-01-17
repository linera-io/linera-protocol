// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Traits used to allow complex types to be sent and received between hosts and guests using WIT.

mod implementations;
mod register_wit_types;

use std::borrow::Cow;

pub use self::register_wit_types::RegisterWitTypes;
use crate::{
    GuestPointer, InstanceWithMemory, Layout, Memory, Runtime, RuntimeError, RuntimeMemory,
};

/// A type that is representable by fundamental WIT types.
pub trait WitType {
    /// The size of the type when laid out in memory.
    const SIZE: u32;

    /// The layout of the type as fundamental types.
    type Layout: Layout;

    /// Other [`WitType`]s that this type depends on.
    type Dependencies: RegisterWitTypes;

    /// Generates the WIT type name for this type.
    fn wit_type_name() -> Cow<'static, str>;

    /// Generates the WIT type declaration for this type.
    fn wit_type_declaration() -> Cow<'static, str>;
}

/// A type that can be loaded from a guest Wasm module.
pub trait WitLoad: WitType + Sized {
    /// Loads an instance of the type from the `location` in the guest's `memory`.
    fn load<Instance>(
        memory: &Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;

    /// Lifts an instance of the type from the `flat_layout` representation.
    ///
    /// May read from the `memory` if the type has references to heap data.
    fn lift_from<Instance>(
        flat_layout: <Self::Layout as Layout>::Flat,
        memory: &Memory<'_, Instance>,
    ) -> Result<Self, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}

/// A type that can be stored in a guest Wasm module.
pub trait WitStore: WitType {
    /// Stores the type at the `location` in the guest's `memory`.
    fn store<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
        location: GuestPointer,
    ) -> Result<(), RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;

    /// Lowers the type into its flat layout representation.
    ///
    /// May write to the `memory` if the type has references to heap data or if it doesn't fix in
    /// the maximum flat layout size.
    fn lower<Instance>(
        &self,
        memory: &mut Memory<'_, Instance>,
    ) -> Result<<Self::Layout as Layout>::Flat, RuntimeError>
    where
        Instance: InstanceWithMemory,
        <Instance::Runtime as Runtime>::Memory: RuntimeMemory<Instance>;
}
