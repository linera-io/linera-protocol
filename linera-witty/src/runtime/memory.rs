// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstraction over how different runtimes manipulate the guest WebAssembly module's memory.

use super::RuntimeError;
use crate::{Layout, WitType};
use std::borrow::Cow;

/// An address for a location in a guest WebAssembly module's memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestPointer(u32);

impl GuestPointer {
    /// Returns a new address that's the current address advanced to after the size of `T`.
    pub fn after<T: WitType>(&self) -> Self {
        GuestPointer(self.0 + T::SIZE)
    }

    /// Returns a new address that's the current address advanced to add padding to ensure it's
    /// aligned properly for `T`.
    pub fn after_padding_for<T: WitType>(&self) -> Self {
        let padding = (-(self.0 as i32) & (<T::Layout as Layout>::ALIGNMENT as i32 - 1)) as u32;

        GuestPointer(self.0 + padding)
    }

    /// Returns the address of an element in a contiguous list of properly aligned `T` types.
    pub fn index<T: WitType>(&self, index: u32) -> Self {
        let element_size = GuestPointer(T::SIZE).after_padding_for::<T>();

        GuestPointer(self.0 + index * element_size.0)
    }
}

/// Interface for accessing a runtime specific memory.
pub trait RuntimeMemory<Instance> {
    /// Reads `length` bytes from memory from the provided `location`.
    fn read<'instance>(
        &self,
        instance: &'instance Instance,
        location: GuestPointer,
        length: u32,
    ) -> Result<Cow<'instance, [u8]>, RuntimeError>;

    /// Writes the `bytes` to memory at the provided `location`.
    fn write(
        &mut self,
        instance: &mut Instance,
        location: GuestPointer,
        bytes: &[u8],
    ) -> Result<(), RuntimeError>;
}
