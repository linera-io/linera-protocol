// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! How to access the memory of a Wasmtime guest instance.

use super::{super::traits::InstanceWithMemory, EntrypointInstance};
use crate::{GuestPointer, RuntimeError, RuntimeMemory};
use std::borrow::Cow;
use wasmtime::{Extern, Memory};

impl InstanceWithMemory for EntrypointInstance {
    fn memory_from_export(&self, export: Extern) -> Result<Option<Memory>, RuntimeError> {
        Ok(match export {
            Extern::Memory(memory) => Some(memory),
            _ => None,
        })
    }
}

impl RuntimeMemory<EntrypointInstance> for Memory {
    fn read<'instance>(
        &self,
        instance: &'instance EntrypointInstance,
        location: GuestPointer,
        length: u32,
    ) -> Result<Cow<'instance, [u8]>, RuntimeError> {
        let start = location.0 as usize;
        let end = start + length as usize;

        Ok(Cow::Borrowed(&self.data(instance)[start..end]))
    }

    fn write(
        &mut self,
        instance: &mut EntrypointInstance,
        location: GuestPointer,
        bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        let start = location.0 as usize;
        let end = start + bytes.len();

        self.data_mut(instance)[start..end].copy_from_slice(bytes);

        Ok(())
    }
}
