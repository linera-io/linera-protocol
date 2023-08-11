// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! How to access the memory of a Wasmer guest instance.

use super::{super::traits::InstanceWithMemory, EntrypointInstance};
use crate::{GuestPointer, RuntimeError, RuntimeMemory};
use std::borrow::Cow;
use wasmer::{Extern, Memory};

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
        let mut buffer = vec![0u8; length as usize];
        let start = location.0 as u64;

        self.view(instance).read(start, &mut buffer)?;

        Ok(Cow::Owned(buffer))
    }

    fn write(
        &mut self,
        instance: &mut EntrypointInstance,
        location: GuestPointer,
        bytes: &[u8],
    ) -> Result<(), RuntimeError> {
        let start = location.0 as u64;

        self.view(&*instance).write(start, bytes)?;

        Ok(())
    }
}
