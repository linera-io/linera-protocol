// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! How to access the memory of a Wasmtime guest instance.

use std::borrow::Cow;

use wasmtime::{Extern, Memory};

use super::{super::traits::InstanceWithMemory, EntrypointInstance, ReentrantInstance};
use crate::{GuestPointer, RuntimeError, RuntimeMemory};

macro_rules! impl_memory_traits {
    ($instance:ty) => {
        impl<UserData> InstanceWithMemory for $instance {
            fn memory_from_export(&self, export: Extern) -> Result<Option<Memory>, RuntimeError> {
                Ok(match export {
                    Extern::Memory(memory) => Some(memory),
                    _ => None,
                })
            }
        }

        impl<UserData> RuntimeMemory<$instance> for Memory {
            fn read<'instance>(
                &self,
                instance: &'instance $instance,
                location: GuestPointer,
                length: u32,
            ) -> Result<Cow<'instance, [u8]>, RuntimeError> {
                let start = location.0 as usize;
                let end = start + length as usize;

                Ok(Cow::Borrowed(&self.data(instance)[start..end]))
            }

            fn write(
                &mut self,
                instance: &mut $instance,
                location: GuestPointer,
                bytes: &[u8],
            ) -> Result<(), RuntimeError> {
                let start = location.0 as usize;
                let end = start + bytes.len();

                self.data_mut(instance)[start..end].copy_from_slice(bytes);

                Ok(())
            }
        }
    };
}

impl_memory_traits!(EntrypointInstance<UserData>);
impl_memory_traits!(ReentrantInstance<'_, UserData>);
