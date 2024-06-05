// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! How to access the memory of a Wasmer guest instance.

use std::borrow::Cow;

use wasmer::{Extern, Memory};

use super::{super::traits::InstanceWithMemory, EntrypointInstance, ReentrantInstance};
use crate::{GuestPointer, RuntimeError, RuntimeMemory};

macro_rules! impl_memory_traits {
    ($instance:ty) => {
        impl<UserData: 'static> InstanceWithMemory for $instance {
            fn memory_from_export(&self, export: Extern) -> Result<Option<Memory>, RuntimeError> {
                Ok(match export {
                    Extern::Memory(memory) => Some(memory),
                    _ => None,
                })
            }
        }

        impl<UserData: 'static> RuntimeMemory<$instance> for Memory {
            fn read<'instance>(
                &self,
                instance: &'instance $instance,
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
                instance: &mut $instance,
                location: GuestPointer,
                bytes: &[u8],
            ) -> Result<(), RuntimeError> {
                let start = location.0 as u64;

                self.view(&*instance).write(start, bytes)?;

                Ok(())
            }
        }
    };
}

impl_memory_traits!(EntrypointInstance<UserData>);
impl_memory_traits!(ReentrantInstance<'_, UserData>);
