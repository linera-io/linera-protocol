// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use std::any::Any;
use wasmtime::{Caller, Extern, Func, Linker};
use wit_bindgen_host_wasmtime_rust::{
    rt::{get_memory, RawMem},
    Endian,
};

/// A map of resources allocated on the host side.
#[derive(Default)]
pub struct Resources(Vec<Box<dyn Any + Send + 'static>>);

impl Resources {
    /// Adds a resource to the map, returning its handle.
    pub fn insert(&mut self, value: impl Any + Send + 'static) -> i32 {
        let handle = self.0.len().try_into().expect("Resources map overflow");

        self.0.push(Box::new(value));

        handle
    }

    /// Returns an immutable reference to a resource referenced by the provided `handle`.
    pub fn get<T: 'static>(&self, handle: i32) -> &T {
        self.0[usize::try_from(handle).expect("Invalid handle")]
            .downcast_ref()
            .expect("Incorrect handle type")
    }
}

/// Retrieves a function exported from the guest WebAssembly module.
fn get_function(caller: &mut Caller<'_, Resources>, name: &str) -> Option<Func> {
    match caller.get_export(name)? {
        Extern::Func(function) => Some(function),
        _ => None,
    }
}

/// Copies data from the `source_offset` to the `destination_offset` inside the guest WebAssembly
/// module's memory.
fn copy_memory_slices(
    caller: &mut Caller<'_, Resources>,
    source_offset: i32,
    destination_offset: i32,
    size: i32,
) {
    let memory = get_memory(caller, "memory").expect("Missing `memory` export in the module.");
    let memory_data = memory.data_mut(caller);

    let size = usize::try_from(size).expect("Invalid size of memory slice to copy");

    let source_start = usize::try_from(source_offset).expect("Invalid pointer to copy data from");
    let source_end = source_start + size;

    let destination_start =
        usize::try_from(destination_offset).expect("Invalid pointer to copy data to");

    memory_data.copy_within(source_start..source_end, destination_start);
}

/// Stores a `value` at the `offset` of the guest WebAssembly module's memory.
fn store_in_memory(caller: &mut Caller<'_, Resources>, offset: i32, value: impl Endian) {
    let memory = get_memory(caller, "memory").expect("Missing `memory` export in the module.");
    let memory_data = memory.data_mut(caller);

    memory_data
        .store(offset, value)
        .expect("Failed to write to guest WebAssembly module");
}

/// Adds the mock system APIs to the linker, so that they are available to guest WebAsembly
/// modules.
///
/// The system APIs are proxied back to the guest module, to be handled by the functions exported
/// from `linera_sdk::test::unit`.
pub fn add_to_linker(linker: &mut Linker<Resources>) -> Result<()> {
    linker.func_wrap1_async(
        "writable_system",
        "chain-id: func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-chain-id: \
                        func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
                )
                .expect(
                    "Missing `mocked-chain-id` function in the module. \
                    Please ensure `linera_sdk::test::mock_chain_id` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-chain-id` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-chain-id` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 32);
            })
        },
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "application-id: func() -> record { \
            bytecode-id: record { \
                chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                height: u64, \
                index: u32 \
            }, \
            creation: record { \
                chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                height: u64, \
                index: u32 \
            } \
        }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-application-id: func() -> record { \
                        bytecode-id: record { \
                            chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                            height: u64, \
                            index: u32 \
                        }, \
                        creation: record { \
                            chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                            height: u64, \
                            index: u32 \
                        } \
                    }",
                )
                .expect(
                    "Missing `mocked-application-id` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_id` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-application-id` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-application-id` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 96);
            })
        },
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "application-parameters: func() -> list<u8>",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-application-parameters: func() -> list<u8>",
                )
                .expect(
                    "Missing `mocked-application-parameters` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_parameters` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-application-parameters` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-application-parameters` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "read-system-balance: func() -> record { lower-half: u64, upper-half: u64 }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-read-system-balance: \
                        func() -> record { lower-half: u64, upper-half: u64 }",
                )
                .expect(
                    "Missing `mocked-read-system-balance` function in the module. \
                    Please ensure `linera_sdk::test::mock_system_balance` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-system-balance` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-read-system-balance` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 16);
            })
        },
    )?;
    linker.func_wrap0_async(
        "writable_system",
        "read-system-timestamp: func() -> u64",
        move |mut caller: Caller<'_, Resources>| {
            Box::new(async move {
                let function =
                    get_function(&mut caller, "mocked-read-system-timestamp: func() -> u64")
                        .expect(
                            "Missing `mocked-read-system-timestamp` function in the module. \
                            Please ensure `linera_sdk::test::mock_system_timestamp` was called",
                        );

                let (timestamp,) = function
                    .typed::<(), (i64,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-system-timestamp` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-read-system-timestamp` function");

                timestamp
            })
        },
    )?;
    linker.func_wrap3_async(
        "writable_system",
        "log: func(message: string, level: enum { trace, debug, info, warn, error }) -> unit",
        move |mut caller: Caller<'_, Resources>,
              message_address: i32,
              message_length: i32,
              level: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-log: func(\
                        message: string, \
                        level: enum { trace, debug, info, warn, error }\
                    ) -> unit",
                )
                .expect(
                    "Missing `mocked-log` function in the module. \
                    Please ensure `linera_sdk` is compiled with the `test` feature enabled",
                );

                let alloc_function = get_function(&mut caller, "cabi_realloc").expect(
                    "Missing `cabi_realloc` function in the module. \
                    Please ensure `linera_sdk` is compiled in with the module",
                );

                let new_message_address = alloc_function
                    .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                    .expect("Incorrect `cabi_realloc` function signature")
                    .call_async(&mut caller, (0, 0, 1, message_length))
                    .await
                    .expect("Failed to call `cabi_realloc` function");

                copy_memory_slices(
                    &mut caller,
                    message_address,
                    new_message_address,
                    message_length,
                );

                function
                    .typed::<(i32, i32, i32), (), _>(&mut caller)
                    .expect("Incorrect `mocked-log` function signature")
                    .call_async(&mut caller, (new_message_address, message_length, level))
                    .await
                    .expect("Failed to call `mocked-log` function");
            })
        },
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "load: func() -> list<u8>",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(&mut caller, "mocked-load: func() -> list<u8>").expect(
                    "Missing `mocked-load` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-load` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-load` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "load-and-lock: func() -> option<list<u8>>",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-load-and-lock: func() -> option<list<u8>>",
                )
                .expect(
                    "Missing `mocked-load-and-lock` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-load-and-lock` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-load`-and-lock function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
            })
        },
    )?;
    linker.func_wrap0_async(
        "writable_system",
        "lock::new: func() -> handle<lock>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap1_async(
        "writable_system",
        "lock::poll: func(self: handle<lock>) -> variant { \
            pending(unit), \
            ready-locked(unit), \
            ready-not-locked(unit) \
        }",
        move |mut caller: Caller<'_, Resources>, _handle: i32| {
            Box::new(async move {
                let function = get_function(&mut caller, "mocked-lock: func() -> bool").expect(
                    "Missing `mocked-lock` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                let (locked,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-lock` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-lock` function");

                match locked {
                    0 => 2,
                    _ => 1,
                }
            })
        },
    )?;

    linker.func_wrap1_async(
        "queryable_system",
        "chain-id: func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-chain-id: \
                        func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
                )
                .expect(
                    "Missing `mocked-chain-id` function in the module. \
                    Please ensure `linera_sdk::test::mock_chain_id` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-chain-id` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-chain-id` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 32);
            })
        },
    )?;
    linker.func_wrap1_async(
        "queryable_system",
        "application-id: func() -> record { \
            bytecode-id: record { \
                chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                height: u64, \
                index: u32 \
            }, \
            creation: record { \
                chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                height: u64, \
                index: u32 \
            } \
        }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-application-id: func() -> record { \
                        bytecode-id: record { \
                            chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                            height: u64, \
                            index: u32 \
                        }, \
                        creation: record { \
                            chain-id: record { part1: u64, part2: u64, part3: u64, part4: u64 }, \
                            height: u64, \
                            index: u32 \
                        } \
                    }",
                )
                .expect(
                    "Missing `mocked-application-id` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_id` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-application-id` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-application-id` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 96);
            })
        },
    )?;
    linker.func_wrap1_async(
        "queryable_system",
        "application-parameters: func() -> list<u8>",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-application-parameters: func() -> list<u8>",
                )
                .expect(
                    "Missing `mocked-application-parameters` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_parameters` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-application-parameters` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-application-parameters` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "queryable_system",
        "read-system-balance: func() -> record { lower-half: u64, upper-half: u64 }",
        move |mut caller: Caller<'_, Resources>, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-read-system-balance: \
                        func() -> record { lower-half: u64, upper-half: u64 }",
                )
                .expect(
                    "Missing `mocked-read-system-balance` function in the module. \
                    Please ensure `linera_sdk::test::mock_system_balance` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-system-balance` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-read-system-balance` function");

                copy_memory_slices(&mut caller, result_offset, return_offset, 16);
            })
        },
    )?;
    linker.func_wrap0_async(
        "queryable_system",
        "read-system-timestamp: func() -> u64",
        move |mut caller: Caller<'_, Resources>| {
            Box::new(async move {
                let function =
                    get_function(&mut caller, "mocked-read-system-timestamp: func() -> u64")
                        .expect(
                            "Missing `mocked-read-system-timestamp` function in the module. \
                            Please ensure `linera_sdk::test::mock_system_timestamp` was called",
                        );

                let (timestamp,) = function
                    .typed::<(), (i64,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-system-timestamp` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-read-system-timestamp` function");

                timestamp
            })
        },
    )?;
    linker.func_wrap3_async(
        "queryable_system",
        "log: func(message: string, level: enum { trace, debug, info, warn, error }) -> unit",
        move |mut caller: Caller<'_, Resources>,
              message_address: i32,
              message_length: i32,
              level: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-log: func(\
                        message: string, \
                        level: enum { trace, debug, info, warn, error }\
                    ) -> unit",
                )
                .expect(
                    "Missing `mocked-log` function in the module. \
                    Please ensure `linera_sdk` is compiled with the `test` feature enabled",
                );

                let alloc_function = get_function(&mut caller, "cabi_realloc").expect(
                    "Missing `cabi_realloc` function in the module. \
                    Please ensure `linera_sdk` is compiled in with the module",
                );

                let new_message_address = alloc_function
                    .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                    .expect("Incorrect `cabi_realloc` function signature")
                    .call_async(&mut caller, (0, 0, 1, message_length))
                    .await
                    .expect("Failed to call `cabi_realloc` function");

                copy_memory_slices(
                    &mut caller,
                    message_address,
                    new_message_address,
                    message_length,
                );

                function
                    .typed::<(i32, i32, i32), (), _>(&mut caller)
                    .expect("Incorrect `mocked-log` function signature")
                    .call_async(&mut caller, (new_message_address, message_length, level))
                    .await
                    .expect("Failed to call `mocked-log` function");
            })
        },
    )?;
    linker.func_wrap0_async(
        "queryable_system",
        "load::new: func() -> handle<load>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap2_async(
        "queryable_system",
        "load::poll: \
            func(self: handle<load>) -> variant { pending(unit), ready(result<list<u8>, string>) }",
        move |mut caller: Caller<'_, Resources>, _handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(&mut caller, "mocked-load: func() -> list<u8>").expect(
                    "Missing `mocked-load` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                let (result_offset,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-load` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-load` function");

                store_in_memory(&mut caller, return_offset, 1_i32);
                store_in_memory(&mut caller, return_offset + 4, 0_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 8, 8);
            })
        },
    )?;
    linker.func_wrap0_async(
        "queryable_system",
        "lock::new: func() -> handle<lock>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap2_async(
        "queryable_system",
        "lock::poll: \
            func(self: handle<lock>) -> variant { pending(unit), ready(result<unit, string>) }",
        move |mut caller: Caller<'_, Resources>, _handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(&mut caller, "mocked-lock: func() -> bool").expect(
                    "Missing `mocked-lock` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                let (locked,) = function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-lock` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect("Failed to call `mocked-lock` function");

                match locked {
                    0 => {
                        store_in_memory(&mut caller, return_offset, 1_i32);
                        store_in_memory(&mut caller, return_offset + 4, 1_i32);
                    }
                    _ => {
                        let alloc_function = get_function(&mut caller, "cabi_realloc").expect(
                            "Missing `cabi_realloc` function in the module. \
                            Please ensure `linera_sdk` is compiled in with the module",
                        );

                        let error_message = "Failed to lock view".as_bytes();
                        let error_message_length = error_message.len() as i32;
                        let error_message_address = alloc_function
                            .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                            .expect("Incorrect `cabi_realloc` function signature")
                            .call_async(&mut caller, (0, 0, 1, error_message_length))
                            .await
                            .expect("Failed to call `cabi_realloc` function");

                        store_in_memory(&mut caller, return_offset, 1_i32);
                        store_in_memory(&mut caller, return_offset + 4, 0_i32);
                        store_in_memory(&mut caller, return_offset + 8, error_message_address);
                        store_in_memory(&mut caller, return_offset + 12, error_message_length);
                    }
                }
            })
        },
    )?;

    let resource_names = ["load", "lock"];

    for resource_name in resource_names {
        linker.func_wrap1_async(
            "canonical_abi",
            &format!("resource_drop_{resource_name}"),
            move |_: Caller<'_, Resources>, _handle: i32| Box::new(async move {}),
        )?;
    }

    Ok(())
}
