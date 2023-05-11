// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_base::identifiers::{ApplicationId, ChainId, EffectId};
use linera_views::batch::WriteOperation;
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

/// A resource representing a query.
#[derive(Clone)]
struct Query {
    application_id: ApplicationId,
    query: Vec<u8>,
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

/// Loads a vector of `length` bytes starting at `offset` from the WebAssembly module's memory.
fn load_bytes(caller: &mut Caller<'_, Resources>, offset: i32, length: i32) -> Vec<u8> {
    let start = usize::try_from(offset).expect("Invalid address");
    let length = usize::try_from(length).expect("Invalid length");
    let end = start + length;

    let memory =
        get_memory(&mut *caller, "memory").expect("Missing `memory` export in the module.");
    let memory_data = memory.data_mut(caller);

    memory_data[start..end].to_vec()
}

/// Loads a vector of bytes with its starting offset and length stored in the WebAssembly module's
/// memory.
fn load_indirect_bytes(
    caller: &mut Caller<'_, Resources>,
    offset_and_length_location: i32,
) -> Vec<u8> {
    let memory =
        get_memory(&mut *caller, "memory").expect("Missing `memory` export in the module.");
    let memory_data = memory.data_mut(&mut *caller);

    let offset = memory_data
        .load(offset_and_length_location)
        .expect("Failed to read from module memory");
    let length = memory_data
        .load(offset_and_length_location + 4)
        .expect("Failed to read from module memory");

    load_bytes(caller, offset, length)
}

/// Stores some bytes from a host-side resource to the WebAssembly module's memory.
///
/// Returns the offset of the module's memory where the bytes were stored, and how many bytes were
/// stored.
async fn store_bytes_from_resource(
    caller: &mut Caller<'_, Resources>,
    bytes_getter: impl Fn(&Resources) -> &[u8],
) -> (i32, i32) {
    let resources = caller.data_mut();
    let bytes = bytes_getter(resources);
    let length = i32::try_from(bytes.len()).expect("Resource bytes is too large");

    let alloc_function = get_function(&mut *caller, "cabi_realloc")
        .expect(
            "Missing `cabi_realloc` function in the module. \
            Please ensure `linera_sdk` is compiled in with the module",
        )
        .typed::<(i32, i32, i32, i32), i32, _>(&mut *caller)
        .expect("Incorrect `cabi_realloc` function signature");

    let address = alloc_function
        .call_async(&mut *caller, (0, 0, 1, length))
        .await
        .expect("Failed to call `cabi_realloc` function");

    let memory = get_memory(caller, "memory").expect("Missing `memory` export in the module.");
    let (memory, resources) = memory.data_and_store_mut(caller);

    let bytes = bytes_getter(resources);
    let start = usize::try_from(address).expect("Invalid address allocated");
    let end = start + bytes.len();

    memory[start..end].copy_from_slice(bytes);

    (address, length)
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
    linker.func_wrap14_async(
        "queryable_system",
        "try-query-application::new: func(\
            application: record { \
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
            }, \
            query: list<u8>\
        ) -> handle<try-query-application>",
        move |mut caller: Caller<'_, Resources>,
              application_bytecode_chain_id_part1: i64,
              application_bytecode_chain_id_part2: i64,
              application_bytecode_chain_id_part3: i64,
              application_bytecode_chain_id_part4: i64,
              application_bytecode_height: i64,
              application_bytecode_index: i32,
              application_creation_chain_id_part1: i64,
              application_creation_chain_id_part2: i64,
              application_creation_chain_id_part3: i64,
              application_creation_chain_id_part4: i64,
              application_creation_height: i64,
              application_creation_index: i32,
              query_address: i32,
              query_length: i32| {
            Box::new(async move {
                let bytecode_chain_id = ChainId(
                    [
                        application_bytecode_chain_id_part1 as u64,
                        application_bytecode_chain_id_part2 as u64,
                        application_bytecode_chain_id_part3 as u64,
                        application_bytecode_chain_id_part4 as u64,
                    ]
                    .into(),
                );
                let creation_chain_id = ChainId(
                    [
                        application_creation_chain_id_part1 as u64,
                        application_creation_chain_id_part2 as u64,
                        application_creation_chain_id_part3 as u64,
                        application_creation_chain_id_part4 as u64,
                    ]
                    .into(),
                );

                let application_id = ApplicationId {
                    bytecode_id: EffectId {
                        chain_id: bytecode_chain_id,
                        height: (application_bytecode_height as u64).into(),
                        index: application_bytecode_index as u32,
                    }
                    .into(),
                    creation: EffectId {
                        chain_id: creation_chain_id,
                        height: (application_creation_height as u64).into(),
                        index: application_creation_index as u32,
                    },
                };
                let query = load_bytes(&mut caller, query_address, query_length);

                let resource = Query {
                    application_id,
                    query,
                };

                let resources = caller.data_mut();

                resources.insert(resource)
            })
        },
    )?;
    linker.func_wrap2_async(
        "queryable_system",
        "try-query-application::poll: func(self: handle<try-query-application>) -> variant { \
            pending(unit), \
            ready(result<list<u8>, string>) \
        }",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-try-query-application: func(\
                        application: record { \
                            bytecode-id: record { \
                                chain-id: record { \
                                    part1: u64, \
                                    part2: u64, \
                                    part3: u64, \
                                    part4: u64 \
                                }, \
                                height: u64, \
                                index: u32 \
                            }, \
                            creation: record { \
                                chain-id: record { \
                                    part1: u64, \
                                    part2: u64, \
                                    part3: u64, \
                                    part4: u64 \
                                }, \
                                height: u64, \
                                index: u32 \
                            } \
                        }, \
                        query: list<u8>\
                    ) -> result<list<u8>, string>",
                )
                .expect(
                    "Missing `mocked-try-query-application` function in the module. \
                    Please ensure `linera_sdk::test::mock_try_call_application` was called",
                );

                let (query_address, query_length) =
                    store_bytes_from_resource(&mut caller, |resources| {
                        let resource: &Query = resources.get(handle);
                        &resource.query
                    })
                    .await;

                let application_id = caller.data().get::<Query>(handle).application_id;

                let application_id_bytecode_chain_id: [u64; 4] =
                    application_id.bytecode_id.0.chain_id.0.into();

                let application_id_creation_chain_id: [u64; 4] =
                    application_id.creation.chain_id.0.into();

                let (result_offset,) = function
                    .typed::<(
                        i64,
                        i64,
                        i64,
                        i64,
                        i64,
                        i32,
                        i64,
                        i64,
                        i64,
                        i64,
                        i64,
                        i32,
                        i32,
                        i32,
                    ), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-try-query-application` function signature")
                    .call_async(
                        &mut caller,
                        (
                            application_id_bytecode_chain_id[0] as i64,
                            application_id_bytecode_chain_id[1] as i64,
                            application_id_bytecode_chain_id[2] as i64,
                            application_id_bytecode_chain_id[3] as i64,
                            application_id.bytecode_id.0.height.0 as i64,
                            application_id.bytecode_id.0.index as i32,
                            application_id_creation_chain_id[0] as i64,
                            application_id_creation_chain_id[1] as i64,
                            application_id_creation_chain_id[2] as i64,
                            application_id_creation_chain_id[3] as i64,
                            application_id.creation.height.0 as i64,
                            application_id.creation.index as i32,
                            query_address,
                            query_length,
                        ),
                    )
                    .await
                    .expect("Failed to call `mocked-try-query-application` function");

                store_in_memory(&mut caller, return_offset, 1_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 4, 12);
            })
        },
    )?;

    linker.func_wrap2_async(
        "view_system_api",
        "read-key-bytes::new: func(key: list<u8>) -> handle<read-key-bytes>",
        move |mut caller: Caller<'_, Resources>, key_address: i32, key_length: i32| {
            Box::new(async move {
                let key = load_bytes(&mut caller, key_address, key_length);
                let resources = caller.data_mut();

                resources.insert(key)
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "read-key-bytes::poll: func(self: handle<read-key-bytes>) -> variant { \
            pending(unit), \
            ready(option<list<u8>>) \
        }",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-read-key-bytes: func(key: list<u8>) -> option<list<u8>>",
                )
                .expect(
                    "Missing `mocked-read-key-bytes` function in the module. \
                    Please ensure `linera_sdk::test::mock_key_value_store` was called",
                );

                let (key_address, key_length) =
                    store_bytes_from_resource(&mut caller, |resources| {
                        let key: &Vec<u8> = resources.get(handle);
                        key
                    })
                    .await;

                let (result_offset,) = function
                    .typed::<(i32, i32), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-key-bytes` function signature")
                    .call_async(&mut caller, (key_address, key_length))
                    .await
                    .expect("Failed to call `mocked-read-key-bytes` function");

                store_in_memory(&mut caller, return_offset, 1_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 4, 12);
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "find-keys::new: func(prefix: list<u8>) -> handle<find-keys>",
        move |mut caller: Caller<'_, Resources>, prefix_address: i32, prefix_length: i32| {
            Box::new(async move {
                let prefix = load_bytes(&mut caller, prefix_address, prefix_length);
                let resources = caller.data_mut();

                resources.insert(prefix)
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "find-keys::poll: func(self: handle<find-keys>) -> variant { \
            pending(unit), \
            ready(list<list<u8>>) \
        }",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-find-keys: func(prefix: list<u8>) -> list<list<u8>>",
                )
                .expect(
                    "Missing `mocked-find-keys` function in the module. \
                    Please ensure `linera_sdk::test::mock_key_value_store` was called",
                );

                let (prefix_address, prefix_length) =
                    store_bytes_from_resource(&mut caller, |resources| {
                        let prefix: &Vec<u8> = resources.get(handle);
                        prefix
                    })
                    .await;

                let (result_offset,) = function
                    .typed::<(i32, i32), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-find-keys` function signature")
                    .call_async(&mut caller, (prefix_address, prefix_length))
                    .await
                    .expect("Failed to call `mocked-find-keys` function");

                store_in_memory(&mut caller, return_offset, 1_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 4, 12);
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "find-key-values::new: func(prefix: list<u8>) -> handle<find-key-values>",
        move |mut caller: Caller<'_, Resources>, prefix_address: i32, prefix_length: i32| {
            Box::new(async move {
                let prefix = load_bytes(&mut caller, prefix_address, prefix_length);
                let resources = caller.data_mut();

                resources.insert(prefix)
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "find-key-values::poll: func(self: handle<find-key-values>) -> variant { \
            pending(unit), \
            ready(list<tuple<list<u8>, list<u8>>>) \
        }",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-find-key-values: \
                        func(prefix: list<u8>) -> list<tuple<list<u8>, list<u8>>>",
                )
                .expect(
                    "Missing `mocked-find-key-values` function in the module. \
                    Please ensure `linera_sdk::test::mock_key_value_store` was called",
                );

                let (prefix_address, prefix_length) =
                    store_bytes_from_resource(&mut caller, |resources| {
                        let prefix: &Vec<u8> = resources.get(handle);
                        prefix
                    })
                    .await;

                let (result_offset,) = function
                    .typed::<(i32, i32), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-find-key-values` function signature")
                    .call_async(&mut caller, (prefix_address, prefix_length))
                    .await
                    .expect("Failed to call `mocked-find-key-values` function");

                store_in_memory(&mut caller, return_offset, 1_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 4, 12);
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "write-batch::new: func(\
            key: list<variant { \
                delete(list<u8>), \
                deleteprefix(list<u8>), \
                put(tuple<list<u8>, list<u8>>) \
            }>\
        ) -> handle<write-batch>",
        move |mut caller: Caller<'_, Resources>,
              operations_address: i32,
              operations_length: i32| {
            Box::new(async move {
                let vector_length = operations_length
                    .try_into()
                    .expect("Invalid operations list length");

                let memory = get_memory(&mut caller, "memory")
                    .expect("Missing `memory` export in the module.");
                let memory_data = memory.data_mut(&mut caller);

                let mut offsets_and_codes = Vec::with_capacity(vector_length);
                let mut operations = Vec::with_capacity(vector_length);
                let operation_size = 20;

                for index in 0..operations_length {
                    let offset = operations_address + index * operation_size;
                    let operation_code = memory_data
                        .load::<u8>(offset)
                        .expect("Failed to read from WebAssembly module's memory");

                    offsets_and_codes.push((offset + 4, operation_code));
                }

                for (offset, operation_code) in offsets_and_codes {
                    let operation = match operation_code {
                        0 => WriteOperation::Delete {
                            key: load_indirect_bytes(&mut caller, offset),
                        },
                        1 => WriteOperation::DeletePrefix {
                            key_prefix: load_indirect_bytes(&mut caller, offset),
                        },
                        2 => WriteOperation::Put {
                            key: load_indirect_bytes(&mut caller, offset),
                            value: load_indirect_bytes(&mut caller, offset + 8),
                        },
                        _ => unreachable!("Unknown write operation"),
                    };

                    operations.push(operation);
                }

                let resources = caller.data_mut();
                resources.insert(operations)
            })
        },
    )?;
    linker.func_wrap1_async(
        "view_system_api",
        "write-batch::poll: func(self: handle<write-batch>) -> variant { \
            pending(unit), \
            ready(unit) \
        }",
        move |mut caller: Caller<'_, Resources>, handle: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-write-batch: func(\
                        operations: list<variant { \
                            delete(list<u8>), \
                            deleteprefix(list<u8>), \
                            put(tuple<list<u8>, list<u8>>) \
                        }>\
                    ) -> unit",
                )
                .expect(
                    "Missing `mocked-write-batch` function in the module. \
                    Please ensure `linera_sdk::test::mock_key_value_store` was called",
                );

                let alloc_function = get_function(&mut caller, "cabi_realloc").expect(
                    "Missing `cabi_realloc` function in the module. \
                    Please ensure `linera_sdk` is compiled in with the module",
                );

                let resources = caller.data_mut();
                let operations: &Vec<WriteOperation> = resources.get(handle);
                let operation_count = operations.len();

                let codes_and_parameter_counts = operations
                    .iter()
                    .map(|operation| match operation {
                        WriteOperation::Delete { .. } => (0, 1),
                        WriteOperation::DeletePrefix { .. } => (1, 1),
                        WriteOperation::Put { .. } => (2, 2),
                    })
                    .collect::<Vec<_>>();

                let operation_size = 20;
                let vector_length =
                    i32::try_from(operation_count).expect("Too many operations in batch");
                let vector_memory_size = vector_length * operation_size;

                let operations_vector = alloc_function
                    .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                    .expect("Incorrect `cabi_realloc` function signature")
                    .call_async(&mut caller, (0, 0, 1, vector_memory_size))
                    .await
                    .expect("Failed to call `cabi_realloc` function");

                for (index, (operation_code, parameter_count)) in
                    codes_and_parameter_counts.into_iter().enumerate()
                {
                    let vector_index = i32::try_from(index).expect("Too many operations in batch");
                    let offset = operations_vector + vector_index * operation_size;

                    store_in_memory(&mut caller, offset, operation_code);

                    for parameter in 0..parameter_count {
                        let (bytes_offset, bytes_length) =
                            store_bytes_from_resource(&mut caller, |resources| {
                                let operations: &Vec<WriteOperation> = resources.get(handle);
                                match (&operations[index], parameter) {
                                    (WriteOperation::Delete { key }, 0) => key,
                                    (WriteOperation::DeletePrefix { key_prefix }, 0) => key_prefix,
                                    (WriteOperation::Put { key, .. }, 0) => key,
                                    (WriteOperation::Put { value, .. }, 1) => value,
                                    _ => unreachable!("Unknown write operation parameter"),
                                }
                            })
                            .await;

                        let parameter_offset = offset + 4 + parameter * 8;

                        store_in_memory(&mut caller, parameter_offset, bytes_offset);
                        store_in_memory(&mut caller, parameter_offset + 4, bytes_length);
                    }
                }

                function
                    .typed::<(i32, i32), (), _>(&mut caller)
                    .expect("Incorrect `mocked-write-batch` function signature")
                    .call_async(&mut caller, (operations_vector, vector_length))
                    .await
                    .expect("Failed to call `mocked-write-batch` function");

                1
            })
        },
    )?;

    let resource_names = [
        "load",
        "lock",
        "read-key-bytes",
        "find-keys",
        "find-key-values",
        "write-batch",
        "try-query-application",
    ];

    for resource_name in resource_names {
        linker.func_wrap1_async(
            "canonical_abi",
            &format!("resource_drop_{resource_name}"),
            move |_: Caller<'_, Resources>, _handle: i32| Box::new(async move {}),
        )?;
    }

    Ok(())
}
