// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use linera_base::identifiers::{ApplicationId, BytecodeId, ChainId, MessageId};
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

/// Stores some `bytes` at the `offset` of the guest WebAssembly module's memory.
fn store_bytes_in_memory(caller: &mut Caller<'_, Resources>, offset: i32, bytes: &[u8]) {
    let memory = get_memory(caller, "memory").expect("Missing `memory` export in the module.");
    let memory_data = memory.data_mut(caller);

    let start = usize::try_from(offset).expect("Invalid destination address");
    let end = start + bytes.len();

    memory_data[start..end].copy_from_slice(bytes);
}

/// Adds the mock system APIs to the linker, so that they are available to guest WebAsembly
/// modules.
///
/// The system APIs are proxied back to the guest module, to be handled by the functions exported
/// from `linera_sdk::test::unit`.
pub fn add_to_linker(linker: &mut Linker<Resources>) -> Result<()> {
    linker.func_wrap1_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-chain-id` function. \
                        Please ensure `linera_sdk::test::mock_chain_id` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 32);
            })
        },
    )?;
    linker.func_wrap1_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-application-id` function. \
                        Please ensure `linera_sdk::test::mock_application_id` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 96);
            })
        },
    )?;
    linker.func_wrap1_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-application-parameters` function. \
                        Please ensure `linera_sdk::test::mock_application_parameters` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-read-system-balance` function. \
                        Please ensure `linera_sdk::test::mock_system_balance` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 16);
            })
        },
    )?;
    linker.func_wrap0_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-read-system-timestamp` function. \
                        Please ensure `linera_sdk::test::mock_system_timestamp` was called",
                    );

                timestamp
            })
        },
    )?;
    linker.func_wrap3_async(
        "contract_system_api",
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
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-load` function. \
                        Please ensure `linera_sdk::test::mock_application_state` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "contract_system_api",
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
                    .expect(
                        "Failed to call `mocked-load-and-lock` function. \
                        Please ensure `linera_sdk::test::mock_application_state` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
            })
        },
    )?;
    linker.func_wrap0_async(
        "contract_system_api",
        "lock::new: func() -> handle<lock>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap1_async(
        "contract_system_api",
        "lock::wait: func(self: handle<lock>) -> unit",
        move |mut caller: Caller<'_, Resources>, _handle: i32| {
            Box::new(async move {
                let function = get_function(&mut caller, "mocked-lock: func() -> bool").expect(
                    "Missing `mocked-lock` function in the module. \
                    Please ensure `linera_sdk::test::mock_application_state` was called",
                );

                function
                    .typed::<(), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-lock` function signature")
                    .call_async(&mut caller, ())
                    .await
                    .expect(
                        "Failed to call `mocked-lock` function. \
                        Please ensure `linera_sdk::test::mock_application_state` was called",
                    );
            })
        },
    )?;

    linker.func_wrap1_async(
        "service_system_api",
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
                    .expect(
                        "Failed to call `mocked-chain-id` function. \
                        Please ensure `linera_sdk::test::mock_chain_id` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 32);
            })
        },
    )?;
    linker.func_wrap1_async(
        "service_system_api",
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
                    .expect(
                        "Failed to call `mocked-application-id` function. \
                        Please ensure `linera_sdk::test::mock_application_id` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 96);
            })
        },
    )?;
    linker.func_wrap1_async(
        "service_system_api",
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
                    .expect(
                        "Failed to call `mocked-application-parameters` function. \
                        Please ensure `linera_sdk::test::mock_application_parameters` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap1_async(
        "service_system_api",
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
                    .expect(
                        "Failed to call `mocked-read-system-balance` function. \
                        Please ensure `linera_sdk::test::mock_system_balance` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 16);
            })
        },
    )?;
    linker.func_wrap0_async(
        "service_system_api",
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
                    .expect(
                        "Failed to call `mocked-read-system-timestamp` function. \
                        Please ensure `linera_sdk::test::mock_system_timestamp` was called",
                    );

                timestamp
            })
        },
    )?;
    linker.func_wrap3_async(
        "service_system_api",
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
        "service_system_api",
        "load::new: func() -> handle<load>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap2_async(
        "service_system_api",
        "load::wait: func(self: handle<load>) -> result<list<u8>, string>",
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
                    .expect(
                        "Failed to call `mocked-load` function. \
                        Please ensure `linera_sdk::test::mock_application_state` was called",
                    );

                store_in_memory(&mut caller, return_offset, 0_i32);
                copy_memory_slices(&mut caller, result_offset, return_offset + 4, 8);
            })
        },
    )?;
    linker.func_wrap0_async(
        "service_system_api",
        "lock::new: func() -> handle<lock>",
        move |_: Caller<'_, Resources>| Box::new(async move { 0 }),
    )?;
    linker.func_wrap2_async(
        "service_system_api",
        "lock::wait: func(self: handle<lock>) -> result<unit, string>",
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
                    .expect(
                        "Failed to call `mocked-lock` function. \
                        Please ensure `linera_sdk::test::mock_application_state` was called",
                    );

                match locked {
                    0 => {
                        store_in_memory(&mut caller, return_offset, 1_i32);
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

                        store_in_memory(&mut caller, return_offset, 0_i32);
                        store_in_memory(&mut caller, return_offset + 4, error_message_address);
                        store_in_memory(&mut caller, return_offset + 8, error_message_length);
                    }
                }
            })
        },
    )?;
    linker.func_wrap14_async(
        "service_system_api",
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
                    bytecode_id: BytecodeId::new(MessageId {
                        chain_id: bytecode_chain_id,
                        height: (application_bytecode_height as u64).into(),
                        index: application_bytecode_index as u32,
                    }),
                    creation: MessageId {
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
        "service_system_api",
        "try-query-application::wait: func(self: handle<try-query-application>) -> \
            result<list<u8>, string>",
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
                    application_id.bytecode_id.message_id.chain_id.0.into();

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
                            application_id.bytecode_id.message_id.height.0 as i64,
                            application_id.bytecode_id.message_id.index as i32,
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
                    .expect(
                        "Failed to call `mocked-try-query-application` function. \
                        Please ensure `linera_sdk::test::mock_try_call_application` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
            })
        },
    )?;

    linker.func_wrap2_async(
        "view_system_api",
        "read-multi-values-bytes::new: \
            func(key: list<list<u8>>) -> handle<read-multi-values-bytes>",
        move |mut caller: Caller<'_, Resources>, key_list_address: i32, key_list_length: i32| {
            Box::new(async move {
                let key_list_element_size = 8;

                let keys = (0..key_list_length)
                    .map(|index| {
                        load_indirect_bytes(
                            &mut caller,
                            key_list_address + index * key_list_element_size,
                        )
                    })
                    .collect::<Vec<Vec<u8>>>();

                let resources = caller.data_mut();

                resources.insert(keys)
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "read-multi-values-bytes::wait: \
            func(self: handle<read-multi-values-bytes>) -> list<option<list<u8>>>",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-read-multi-values-bytes: func(\
                        keys: list<list<u8>>\
                    ) -> list<option<list<u8>>>",
                )
                .expect(
                    "Missing `mocked-read-multi-values-bytes` function in the module. \
                    Please ensure `linera_sdk::test::mock_key_value_store` was called",
                );

                let keys = caller.data_mut().get::<Vec<Vec<u8>>>(handle).clone();

                let alloc_function = get_function(&mut caller, "cabi_realloc")
                    .expect(
                        "Missing `cabi_realloc` function in the module. \
                        Please ensure `linera_sdk` is compiled in with the module",
                    )
                    .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                    .expect("Incorrect `cabi_realloc` function signature");

                let key_list_element_size = 8;
                let key_list_length = keys.len().try_into().expect("Too many keys");
                let key_list_address = alloc_function
                    .call_async(
                        &mut caller,
                        (0, 0, 1, key_list_length * key_list_element_size),
                    )
                    .await
                    .expect("Failed to call `cabi_realloc` function");

                for (index, key) in keys.into_iter().enumerate() {
                    let key_length = key.len().try_into().expect("Key is too long");
                    let key_address = alloc_function
                        .call_async(&mut caller, (0, 0, 1, key_length))
                        .await
                        .expect("Failed to call `cabi_realloc` function");

                    let key_list_offset = key_list_address + index as i32 * key_list_element_size;

                    store_bytes_in_memory(&mut caller, key_address, &key);
                    store_in_memory(&mut caller, key_list_offset, key_address);
                    store_in_memory(&mut caller, key_list_offset + 4, key_length);
                }

                let (result_offset,) = function
                    .typed::<(i32, i32), (i32,), _>(&mut caller)
                    .expect("Incorrect `mocked-read-multi-values-bytes` function signature")
                    .call_async(&mut caller, (key_list_address, key_list_length))
                    .await
                    .expect(
                        "Failed to call `mocked-read-multi-values-bytes` function. \
                        Please ensure `linera_sdk::test::mock_key_value_store` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 8);
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "read-value-bytes::new: func(key: list<u8>) -> handle<read-value-bytes>",
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
        "read-value-bytes::wait: func(self: handle<read-value-bytes>) -> option<list<u8>>",
        move |mut caller: Caller<'_, Resources>, handle: i32, return_offset: i32| {
            Box::new(async move {
                let function = get_function(
                    &mut caller,
                    "mocked-read-value-bytes: func(key: list<u8>) -> option<list<u8>>",
                )
                .expect(
                    "Missing `mocked-read-value-bytes` function in the module. \
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
                    .expect("Incorrect `mocked-read-value-bytes` function signature")
                    .call_async(&mut caller, (key_address, key_length))
                    .await
                    .expect(
                        "Failed to call `mocked-read-value-bytes` function. \
                        Please ensure `linera_sdk::test::mock_key_value_store` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
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
        "find-keys::wait: func(self: handle<find-keys>) -> list<list<u8>>",
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
                    .expect(
                        "Failed to call `mocked-find-keys` function. \
                        Please ensure `linera_sdk::test::mock_key_value_store` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
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
        "find-key-values::wait: func(self: handle<find-key-values>) -> \
            list<tuple<list<u8>, list<u8>>>",
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
                    .expect(
                        "Failed to call `mocked-find-key-values` function. \
                        Please ensure `linera_sdk::test::mock_key_value_store` was called",
                    );

                copy_memory_slices(&mut caller, result_offset, return_offset, 12);
            })
        },
    )?;
    linker.func_wrap2_async(
        "view_system_api",
        "write-batch: func(\
            key: list<variant { \
                delete(list<u8>), \
                deleteprefix(list<u8>), \
                put(tuple<list<u8>, list<u8>>) \
            }>\
        ) -> unit",
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

                let alloc_function = get_function(&mut caller, "cabi_realloc")
                    .expect(
                        "Missing `cabi_realloc` function in the module. \
                    Please ensure `linera_sdk` is compiled in with the module",
                    )
                    .typed::<(i32, i32, i32, i32), i32, _>(&mut caller)
                    .expect("Incorrect `cabi_realloc` function signature");

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
                        let bytes = match (&operations[index], parameter) {
                            (WriteOperation::Delete { key }, 0) => key,
                            (WriteOperation::DeletePrefix { key_prefix }, 0) => key_prefix,
                            (WriteOperation::Put { key, .. }, 0) => key,
                            (WriteOperation::Put { value, .. }, 1) => value,
                            _ => unreachable!("Unknown write operation parameter"),
                        };
                        let bytes_length =
                            i32::try_from(bytes.len()).expect("Operation is too large");

                        let bytes_offset = alloc_function
                            .call_async(&mut caller, (0, 0, 1, bytes_length))
                            .await
                            .expect("Failed to call `cabi_realloc` function");

                        store_bytes_in_memory(&mut caller, bytes_offset, bytes);

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
                    .expect(
                        "Failed to call `mocked-write-batch` function. \
                        Please ensure `linera_sdk::test::mock_key_value_store` was called",
                    );
            })
        },
    )?;

    let resource_names = [
        "load",
        "lock",
        "read-multi-values-bytes",
        "read-value-bytes",
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
