// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use wasmtime::{Caller, Extern, Func, Linker};
use wit_bindgen_host_wasmtime_rust::rt::get_memory;

/// Retrieves a function exported from the guest WebAssembly module.
fn get_function(caller: &mut Caller<'_, ()>, name: &str) -> Option<Func> {
    match caller.get_export(name)? {
        Extern::Func(function) => Some(function),
        _ => None,
    }
}

/// Copies data from the `source_offset` to the `destination_offset` inside the guest WebAssembly
/// module's memory.
fn copy_memory_slices(
    caller: &mut Caller<'_, ()>,
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

/// Adds the mock system APIs to the linker, so that they are available to guest WebAsembly
/// modules.
///
/// The system APIs are proxied back to the guest module, to be handled by the functions exported
/// from `linera_sdk::test::unit`.
pub fn add_to_linker(linker: &mut Linker<()>) -> Result<()> {
    linker.func_wrap1_async(
        "writable_system",
        "chain-id: func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>| {
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

    linker.func_wrap1_async(
        "queryable_system",
        "chain-id: func() -> record { part1: u64, part2: u64, part3: u64, part4: u64 }",
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>, return_offset: i32| {
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
        move |mut caller: Caller<'_, ()>| {
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

    Ok(())
}
