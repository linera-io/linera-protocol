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

    Ok(())
}
