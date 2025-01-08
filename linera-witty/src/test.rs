// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functions and types useful for writing tests.

use std::{collections::BTreeMap, fmt::Debug};

use crate::{
    wit_generation::WitInterface, InstanceWithMemory, Layout, MockInstance, RegisterWitTypes,
    WitLoad, WitStore,
};

/// Test storing an instance of `T` to memory, checking that the instance can be loaded from those
/// bytes.
///
/// Also checks if storing the loaded instance results in exactly the same bytes in
/// memory.
pub fn test_memory_roundtrip<T>(input: &T) -> anyhow::Result<()>
where
    T: Debug + Eq + WitLoad + WitStore,
{
    let mut first_instance = MockInstance::<()>::default();
    let mut first_memory = first_instance.memory()?;

    let first_address = first_memory.allocate(T::SIZE, <T::Layout as Layout>::ALIGNMENT)?;

    input.store(&mut first_memory, first_address)?;

    let loaded_instance = T::load(&first_memory, first_address)?;

    assert_eq!(&loaded_instance, input);

    // Create a clean separate memory instance
    let mut second_instance = MockInstance::<()>::default();
    let mut second_memory = second_instance.memory()?;

    let second_address = second_memory.allocate(T::SIZE, <T::Layout as Layout>::ALIGNMENT)?;

    loaded_instance.store(&mut second_memory, second_address)?;

    let total_allocated_memory = first_memory.allocate(0, 1)?.0;

    assert_eq!(
        first_memory.read(first_address, total_allocated_memory)?,
        second_memory.read(second_address, total_allocated_memory)?
    );

    Ok(())
}

/// Test lowering an instance of `T`, checking that the resulting flat layout matches the expected
/// `flat_layout`, and check that the instance can be lifted from that flat layout.
pub fn test_flattening_roundtrip<T>(input: &T) -> anyhow::Result<()>
where
    T: Debug + Eq + WitLoad + WitStore,
    <T::Layout as Layout>::Flat: Copy + Debug + Eq,
{
    let mut first_instance = MockInstance::<()>::default();
    let mut first_memory = first_instance.memory()?;
    let first_start_address = first_memory.allocate(0, 1)?;

    let first_lowered_layout = input.lower(&mut first_memory)?;
    let lifted_instance = T::lift_from(first_lowered_layout, &first_memory)?;

    assert_eq!(&lifted_instance, input);

    // Create a clean separate memory instance
    let mut second_instance = MockInstance::<()>::default();
    let mut second_memory = second_instance.memory()?;
    let second_start_address = second_memory.allocate(0, 1)?;

    let second_lowered_layout = lifted_instance.lower(&mut second_memory)?;

    assert_eq!(first_lowered_layout, second_lowered_layout);

    let total_allocated_memory = first_memory.allocate(0, 1)?.0;

    assert_eq!(
        first_memory.read(first_start_address, total_allocated_memory)?,
        second_memory.read(second_start_address, total_allocated_memory)?
    );

    Ok(())
}

/// Asserts that the WIT type dependencies of the `Interface` are the `expected_types`.
pub fn assert_interface_dependencies<'i, Interface>(
    expected_types: impl IntoIterator<Item = (&'i str, &'i str)>,
) where
    Interface: WitInterface,
{
    let mut wit_types = BTreeMap::new();

    Interface::Dependencies::register_wit_types(&mut wit_types);

    assert_eq!(
        wit_types
            .iter()
            .map(|(name, declaration)| (name.as_str(), declaration.as_str()))
            .collect::<Vec<_>>(),
        expected_types.into_iter().collect::<Vec<_>>(),
    );
}

/// Asserts that the function declarations of the `Interface` are the `expected_declarations`.
pub fn assert_interface_functions<Interface>(expected_declarations: &[impl AsRef<str>])
where
    Interface: WitInterface,
{
    let wit_functions = Interface::wit_functions();

    assert_eq!(
        wit_functions.iter().map(String::as_str).collect::<Vec<_>>(),
        expected_declarations
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>()
    );
}
