// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for implementations of the custom traits for existing types.

use crate::{FakeInstance, InstanceWithMemory, Layout, WitLoad, WitStore};
use frunk::hlist;
use std::fmt::Debug;

/// Test roundtrip of a heterogeneous list that doesn't need any internal padding.
#[test]
fn hlist_without_padding() {
    let input = hlist![
        0x1011_1213_1415_1617_1819_1a1b_1c1d_1e1f_u128,
        0x2021_2223_2425_2627_i64,
        0x3031_3233_u32,
        0x4041_i16,
        true,
    ];

    test_memory_roundtrip(
        input,
        &[
            0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12,
            0x11, 0x10, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x20, 0x33, 0x32, 0x31, 0x30,
            0x41, 0x40, 0x01,
        ],
    );
    test_flattening_roundtrip(
        input,
        hlist![
            0x1819_1a1b_1c1d_1e1f_i64,
            0x1011_1213_1415_1617_i64,
            0x2021_2223_2425_2627_i64,
            0x3031_3233_i32,
            0x0000_4041_i32,
            0x0000_0001_i32,
        ],
    );
}

/// Test roundtrip of a heterogeneous list that needs internal padding between some of its elements.
#[test]
fn hlist_with_padding() {
    let input = hlist![
        true,
        0x1011_i16,
        0x2021_u16,
        0x3031_3233_u32,
        0x4041_4243_4445_4647_i64,
    ];

    test_memory_roundtrip(
        input,
        &[
            0x01, 0, 0x11, 0x10, 0x21, 0x20, 0, 0, 0x33, 0x32, 0x31, 0x30, 0, 0, 0, 0, 0x47, 0x46,
            0x45, 0x44, 0x43, 0x42, 0x41, 0x40,
        ],
    );
    test_flattening_roundtrip(
        input,
        hlist![
            0x0000_0001_i32,
            0x0000_1011_i32,
            0x0000_2021_i32,
            0x3031_3233_i32,
            0x4041_4243_4445_4647_i64,
        ],
    );
}

/// Test storing an instance of `T` to memory, checking that the `memory_data` bytes are correctly
/// written, and check that the instance can be loaded from those bytes.
fn test_memory_roundtrip<T>(input: T, memory_data: &[u8])
where
    T: Debug + Eq + WitLoad + WitStore,
{
    let mut instance = FakeInstance::default();
    let mut memory = instance.memory().unwrap();
    let length = memory_data.len() as u32;

    assert_eq!(length, T::SIZE);

    let address = memory.allocate(length).unwrap();

    input.store(&mut memory, address).unwrap();

    assert_eq!(memory.read(address, length).unwrap(), memory_data);
    assert_eq!(T::load(&memory, address).unwrap(), input);
}

/// Test lowering an instance of `T`, checking that the resulting flat layout matches the expected
/// `flat_layout`, and check that the instance can be lifted from that flat layout.
fn test_flattening_roundtrip<T>(input: T, flat_layout: <T::Layout as Layout>::Flat)
where
    T: Debug + Eq + WitLoad + WitStore,
    <T::Layout as Layout>::Flat: Debug + Eq,
{
    let mut instance = FakeInstance::default();
    let mut memory = instance.memory().unwrap();

    let lowered_layout = input.lower(&mut memory).unwrap();

    assert_eq!(lowered_layout, flat_layout);
    assert_eq!(T::lift_from(lowered_layout, &memory).unwrap(), input);
}
