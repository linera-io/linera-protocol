// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for implementations of the custom traits for existing types.

use std::{collections::BTreeMap, fmt::Debug, time::Duration};

use frunk::hlist;

use crate::{GuestPointer, InstanceWithMemory, Layout, MockInstance, WitLoad, WitStore, WitType};

/// Test roundtrip of a heterogeneous list that doesn't need any internal padding.
#[test]
fn hlist_without_padding() {
    let input = hlist![
        0x1011_1213_1415_1617_1819_1a1b_1c1d_1e1f_u128,
        0x2021_2223_2425_2627_i64,
        0x3031_3233_u32,
        0x4041_i16,
        true,
        false,
    ];

    test_memory_roundtrip(
        &input,
        &[
            0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12,
            0x11, 0x10, 0x27, 0x26, 0x25, 0x24, 0x23, 0x22, 0x21, 0x20, 0x33, 0x32, 0x31, 0x30,
            0x41, 0x40, 0x01, 0x00,
        ],
        &[],
    );
    test_flattening_roundtrip(
        &input,
        hlist![
            0x1819_1a1b_1c1d_1e1f_i64,
            0x1011_1213_1415_1617_i64,
            0x2021_2223_2425_2627_i64,
            0x3031_3233_i32,
            0x0000_4041_i32,
            0x0000_0001_i32,
            0x0000_0000_i32,
        ],
        &[],
    );
}

/// Test roundtrip of a heterogeneous list that needs some padding at the end to align its
/// size.
#[test]
fn hlist_with_padding_at_the_end_for_size_alignment() {
    let input = hlist![0x8081_8283_8485_8687_u64, true];

    test_memory_roundtrip(
        &input,
        &[
            0x87, 0x86, 0x85, 0x84, 0x83, 0x82, 0x81, 0x80, 0x01, 0, 0, 0, 0, 0, 0, 0,
        ],
        &[],
    );
    test_flattening_roundtrip(
        &input,
        hlist![0x8081_8283_8485_8687_u64 as i64, 0x0000_0001_i32],
        &[],
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
        &input,
        &[
            0x01, 0, 0x11, 0x10, 0x21, 0x20, 0, 0, 0x33, 0x32, 0x31, 0x30, 0, 0, 0, 0, 0x47, 0x46,
            0x45, 0x44, 0x43, 0x42, 0x41, 0x40,
        ],
        &[],
    );
    test_flattening_roundtrip(
        &input,
        hlist![
            0x0000_0001_i32,
            0x0000_1011_i32,
            0x0000_2021_i32,
            0x3031_3233_i32,
            0x4041_4243_4445_4647_i64,
        ],
        &[],
    );
}

/// Test roundtrip of `None::<i8>`.
#[test]
fn none() {
    let input = None::<i8>;

    test_memory_roundtrip(&input, &[0x00, 0], &[]);
    test_flattening_roundtrip(&input, hlist![0_i32, 0_i32], &[]);
}

/// Test roundtrip of `Some::<i8>`.
#[test]
fn some_byte() {
    let input = Some(-100_i8);

    test_memory_roundtrip(&input, &[0x01, 0x9c], &[]);
    test_flattening_roundtrip(&input, hlist![1_i32, -100_i32], &[]);
}

/// Test roundtrip of `Ok::<i16, u128>`.
#[test]
fn ok_two_bytes_but_large_err() {
    let input = Ok::<_, u128>(0x1234_i16);

    assert_eq!(
        <<Result<i16, u128> as WitType>::Layout as Layout>::ALIGNMENT,
        8
    );
    test_memory_roundtrip(
        &input,
        &[
            0x00, 0, 0, 0, 0, 0, 0, 0, 0x34, 0x12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        &[],
    );
    test_flattening_roundtrip(&input, hlist![0_i32, 0x0000_1234_i64, 0_i64], &[]);
}

/// Test roundtrip of `Err::<i16, u128>`.
#[test]
fn large_err() {
    let input = Err::<i16, _>(0x0001_0203_0405_0607_0809_0a0b_0c0d_0e0f_u128);

    test_memory_roundtrip(
        &input,
        &[
            0x01, 0, 0, 0, 0, 0, 0, 0, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06,
            0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
        ],
        &[],
    );
    test_flattening_roundtrip(
        &input,
        hlist![1_i32, 0x0809_0a0b_0c0d_0e0f_i64, 0x0001_0203_0405_0607_i64],
        &[],
    );
}

/// Test roundtrip of [`Duration`].
#[test]
fn duration() {
    let seconds = 0x4aab_acad_aeaf_babb;
    let nanos = 0x3837_3635;
    let input = Duration::new(seconds, nanos);

    assert_eq!(input.as_secs(), seconds);
    assert_eq!(input.subsec_nanos(), nanos);

    assert_eq!(<<Duration as WitType>::Layout as Layout>::ALIGNMENT, 8);
    test_memory_roundtrip(
        &input,
        &[
            0xbb, 0xba, 0xaf, 0xae, 0xad, 0xac, 0xab, 0x4a, 0x35, 0x36, 0x37, 0x38, 0, 0, 0, 0,
        ],
        &[],
    );
    test_flattening_roundtrip(&input, hlist![seconds as i64, nanos as i32], &[]);
}

/// Test roundtrip of `BTreeMap<u8, i32>`.
#[test]
fn btree_map() {
    let input = [(0xaa, 0x1122_3344), (0xbb, 0x7788_99aa)]
        .into_iter()
        .collect::<BTreeMap<u8, i32>>();

    assert_eq!(
        <<BTreeMap<u8, i32> as WitType>::Layout as Layout>::ALIGNMENT,
        4
    );
    test_memory_roundtrip(
        &input,
        &[0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00],
        &[
            0xaa, 0, 0, 0, 0x44, 0x33, 0x22, 0x11, 0xbb, 0, 0, 0, 0xaa, 0x99, 0x88, 0x77,
        ],
    );
    test_flattening_roundtrip(
        &input,
        hlist![0_i32, 2_i32],
        &[
            0xaa, 0, 0, 0, 0x44, 0x33, 0x22, 0x11, 0xbb, 0, 0, 0, 0xaa, 0x99, 0x88, 0x77,
        ],
    );
}

/// Test roundtrip of [`log::Level`].
#[cfg(with_log)]
#[test]
fn log_level() {
    use log::Level::*;
    for (index, level) in [Error, Warn, Info, Debug, Trace].into_iter().enumerate() {
        test_memory_roundtrip(&level, &[index as u8], &[]);
        test_flattening_roundtrip(&level, hlist![index as i32], &[]);
    }
}

/// Test storing an instance of `T` to memory, checking that the `layout_data` bytes followed by
/// the `heap_data` bytes are correctly written, and check that the instance can be loaded from
/// those bytes.
fn test_memory_roundtrip<T>(input: &T, layout_data: &[u8], heap_data: &[u8])
where
    T: Debug + Eq + WitLoad + WitStore,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let layout_length = layout_data.len() as u32;
    let heap_length = heap_data.len() as u32;

    assert_eq!(layout_length, T::SIZE);

    let layout_address = memory
        .allocate(layout_length, <T::Layout as Layout>::ALIGNMENT)
        .unwrap();
    let heap_address = layout_address.after::<T>();

    input.store(&mut memory, layout_address).unwrap();

    assert_eq!(
        memory.read(layout_address, layout_length).unwrap(),
        layout_data
    );
    assert_eq!(memory.read(heap_address, heap_length).unwrap(), heap_data);
    assert_eq!(&T::load(&memory, layout_address).unwrap(), input);
}

/// Test lowering an instance of `T`, checking that the resulting flat layout matches the expected
/// `flat_layout`, and check that the instance can be lifted from that flat layout.
fn test_flattening_roundtrip<T>(
    input: &T,
    flat_layout: <T::Layout as Layout>::Flat,
    heap_data: &[u8],
) where
    T: Debug + Eq + WitLoad + WitStore,
    <T::Layout as Layout>::Flat: Debug + Eq,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let heap_address = GuestPointer(0);
    let heap_length = heap_data.len() as u32;

    let lowered_layout = input.lower(&mut memory).unwrap();

    assert_eq!(lowered_layout, flat_layout);
    assert_eq!(memory.read(heap_address, heap_length).unwrap(), heap_data);
    assert_eq!(&T::lift_from(lowered_layout, &memory).unwrap(), input);
}
