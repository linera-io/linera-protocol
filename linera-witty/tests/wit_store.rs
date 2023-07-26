// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitStore` derive macro.

#[path = "common/types.rs"]
mod types;

use self::types::{
    Branch, Leaf, RecordWithDoublePadding, SimpleWrapper, TupleWithPadding, TupleWithoutPadding,
};
use linera_witty::{hlist, FakeInstance, InstanceWithMemory, Layout, WitStore};
use std::fmt::Debug;

/// Check that a wrapper type is properly stored in memory and lowered into its flat layout.
#[test]
fn simple_bool_wrapper() {
    test_store_in_memory(&SimpleWrapper(true), &[1]);
    test_store_in_memory(&SimpleWrapper(false), &[0]);

    test_lower_to_flat_layout(&SimpleWrapper(true), hlist![1]);
    test_lower_to_flat_layout(&SimpleWrapper(false), hlist![0]);
}

/// Check that a type with multiple fields ordered in a way that doesn't require any padding is
/// properly stored in memory and lowered into its flat layout.
#[test]
fn tuple_struct_without_padding() {
    let data = TupleWithoutPadding(0x0123_4567_89ab_cdef_u64, 0x0011_2233_i32, 0x4455_i16);

    test_store_in_memory(
        &data,
        &[
            0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x33, 0x22, 0x11, 0x00, 0x55, 0x44,
        ],
    );
    test_lower_to_flat_layout(
        &data,
        hlist![0x0123_4567_89ab_cdef_i64, 0x0011_2233_i32, 0x0000_4455_i32],
    );
}

/// Check that a type with multiple fields ordered in a way that requires padding between two of its
/// fields is properly stored in memory and lowered into its flat layout.
#[test]
fn tuple_struct_with_padding() {
    let data = TupleWithPadding(0x0123_u16, 0x4567_89ab_u32, 0x0011_2233_4455_6677_i64);

    test_store_in_memory(
        &data,
        &[
            0x23, 0x01, 0, 0, 0xab, 0x89, 0x67, 0x45, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
            0x00,
        ],
    );
    test_lower_to_flat_layout(
        &data,
        hlist![0x0000_0123_i32, 0x4567_89ab_i32, 0x0011_2233_4455_6677_i64],
    );
}

/// Check that a type with multiple named fields ordered in a way that requires padding before two
/// fields is properly stored in memory and lowered into its flat layout.
#[test]
fn named_struct_with_double_padding() {
    let data = RecordWithDoublePadding {
        first: 0x0123_u16,
        second: 0x0011_2233_u32,
        third: 0x45_i8,
        fourth: 0x6789_abcd_ef44_5566_i64,
    };

    test_store_in_memory(
        &data,
        &[
            0x23, 0x01, 0, 0, 0x33, 0x22, 0x11, 0x00, 0x45, 0, 0, 0, 0, 0, 0, 0, 0x66, 0x55, 0x44,
            0xef, 0xcd, 0xab, 0x89, 0x67,
        ],
    );
    test_lower_to_flat_layout(
        &data,
        hlist![
            0x0000_0123_i32,
            0x0011_2233_i32,
            0x0000_0045_i32,
            0x6789_abcd_ef44_5566_i64,
        ],
    );
}

/// Check that a type that contains a field with a type that also has `WitStore` derived for it is
/// properly stored in memory and lowered into its flat layout.
#[test]
fn nested_types() {
    let data = Branch {
        tag: 0x0123_u16,
        first_leaf: Leaf {
            first: false,
            second: 0x4567_89ab_cdef_0011_2233_4455_6677_8899_u128,
        },
        second_leaf: Leaf {
            first: true,
            second: 0xaabb_ccdd_eeff_0f1e_2d3c_4b5a_6978_8796_u128,
        },
    };

    test_store_in_memory(
        &data,
        &[
            0x23, 0x01, 0, 0, 0, 0, 0, 0, 0x00, 0, 0, 0, 0, 0, 0, 0, 0x99, 0x88, 0x77, 0x66, 0x55,
            0x44, 0x33, 0x22, 0x11, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x01, 0, 0, 0, 0, 0,
            0, 0, 0x96, 0x87, 0x78, 0x69, 0x5a, 0x4b, 0x3c, 0x2d, 0x1e, 0x0f, 0xff, 0xee, 0xdd,
            0xcc, 0xbb, 0xaa,
        ],
    );
    test_lower_to_flat_layout(
        &data,
        hlist![
            0x0000_0123_i32,
            0x0000_0000_i32,
            0x2233_4455_6677_8899_i64,
            0x4567_89ab_cdef_0011_i64,
            0x0000_0001_i32,
            0x2d3c_4b5a_6978_8796_i64,
            0xaabb_ccdd_eeff_0f1e_u64 as i64,
        ],
    );
}

/// Tests that the `data` of type `T` can be stored as a sequence of bytes in memory and that it
/// matches the `expected` bytes.
fn test_store_in_memory<T>(data: &T, expected: &[u8])
where
    T: WitStore,
{
    let mut instance = FakeInstance::default();
    let mut memory = instance.memory().unwrap();

    let length = expected.len() as u32;
    let address = memory.allocate(length).unwrap();

    data.store(&mut memory, address).unwrap();

    assert_eq!(memory.read(address, length).unwrap(), expected);
}

/// Tests that the `data` of type `T` can be lowered to its flat layout and that it matches the
/// `expected` value.
fn test_lower_to_flat_layout<T>(data: &T, expected: <T::Layout as Layout>::Flat)
where
    T: WitStore,
    <T::Layout as Layout>::Flat: Debug + Eq,
{
    let mut instance = FakeInstance::default();
    let mut memory = instance.memory().unwrap();

    assert_eq!(data.lower(&mut memory).unwrap(), expected);
}
