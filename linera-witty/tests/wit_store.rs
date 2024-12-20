// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitStore` derive macro.

#[path = "common/types.rs"]
mod types;

use std::{fmt::Debug, rc::Rc};

use linera_witty::{hlist, InstanceWithMemory, Layout, MockInstance, WitStore};

use self::types::{
    Branch, Enum, Leaf, RecordWithDoublePadding, SimpleWrapper, SpecializedGenericEnum,
    SpecializedGenericStruct, StructWithHeapFields, TupleWithPadding, TupleWithoutPadding,
};

/// Check that a wrapper type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_simple_bool_wrapper() {
    test_store_in_memory(SimpleWrapper(true), &[1], &[]);
    test_store_in_memory(SimpleWrapper(false), &[0], &[]);

    test_lower_to_flat_layout(SimpleWrapper(true), hlist![1], &[]);
    test_lower_to_flat_layout(SimpleWrapper(false), hlist![0], &[]);
}

/// Check that a type with multiple fields ordered in a way that doesn't require any padding is
/// properly stored in memory and lowered into its flat layout.
#[test]
fn test_tuple_struct_without_padding() {
    let data = TupleWithoutPadding(0x0123_4567_89ab_cdef_u64, 0x0011_2233_i32, 0x4455_i16);

    test_store_in_memory(
        data,
        &[
            0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0,
            0,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0123_4567_89ab_cdef_i64, 0x0011_2233_i32, 0x0000_4455_i32],
        &[],
    );
}

/// Check that a type with multiple fields ordered in a way that requires padding between two of its
/// fields is properly stored in memory and lowered into its flat layout.
#[test]
fn test_tuple_struct_with_padding() {
    let data = TupleWithPadding(0x0123_u16, 0x4567_89ab_u32, 0x0011_2233_4455_6677_i64);

    test_store_in_memory(
        data,
        &[
            0x23, 0x01, 0, 0, 0xab, 0x89, 0x67, 0x45, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
            0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0123_i32, 0x4567_89ab_i32, 0x0011_2233_4455_6677_i64],
        &[],
    );
}

/// Check that a type with multiple named fields ordered in a way that requires padding before two
/// fields is properly stored in memory and lowered into its flat layout.
#[test]
fn test_named_struct_with_double_padding() {
    let data = RecordWithDoublePadding {
        first: 0x0123_u16,
        second: 0x0011_2233_u32,
        third: 0x45_i8,
        fourth: 0x6789_abcd_ef44_5566_i64,
    };

    test_store_in_memory(
        data,
        &[
            0x23, 0x01, 0, 0, 0x33, 0x22, 0x11, 0x00, 0x45, 0, 0, 0, 0, 0, 0, 0, 0x66, 0x55, 0x44,
            0xef, 0xcd, 0xab, 0x89, 0x67,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0x0000_0123_i32,
            0x0011_2233_i32,
            0x0000_0045_i32,
            0x6789_abcd_ef44_5566_i64,
        ],
        &[],
    );
}

/// Check that a type that contains a field with a type that also has `WitStore` derived for it is
/// properly stored in memory and lowered into its flat layout.
#[test]
fn test_nested_types() {
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
        data,
        &[
            0x23, 0x01, 0, 0, 0, 0, 0, 0, 0x00, 0, 0, 0, 0, 0, 0, 0, 0x99, 0x88, 0x77, 0x66, 0x55,
            0x44, 0x33, 0x22, 0x11, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x01, 0, 0, 0, 0, 0,
            0, 0, 0x96, 0x87, 0x78, 0x69, 0x5a, 0x4b, 0x3c, 0x2d, 0x1e, 0x0f, 0xff, 0xee, 0xdd,
            0xcc, 0xbb, 0xaa,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0x0000_0123_i32,
            0x0000_0000_i32,
            0x2233_4455_6677_8899_i64,
            0x4567_89ab_cdef_0011_i64,
            0x0000_0001_i32,
            0x2d3c_4b5a_6978_8796_i64,
            0xaabb_ccdd_eeff_0f1e_u64 as i64,
        ],
        &[],
    );
}

/// Check that an enum type's variants are properly stored in memory and lowered into its flat
/// layout.
#[test]
fn test_enum_type() {
    let data = Enum::Empty;

    test_store_in_memory(
        data,
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0, 0, 0, 0, 0, 0,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0x0000_0000_i32,
            0x0000_0000_0000_0000_i64,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
        ],
        &[],
    );

    let data = Enum::LargeVariantWithLooseAlignment(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    test_store_in_memory(
        data,
        &[
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
            0x06, 0x07, 0x08, 0x09, 0, 0, 0, 0, 0, 0,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0x0000_0001_i32,
            0x0000_0000_0000_0000_i64,
            0x0000_0001_i32,
            0x0000_0002_i32,
            0x0000_0003_i32,
            0x0000_0004_i32,
            0x0000_0005_i32,
            0x0000_0006_i32,
            0x0000_0007_i32,
            0x0000_0008_i32,
            0x0000_0009_i32,
        ],
        &[],
    );

    let data = Enum::SmallerVariantWithStrictAlignment {
        inner: 0x0102_0304_0506_0708_u64,
    };

    test_store_in_memory(
        data,
        &[
            0x02, 0, 0, 0, 0, 0, 0, 0, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0, 0, 0, 0,
            0, 0, 0, 0,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0x0000_0002_i32,
            0x0102_0304_0506_0708_i64,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
            0x0000_0000_i32,
        ],
        &[],
    );
}

/// Check that a generic type with a specialization request is properly stored in memory and
/// lowered into its flat layout.
#[test]
fn test_specialized_generic_struct() {
    let data = SpecializedGenericStruct {
        first: 200_u8,
        second: -200_i16,
        both: vec![(4, -4), (3, -3), (2, -2), (1, -1)],
    };

    let expected_heap = [
        0x04, 0, 0xfc, 0xff, 0x03, 0, 0xfd, 0xff, 0x02, 0, 0xfe, 0xff, 0x01, 0, 0xff, 0xff,
    ];

    test_store_in_memory(
        data.clone(),
        &[
            0xc8, 0, 0x38, 0xff, 0x0c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        ],
        &expected_heap,
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_00c8_i32, -200_i32, 0x0000_0000_i32, 0x0000_0004_i32],
        &expected_heap,
    );
}

/// Check that a generic enum with a specialization request type's variants are properly stored in
/// memory and lowered into its flat layout.
#[test]
fn test_specialized_generic_enum_type() {
    let data = SpecializedGenericEnum::None;

    test_store_in_memory(
        data,
        &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0000_i32, 0x0000_0000_i32, 0x0000_0000_i32],
        &[],
    );

    let data = SpecializedGenericEnum::First(None);

    test_store_in_memory(
        data,
        &[
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0001_i32, 0x0000_0000_i32, 0x0000_0000_i32],
        &[],
    );

    let data = SpecializedGenericEnum::First(Some(false));

    test_store_in_memory(
        data,
        &[
            0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0001_i32, 0x0000_0001_i32, 0x0000_0000_i32],
        &[],
    );

    let data = SpecializedGenericEnum::First(Some(true));

    test_store_in_memory(
        data,
        &[
            0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0001_i32, 0x0000_0001_i32, 0x0000_0001_i32],
        &[],
    );

    let data = SpecializedGenericEnum::MaybeSecond { maybe: None };

    test_store_in_memory(
        data,
        &[
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0002_i32, 0x0000_0000_i32, 0x0000_0000_i32],
        &[],
    );

    let data = SpecializedGenericEnum::MaybeSecond { maybe: Some(9) };

    test_store_in_memory(
        data,
        &[
            0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![0x0000_0002_i32, 0x0000_0001_i32, 0x0000_0009_i32],
        &[],
    );
}

/// Check that a type with fields stored in the heap is properly stored in memory and lowered into
/// its flat layout.
#[test]
fn test_heap_allocated_fields() {
    let data = StructWithHeapFields {
        boxed: Box::new(SimpleWrapper(false)),
        rced: Rc::new(Leaf {
            first: true,
            second: 0x7071_7273_7475_7677_7879_7a7b_7c7d_7e7f_u128,
        }),
    };

    test_store_in_memory(
        data.clone(),
        &[
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x7e, 0x7d, 0x7c, 0x7b, 0x7a,
            0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x70,
        ],
        &[],
    );
    test_lower_to_flat_layout(
        data,
        hlist![
            0_i32,
            1_i32,
            0x7879_7a7b_7c7d_7e7f_i64,
            0x7071_7273_7475_7677_i64,
        ],
        &[],
    );
}

/// Tests that the `data` of type `T` and wrapped versions of it can be stored as a sequence of
/// bytes in memory and that they match the `expected` bytes.
fn test_store_in_memory<T>(
    data: T,
    expected_without_allocation: &[u8],
    expected_additionally_allocated: &[u8],
) where
    T: Clone + WitStore,
{
    test_single_store_in_memory(
        &data,
        expected_without_allocation,
        expected_additionally_allocated,
    );
    test_single_store_in_memory(
        &Box::new(data.clone()),
        expected_without_allocation,
        expected_additionally_allocated,
    );
    test_single_store_in_memory(
        &Rc::new(data),
        expected_without_allocation,
        expected_additionally_allocated,
    );
}

/// Tests that the `data` of type `T` can be stored as a sequence of bytes in memory and that it
/// matches the `expected` bytes.
fn test_single_store_in_memory<T>(
    data: &T,
    expected_without_allocation: &[u8],
    expected_additionally_allocated: &[u8],
) where
    T: WitStore,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let length = expected_without_allocation.len() as u32;
    let address = memory.allocate(length).unwrap();

    data.store(&mut memory, address).unwrap();

    let additional_allocations_address = address.after::<T>();
    let additional_allocations_length = expected_additionally_allocated.len() as u32;

    assert_eq!(
        memory.read(address, length).unwrap(),
        expected_without_allocation
    );
    assert_eq!(
        memory
            .read(
                additional_allocations_address,
                additional_allocations_length
            )
            .unwrap(),
        expected_additionally_allocated
    );
}

/// Tests that the `data` of type `T` and wrapped versions of it can be lowered to their flat layout
/// and that they match the `expected` value.
fn test_lower_to_flat_layout<T>(
    data: T,
    expected: <T::Layout as Layout>::Flat,
    expected_memory: &[u8],
) where
    T: Clone + WitStore,
    <T::Layout as Layout>::Flat: Copy + Debug + Eq,
{
    test_single_lower_to_flat_layout(&data, expected, expected_memory);
    test_single_lower_to_flat_layout(&Box::new(data.clone()), expected, expected_memory);
    test_single_lower_to_flat_layout(&Rc::new(data), expected, expected_memory);
}

/// Tests that the `data` of type `T` can be lowered to its flat layout and that it matches the
/// `expected` value.
fn test_single_lower_to_flat_layout<T>(
    data: &T,
    expected: <T::Layout as Layout>::Flat,
    expected_memory: &[u8],
) where
    T: WitStore,
    <T::Layout as Layout>::Flat: Debug + Eq,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    assert_eq!(data.lower(&mut memory).unwrap(), expected);
    assert_eq!(&instance.memory_contents(), expected_memory);
}
