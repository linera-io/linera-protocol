// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitStore` derive macro.

#[path = "common/types.rs"]
mod types;

use std::{fmt::Debug, iter, rc::Rc, sync::Arc};

use linera_witty::{hlist, InstanceWithMemory, Layout, MockInstance, WitStore};

use self::types::{
    Branch, Enum, Leaf, RecordWithDoublePadding, SimpleWrapper, SliceWrapper,
    SpecializedGenericEnum, SpecializedGenericStruct, StructWithHeapFields, StructWithLists,
    TupleWithPadding, TupleWithoutPadding,
};

/// Checks that a wrapper type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_simple_bool_wrapper() {
    test_store_in_memory(SimpleWrapper(true), &[1], &[]);
    test_store_in_memory(SimpleWrapper(false), &[0], &[]);

    test_lower_to_flat_layout(SimpleWrapper(true), hlist![1], &[]);
    test_lower_to_flat_layout(SimpleWrapper(false), hlist![0], &[]);
}

/// Checks that a type with multiple fields ordered in a way that doesn't require any padding is
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

/// Checks that a type with multiple fields ordered in a way that requires padding between two of its
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

/// Checks that a type with multiple named fields ordered in a way that requires padding before two
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

/// Checks that a type that contains a field with a type that also has `WitStore` derived for it is
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

/// Checks that an enum type's variants are properly stored in memory and lowered into its flat
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

/// Checks that a generic type with a specialization request is properly stored in memory and
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

/// Checks that a generic enum with a specialization request type's variants are properly stored in
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

/// Checks that a type with fields stored in the heap is properly stored in memory and lowered into
/// its flat layout.
#[test]
fn test_heap_allocated_fields() {
    let data = StructWithHeapFields {
        boxed: Box::new(SimpleWrapper(false)),
        rced: Rc::new(Leaf {
            first: true,
            second: 0x7071_7273_7475_7677_7879_7a7b_7c7d_7e7f_u128,
        }),
        arced: Arc::new(Enum::SmallerVariantWithStrictAlignment {
            inner: 0x4041_4243_4445_4647_u64,
        }),
    };

    test_store_in_memory(
        data.clone(),
        &[
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0x7e, 0x7d, 0x7c, 0x7b, 0x7a,
            0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x72, 0x71, 0x70, 2, 0, 0, 0, 0, 0, 0, 0,
            0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41, 0x40, 0, 0, 0, 0, 0, 0, 0, 0,
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
            2_i32,
            0x4041_4243_4445_4647_i64,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
        &[],
    );
}

/// Check that a slice type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_slice() {
    let data = [
        SimpleWrapper(false),
        SimpleWrapper(false),
        SimpleWrapper(true),
        SimpleWrapper(true),
    ];

    test_store_in_memory(data.as_slice(), &[8, 0, 0, 0, 4, 0, 0, 0], &[0, 0, 1, 1]);
    test_lower_to_flat_layout(data.as_slice(), hlist![0_i32, 4_i32,], &[0, 0, 1, 1]);
}

/// Checks that a [`Vec`] type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_vec() {
    let data = vec![
        SimpleWrapper(true),
        SimpleWrapper(false),
        SimpleWrapper(true),
    ];

    test_store_in_memory(data.clone(), &[8, 0, 0, 0, 3, 0, 0, 0], &[1, 0, 1]);
    test_lower_to_flat_layout(data, hlist![0_i32, 3_i32,], &[1, 0, 1]);
}

/// Check that a boxed slice type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_boxed_slice() {
    let data: Box<[Enum]> = Box::new([
        Enum::LargeVariantWithLooseAlignment(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
        Enum::Empty,
        Enum::Empty,
        Enum::SmallerVariantWithStrictAlignment { inner: 0xFFFF_FFFF },
    ]);

    let heap_memory = iter::empty()
        .chain(
            iter::empty()
                .chain([1])
                .chain([0; 7])
                .chain([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
                .chain([0; 6]),
        )
        .chain(iter::empty().chain([0]).chain([0; 23]))
        .chain(iter::empty().chain([0]).chain([0; 23]))
        .chain(
            iter::empty()
                .chain([2])
                .chain([0; 7])
                .chain([0xff, 0xff, 0xff, 0xff])
                .chain([0; 12]),
        )
        .collect::<Vec<u8>>();

    test_store_in_memory(data.clone(), &[8, 0, 0, 0, 4, 0, 0, 0], &heap_memory);
    test_lower_to_flat_layout(data, hlist![0_i32, 4_i32,], &heap_memory);
}

/// Check that a rc-ed slice type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_rced_slice() {
    let data: Rc<[Leaf]> = Rc::new([
        Leaf {
            first: true,
            second: 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff,
        },
        Leaf {
            first: false,
            second: 0xffee_ddcc_bbaa_9988_7766_5544_3322_1100,
        },
    ]);

    let heap_memory = iter::empty()
        .chain(iter::empty().chain([1]).chain([0; 7]).chain([
            0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22,
            0x11, 0x00,
        ]))
        .chain(iter::empty().chain([0]).chain([0; 7]).chain([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]))
        .collect::<Vec<u8>>();

    test_store_in_memory(data.clone(), &[8, 0, 0, 0, 2, 0, 0, 0], &heap_memory);
    test_lower_to_flat_layout(data, hlist![0_i32, 2_i32,], &heap_memory);
}

/// Check that a rc-ed slice type is properly stored in memory and lowered into its flat layout.
#[test]
fn test_arced_slice() {
    let data: Arc<[RecordWithDoublePadding]> = Arc::new([
        RecordWithDoublePadding {
            first: 0x0300,
            second: 0x4422_1100,
            third: -2,
            fourth: -3,
        },
        RecordWithDoublePadding {
            first: 32_767,
            second: 9,
            third: 127,
            fourth: -32_768,
        },
    ]);

    let heap_memory = iter::empty()
        .chain(
            iter::empty()
                .chain([0x00, 0x03])
                .chain([0; 2])
                .chain([0x00, 0x11, 0x22, 0x44])
                .chain([0xfe])
                .chain([0; 7])
                .chain([0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        )
        .chain(
            iter::empty()
                .chain([0xff, 0x7f])
                .chain([0; 2])
                .chain([9, 0, 0, 0])
                .chain([0x7f])
                .chain([0; 7])
                .chain([0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        )
        .collect::<Vec<u8>>();

    test_store_in_memory(data.clone(), &[8, 0, 0, 0, 2, 0, 0, 0], &heap_memory);
    test_lower_to_flat_layout(data, hlist![0_i32, 2_i32,], &heap_memory);
}

/// Check that a type with a slice field is properly stored in memory and lowered into its
/// flat layout.
#[test]
fn test_slice_field() {
    let slice = [
        TupleWithoutPadding(0, 1, 2),
        TupleWithoutPadding(3, 4, 5),
        TupleWithoutPadding(6, 7, 8),
    ];
    let data = SliceWrapper(&slice);

    let expected_memory = iter::empty()
        .chain(
            iter::empty()
                .chain([0, 0, 0, 0, 0, 0, 0, 0])
                .chain([1, 0, 0, 0])
                .chain([2, 0])
                .chain([0; 2]),
        )
        .chain(
            iter::empty()
                .chain([3, 0, 0, 0, 0, 0, 0, 0])
                .chain([4, 0, 0, 0])
                .chain([5, 0])
                .chain([0; 2]),
        )
        .chain(
            iter::empty()
                .chain([6, 0, 0, 0, 0, 0, 0, 0])
                .chain([7, 0, 0, 0])
                .chain([8, 0])
                .chain([0; 2]),
        )
        .collect::<Vec<u8>>();

    test_store_in_memory(data, &[8, 0, 0, 0, 3, 0, 0, 0], &expected_memory);
    test_lower_to_flat_layout(data, hlist![0_i32, 3_i32], &expected_memory);
}

/// Checks that a type with list fields is properly stored in memory and lowered into its
/// flat layout.
#[test]
fn test_list_fields() {
    let data = StructWithLists {
        vec: vec![
            SimpleWrapper(false),
            SimpleWrapper(true),
            SimpleWrapper(false),
            SimpleWrapper(true),
        ],
        boxed_slice: Box::new([TupleWithPadding(1, 0, -1), TupleWithPadding(10, 11, 12)]),
        rced_slice: Rc::new([
            Leaf {
                first: true,
                second: 0x0011_2233_4455_6677_8899_aabb_ccdd_eeff,
            },
            Leaf {
                first: false,
                second: 0xffee_ddcc_bbaa_9988_7766_5544_3322_1100,
            },
            Leaf {
                first: false,
                second: 0xf0e1_d2c3_b4a5_9687_7869_5a4b_3c2d_1e0f,
            },
        ]),
        arced_slice: Arc::new([
            RecordWithDoublePadding {
                first: 0x1020,
                second: 0x0a0b_0c0d,
                third: 0x7a,
                fourth: -0x0abb_ccdd_eeff_0011,
            },
            RecordWithDoublePadding {
                first: 0x1525,
                second: 0x8191_a1b1,
                third: -0x7a,
                fourth: 0x0abb_ccdd_eeff_0011,
            },
        ]),
    };

    let vec_contents = [0, 1, 0, 1];

    let boxed_contents = iter::empty()
        .chain(
            iter::empty()
                .chain([1, 0])
                .chain([0; 2])
                .chain([0, 0, 0, 0])
                .chain([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        )
        .chain(
            iter::empty()
                .chain([10, 0])
                .chain([0; 2])
                .chain([11, 0, 0, 0])
                .chain([12, 0, 0, 0, 0, 0, 0, 0]),
        );

    let rced_contents = iter::empty()
        .chain(iter::empty().chain([1]).chain([0; 7]).chain([
            0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22,
            0x11, 0x00,
        ]))
        .chain(iter::empty().chain([0]).chain([0; 7]).chain([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]))
        .chain(iter::empty().chain([0]).chain([0; 7]).chain([
            0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78, 0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2,
            0xe1, 0xf0,
        ]));

    let arced_contents = iter::empty()
        .chain(
            iter::empty()
                .chain([0x20, 0x10])
                .chain([0; 2])
                .chain([0x0d, 0x0c, 0x0b, 0x0a])
                .chain([0x7a])
                .chain([0; 7])
                .chain([0xef, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0xf5]),
        )
        .chain(
            iter::empty()
                .chain([0x25, 0x15])
                .chain([0; 2])
                .chain([0xb1, 0xa1, 0x91, 0x81])
                .chain([0x86])
                .chain([0; 7])
                .chain([0x11, 0x00, 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0x0a]),
        );

    let expected_heap = iter::empty()
        .chain(vec_contents)
        .chain([0; 4])
        .chain(boxed_contents)
        .chain(rced_contents)
        .chain(arced_contents)
        .collect::<Vec<_>>();

    test_store_in_memory(
        data.clone(),
        &[
            32, 0, 0, 0, 4, 0, 0, 0, 40, 0, 0, 0, 2, 0, 0, 0, 72, 0, 0, 0, 3, 0, 0, 0, 144, 0, 0,
            0, 2, 0, 0, 0,
        ],
        &expected_heap,
    );
    test_lower_to_flat_layout(
        data,
        hlist![0_i32, 4_i32, 8_i32, 2_i32, 40_i32, 3_i32, 112_i32, 2_i32],
        &expected_heap,
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
    let address = memory.allocate(length, 1).unwrap();

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
    test_single_lower_to_flat_layout(&Rc::new(data.clone()), expected, expected_memory);
    test_single_lower_to_flat_layout(&Arc::new(data), expected, expected_memory);
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
