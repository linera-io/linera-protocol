// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitLoad` derive macro.

#[path = "common/types.rs"]
mod types;

use std::{fmt::Debug, iter, rc::Rc, sync::Arc};

use assert_matches::assert_matches;
use linera_witty::{hlist, InstanceWithMemory, Layout, MockInstance, RuntimeError, WitLoad};

use self::types::{
    Branch, Enum, Leaf, RecordWithDoublePadding, SimpleWrapper, SpecializedGenericEnum,
    SpecializedGenericStruct, StructWithHeapFields, StructWithLists, TupleWithPadding,
    TupleWithoutPadding,
};

/// Checks that a wrapper type is properly loaded from memory and lifted from its flat layout.
#[test]
fn test_simple_bool_wrapper() {
    test_load_from_memory(&[1], SimpleWrapper(true));
    test_load_from_memory(&[0], SimpleWrapper(false));

    test_lift_from_flat_layout(hlist![1], SimpleWrapper(true), &[]);
    test_lift_from_flat_layout(hlist![0], SimpleWrapper(false), &[]);
}

/// Checks that a type with multiple fields ordered in a way that doesn't require any padding is
/// properly loaded from memory and lifted from its flat layout.
#[test]
fn test_tuple_struct_without_padding() {
    let expected = TupleWithoutPadding(0x0807_0605_0403_0201_u64, 0x0c0b_0a09_i32, 0x0e0d_i16);

    test_load_from_memory(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![0x0807_0605_0403_0201_i64, 0x0c0b_0a09_i32, 0x0000_0e0d_i32],
        expected,
        &[],
    );
}

/// Checks that a type with multiple fields ordered in a way that requires padding between two of its
/// fields is properly loaded from memory and lifted from its flat layout.
#[test]
fn test_tuple_struct_with_padding() {
    let expected = TupleWithPadding(0x0201_u16, 0x0807_0605_u32, 0x100f_0e0d_0c0b_0a09_i64);

    test_load_from_memory(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![0x0000_0201_i32, 0x0807_0605_i32, 0x100f_0e0d_0c0b_0a09_i64],
        expected,
        &[],
    );
}

/// Checks that a type with multiple named fields ordered in a way that requires padding before two
/// fields is properly loaded from memory and lifted from its flat layout.
#[test]
fn test_named_struct_with_double_padding() {
    let expected = RecordWithDoublePadding {
        first: 0x0201_u16,
        second: 0x0807_0605_u32,
        third: 0x09_i8,
        fourth: 0x1817_1615_1413_1211_i64,
    };

    test_load_from_memory(
        &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        ],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![
            0x0000_0201_i32,
            0x0807_0605_i32,
            0x0000_0009_i32,
            0x1817_1615_1413_1211_i64,
        ],
        expected,
        &[],
    );
}

/// Checks that a type that contains a field with a type that also has `WitStore` derived for it is
/// properly loaded from memory and lifted from its flat layout.
#[test]
fn test_nested_types() {
    let expected = Branch {
        tag: 0x0201_u16,
        first_leaf: Leaf {
            first: true,
            second: 0x201f_1e1d_1c1b_1a19_1817_1615_1413_1211_u128,
        },
        second_leaf: Leaf {
            first: true,
            second: 0x3837_3635_3433_3231_302f_2e2d_2c2b_2a29_u128,
        },
    };

    test_load_from_memory(
        &[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
            47, 48, 49, 50, 51, 52, 53, 54, 55, 56,
        ],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![
            0x0000_0201_i32,
            0x0000_0009_i32,
            0x1817_1615_1413_1211_i64,
            0x201f_1e1d_1c1b_1a19_i64,
            0x0000_0021_i32,
            0x302f_2e2d_2c2b_2a29_i64,
            0x3837_3635_3433_3231_i64,
        ],
        expected,
        &[],
    );
}

/// Checks that an enum type's variants are properly loaded from memory and lifted from its flat
/// layout.
#[test]
fn test_enum_type() {
    let expected = Enum::Empty;

    test_load_from_memory(
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![0_i32, 0_i64, 0_i32, 0_i32, 0_i32, 0_i32, 0_i32, 0_i32, 0_i32, 0_i32, 0_i32],
        expected,
        &[],
    );

    let expected = Enum::LargeVariantWithLooseAlignment(7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

    test_load_from_memory(
        &[1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![1_i32, 7_i64, 8_i32, 9_i32, 10_i32, 11_i32, 12_i32, 13_i32, 14_i32, 15_i32, 16_i32],
        expected,
        &[],
    );

    let expected = Enum::SmallerVariantWithStrictAlignment {
        inner: 0x0e0d_0c0b_0a09_0807_u64,
    };

    test_load_from_memory(
        &[2, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        expected,
    );
    test_lift_from_flat_layout(
        hlist![
            2_i32,
            0x0e0d_0c0b_0a09_0807_i64,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32
        ],
        expected,
        &[],
    );
}

/// Checks that a generic type with a specialization request is properly loaded from memory and
/// lifted from its flat layout.
#[test]
fn test_specialized_generic_struct() {
    let expected = SpecializedGenericStruct {
        first: 254_u8,
        second: -10_i16,
        both: vec![(1, -1), (2, -2)],
    };

    test_load_from_memory(
        &[
            254, 0, 246, 255, 12, 0, 0, 0, 2, 0, 0, 0, 1, 0, 255, 255, 2, 0, 254, 255,
        ],
        expected.clone(),
    );
    test_lift_from_flat_layout(
        hlist![0x0000_00fe_i32, -10_i32, 0_i32, 2_i32,],
        expected,
        &[1, 0, 255, 255, 2, 0, 254, 255],
    );
}

/// Checks that a generic enum with a specialization request type's variants are properly loaded
/// from memory and lifted from its flat layout.
#[test]
fn test_specialized_generic_enum_type() {
    let expected = SpecializedGenericEnum::None;

    test_load_from_memory(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], expected);
    test_lift_from_flat_layout(hlist![0_i32, 0_i32, 0_i32], expected, &[]);

    let expected = SpecializedGenericEnum::First(None);

    test_load_from_memory(&[1, 2, 3, 4, 0, 5, 6, 7, 8, 9, 10, 11], expected);
    test_lift_from_flat_layout(hlist![1_i32, 0_i32, 0_i32], expected, &[]);

    let expected = SpecializedGenericEnum::First(Some(false));

    test_load_from_memory(&[1, 2, 3, 4, 1, 0, 6, 7, 8, 9, 10, 11], expected);
    test_lift_from_flat_layout(hlist![1_i32, 1_i32, 0_i32], expected, &[]);

    let expected = SpecializedGenericEnum::First(Some(true));

    test_load_from_memory(&[1, 2, 3, 4, 1, 1, 6, 7, 8, 9, 10, 11], expected);
    test_lift_from_flat_layout(hlist![1_i32, 1_i32, 1_i32], expected, &[]);

    let expected = SpecializedGenericEnum::MaybeSecond { maybe: None };

    test_load_from_memory(&[2, 3, 4, 5, 0, 6, 7, 8, 9, 10, 11, 12], expected);
    test_lift_from_flat_layout(hlist![2_i32, 0_i32, 0_i32], expected, &[]);

    let expected = SpecializedGenericEnum::MaybeSecond {
        maybe: Some(0x0c0b_0a09),
    };

    test_load_from_memory(&[2, 3, 4, 5, 1, 6, 7, 8, 9, 10, 11, 12], expected);
    test_lift_from_flat_layout(hlist![2_i32, 1_i32, 0x0c0b_0a09_i32], expected, &[]);
}

/// Checks that an invalid discriminant reports a useful error.
#[test]
fn test_invalid_discriminant() {
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let invalid_discriminant = 119_i8;

    let memory_bytes = [
        invalid_discriminant as u8,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
    ];

    let address = memory.allocate(memory_bytes.len() as u32, 1).unwrap();

    memory.write(address, &memory_bytes).unwrap();

    assert_matches!(
        &Enum::load(&memory, address),
        Err(RuntimeError::InvalidVariant {
            type_name: "wit_load::types::Enum",
            discriminant,
        }) if *discriminant == invalid_discriminant as i64
    );

    let flat_layout = hlist![
        invalid_discriminant as i32,
        0_i64,
        0_i32,
        0_i32,
        0_i32,
        0_i32,
        0_i32,
        0_i32,
        0_i32,
        0_i32,
        0_i32
    ];

    assert_matches!(
        &Enum::lift_from(flat_layout, &memory),
        Err(RuntimeError::InvalidVariant {
            type_name: "wit_load::types::Enum",
            discriminant,
        }) if *discriminant == invalid_discriminant as i64
    );
}

/// Checks that a type with fields stored in the heap is properly loaded from memory and lifted from
/// its flat layout.
#[test]
fn test_heap_allocated_fields() {
    let expected = StructWithHeapFields {
        boxed: Box::new(SimpleWrapper(true)),
        rced: Rc::new(Leaf {
            first: false,
            second: 0x201f_1e1d_1c1b_1a19_1817_1615_1413_1211_u128,
        }),
        arced: Arc::new(Enum::SmallerVariantWithStrictAlignment {
            inner: 0x2f2e_2d2c_2b2a_2928,
        }),
    };

    test_load_from_memory(
        &[
            1, 2, 3, 4, 5, 6, 7, 8, 0, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32, 2, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48,
        ],
        expected.clone(),
    );
    test_lift_from_flat_layout(
        hlist![
            1_i32,
            0_i32,
            0x1817_1615_1413_1211_i64,
            0x201f_1e1d_1c1b_1a19_i64,
            2_i32,
            0x2f2e_2d2c_2b2a_2928_i64,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
            0_i32,
        ],
        expected,
        &[],
    );
}

/// Checks that a [`Vec`] type is properly loaded from memory and lifted from its flat
/// layout.
#[test]
fn test_vec() {
    let expected = vec![SimpleWrapper(false), SimpleWrapper(true)];

    test_load_from_memory(&[8, 0, 0, 0, 2, 0, 0, 0, 0, 1], expected.clone());
    test_lift_from_flat_layout(hlist![0_i32, 2_i32], expected, &[0, 1]);
}

/// Checks that a boxed slice type is properly loaded from memory and lifted from its flat
/// layout.
#[test]
fn test_boxed_slice() {
    let expected: Box<[Enum]> = Box::new([
        Enum::Empty,
        Enum::SmallerVariantWithStrictAlignment {
            inner: 0x2726_2524_2322_2120,
        },
    ]);

    let memory = iter::empty()
        .chain([8, 0, 0, 0, 2, 0, 0, 0])
        .chain(iter::empty().chain([0]).chain(1..8).chain(8..24))
        .chain(
            iter::empty()
                .chain([2])
                .chain(25..32)
                .chain([0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27])
                .chain(40..48),
        )
        .collect::<Vec<u8>>();

    test_load_from_memory(&memory, expected.clone());
    test_lift_from_flat_layout(hlist![0_i32, 2_i32], expected, &memory[8..]);
}

/// Checks that an rc-ed slice type is properly loaded from memory and lifted from its flat
/// layout.
#[test]
fn test_rced_slice() {
    let expected: Rc<[Leaf]> = Rc::new([
        Leaf {
            first: false,
            second: 0x1716_1514_1312_1110_0f0e_0d0c_0b0a_0908,
        },
        Leaf {
            first: true,
            second: 0x2f2e_2d2c_2b2a_2928_2726_2524_2322_2120,
        },
        Leaf {
            first: false,
            second: 0x4746_4544_4342_4140_3f3e_3d3c_3b3a_3938,
        },
    ]);

    let memory = iter::empty()
        .chain([8, 0, 0, 0, 3, 0, 0, 0])
        .chain(iter::empty().chain([0]).chain(1..8).chain([
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
            0x16, 0x17,
        ]))
        .chain(iter::empty().chain([1]).chain(25..32).chain([
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d,
            0x2e, 0x2f,
        ]))
        .chain(iter::empty().chain([0]).chain(49..56).chain([
            0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45,
            0x46, 0x47,
        ]))
        .collect::<Vec<u8>>();

    test_load_from_memory(&memory, expected.clone());
    test_lift_from_flat_layout(hlist![0_i32, 3_i32], expected, &memory[8..]);
}

/// Checks that an arc-ed slice type is properly loaded from memory and lifted from its flat
/// layout.
#[test]
fn test_arced_slice() {
    let expected: Arc<[RecordWithDoublePadding]> = Arc::new([
        RecordWithDoublePadding {
            first: 0x0908,
            second: 0x0f0e_0d0c,
            third: 0x10,
            fourth: 0x1f1e_1d1c_1b1a_1918,
        },
        RecordWithDoublePadding {
            first: 0x2120,
            second: 0x2726_2524,
            third: 0x28,
            fourth: 0x3736_3534_3332_3130,
        },
    ]);

    let memory = iter::empty()
        .chain([8, 0, 0, 0, 2, 0, 0, 0])
        .chain(
            iter::empty()
                .chain([0x08, 0x09])
                .chain(10..12)
                .chain([0x0c, 0x0d, 0x0e, 0x0f])
                .chain([0x10])
                .chain(17..24)
                .chain([0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f]),
        )
        .chain(
            iter::empty()
                .chain([0x20, 0x21])
                .chain(34..36)
                .chain([0x24, 0x25, 0x26, 0x27])
                .chain([0x28])
                .chain(41..48)
                .chain([0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37]),
        )
        .collect::<Vec<u8>>();

    test_load_from_memory(&memory, expected.clone());
    test_lift_from_flat_layout(hlist![0_i32, 2_i32], expected, &memory[8..]);
}

/// Checks that a type with list fields is properly loaded from memory and lifted from its
/// flat layout.
#[test]
fn test_list_fields() {
    let expected = StructWithLists {
        vec: vec![
            SimpleWrapper(true),
            SimpleWrapper(true),
            SimpleWrapper(false),
        ],
        boxed_slice: Box::new([
            TupleWithPadding(0x2928, 0x2f2e_2d2c, 0x3736_3534_3332_3130),
            TupleWithPadding(0x3938, 0x3f3e_3d3c, 0x4746_4544_4342_4140),
        ]),
        rced_slice: Rc::new([
            Leaf {
                first: true,
                second: 0x5f5e_5d5c_5b5a_5958_5756_5554_5352_5150,
            },
            Leaf {
                first: true,
                second: 0x7776_7574_7372_7170_6f6e_6d6c_6b6a_6968,
            },
            Leaf {
                first: false,
                second: 0x8f8e_8d8c_8b8a_8988_8786_8584_8382_8180,
            },
            Leaf {
                first: false,
                second: 0xa7a6_a5a4_a3a2_a1a0_9f9e_9d9c_9b9a_9998,
            },
        ]),
        arced_slice: Arc::new([
            RecordWithDoublePadding {
                first: 0xa9a8,
                second: 0xafae_adac,
                third: 0xb0_u8 as i8,
                fourth: 0xbfbe_bdbc_bbba_b9b8_u64 as i64,
            },
            RecordWithDoublePadding {
                first: 0xc1c0,
                second: 0xc7c6_c5c4,
                third: 0xc8_u8 as i8,
                fourth: 0xd7d6_d5d4_d3d2_d1d0_u64 as i64,
            },
        ]),
    };

    let vec_metadata = [32, 0, 0, 0, 3, 0, 0, 0];
    let vec_contents = [1, 1, 0];

    let boxed_metadata = [40, 0, 0, 0, 2, 0, 0, 0];
    let boxed_contents = iter::empty()
        .chain(
            iter::empty()
                .chain([0x28, 0x29])
                .chain(42..44)
                .chain([0x2c, 0x2d, 0x2e, 0x2f])
                .chain([0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37]),
        )
        .chain(
            iter::empty()
                .chain([0x38, 0x39])
                .chain(58..60)
                .chain([0x3c, 0x3d, 0x3e, 0x3f])
                .chain([0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47]),
        );

    let rced_metadata = [72, 0, 0, 0, 4, 0, 0, 0];
    let rced_contents = iter::empty()
        .chain(iter::empty().chain([1]).chain(73..80).chain([
            0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d,
            0x5e, 0x5f,
        ]))
        .chain(iter::empty().chain([1]).chain(97..104).chain([
            0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75,
            0x76, 0x77,
        ]))
        .chain(iter::empty().chain([0]).chain(121..128).chain([
            0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d,
            0x8e, 0x8f,
        ]))
        .chain(iter::empty().chain([0]).chain(145..152).chain([
            0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5,
            0xa6, 0xa7,
        ]));

    let arced_metadata = [168, 0, 0, 0, 2, 0, 0, 0];
    let arced_contents = iter::empty()
        .chain(
            iter::empty()
                .chain([0xa8, 0xa9])
                .chain(170..172)
                .chain([0xac, 0xad, 0xae, 0xaf])
                .chain([0xb0])
                .chain(177..184)
                .chain([0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf]),
        )
        .chain(
            iter::empty()
                .chain([0xc0, 0xc1])
                .chain(194..196)
                .chain([0xc4, 0xc5, 0xc6, 0xc7])
                .chain([0xc8])
                .chain(201..208)
                .chain([0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7]),
        );

    let memory = iter::empty()
        .chain(vec_metadata)
        .chain(boxed_metadata)
        .chain(rced_metadata)
        .chain(arced_metadata)
        .chain(vec_contents)
        .chain(35..40)
        .chain(boxed_contents)
        .chain(rced_contents)
        .chain(arced_contents)
        .collect::<Vec<u8>>();

    test_load_from_memory(&memory, expected.clone());
    test_lift_from_flat_layout(
        hlist![0_i32, 3_i32, 8_i32, 2_i32, 40_i32, 4_i32, 136_i32, 2_i32],
        expected,
        &memory[32..],
    );
}

/// Tests that the type `T` and wrapped versions of it can be loaded from an `input` sequence of
/// bytes in memory and that it matches the `expected` value.
fn test_load_from_memory<T>(input: &[u8], expected: T)
where
    T: Clone + Debug + Eq + WitLoad,
{
    test_single_load_from_memory(input, &expected);
    test_single_load_from_memory(input, &Box::new(expected.clone()));
    test_single_load_from_memory(input, &Rc::new(expected.clone()));
    test_single_load_from_memory(input, &Arc::new(expected));
}

/// Tests that the type `T` can be loaded from an `input` sequence of bytes in memory and that it
/// matches the `expected` value.
fn test_single_load_from_memory<T>(input: &[u8], expected: &T)
where
    T: Debug + Eq + WitLoad,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let address = memory.allocate(input.len() as u32, 1).unwrap();

    memory.write(address, input).unwrap();

    assert_eq!(&T::load(&memory, address).unwrap(), expected);
}

/// Tests that the type `T` and wrapped versions of it can be lifted from an `input` flat layout and
/// that they match the `expected` value.
fn test_lift_from_flat_layout<T>(
    input: <T::Layout as Layout>::Flat,
    expected: T,
    initial_memory: &[u8],
) where
    T: Clone + Debug + Eq + WitLoad,
    <T::Layout as Layout>::Flat: Copy,
{
    test_single_lift_from_flat_layout(input, &expected, initial_memory);
    test_single_lift_from_flat_layout(input, &Box::new(expected.clone()), initial_memory);
    test_single_lift_from_flat_layout(input, &Rc::new(expected.clone()), initial_memory);
    test_single_lift_from_flat_layout(input, &Arc::new(expected), initial_memory);
}

/// Tests that the type `T` can be lifted from an `input` flat layout and that they match the
/// `expected` value.
fn test_single_lift_from_flat_layout<T>(
    input: <T::Layout as Layout>::Flat,
    expected: &T,
    initial_memory: &[u8],
) where
    T: Debug + Eq + WitLoad,
{
    let mut instance = MockInstance::<()>::default();
    let mut memory = instance.memory().unwrap();

    let start_address = memory.allocate(initial_memory.len() as u32, 1).unwrap();

    memory.write(start_address, initial_memory).unwrap();

    assert_eq!(&T::lift_from(input, &memory).unwrap(), expected);
}
