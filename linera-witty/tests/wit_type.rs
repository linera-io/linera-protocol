// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitType` derive macro.

#[path = "common/types.rs"]
mod types;

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use linera_witty::{HList, Layout, RegisterWitTypes, WitType};

use self::types::{
    Branch, Enum, Leaf, RecordWithDoublePadding, SimpleWrapper, SliceWrapper,
    SpecializedGenericEnum, SpecializedGenericStruct, StructWithHeapFields, StructWithLists,
    TupleWithPadding, TupleWithoutPadding,
};

/// Checks the memory size, layout and WIT type declaration derived for a wrapper type.
#[test]
fn test_simple_bool_wrapper() {
    test_wit_type_implementation::<SimpleWrapper>(ExpectedMetadata {
        size: 1,
        alignment: 1,
        flat_layout_length: 1,
        declaration: concat!(
            "    record simple-wrapper {\n",
            "        inner0: bool,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for a type with multiple fields
/// ordered in a way that doesn't require any padding.
#[test]
fn test_tuple_struct_without_padding() {
    test_wit_type_implementation::<TupleWithoutPadding>(ExpectedMetadata {
        size: 16,
        alignment: 8,
        flat_layout_length: 3,
        declaration: concat!(
            "    record tuple-without-padding {\n",
            "        inner0: u64,\n",
            "        inner1: s32,\n",
            "        inner2: s16,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for a type with multiple fields
/// ordered in a way that requires padding between all fields.
#[test]
fn test_tuple_struct_with_padding() {
    test_wit_type_implementation::<TupleWithPadding>(ExpectedMetadata {
        size: 16,
        alignment: 8,
        flat_layout_length: 3,
        declaration: concat!(
            "    record tuple-with-padding {\n",
            "        inner0: u16,\n",
            "        inner1: u32,\n",
            "        inner2: s64,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for a type with multiple named
/// fields ordered in a way that requires padding before two fields.
#[test]
fn test_named_struct_with_double_padding() {
    test_wit_type_implementation::<RecordWithDoublePadding>(ExpectedMetadata {
        size: 24,
        alignment: 8,
        flat_layout_length: 4,
        declaration: concat!(
            "    record record-with-double-padding {\n",
            "        first: u16,\n",
            "        second: u32,\n",
            "        third: s8,\n",
            "        fourth: s64,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT type declarations derived for a type that contains a
/// field with a type that also has `WitType` derived for it.
#[test]
fn test_nested_types() {
    test_wit_type_implementation::<Leaf>(ExpectedMetadata {
        size: 24,
        alignment: 8,
        flat_layout_length: 3,
        declaration: concat!(
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n"
        ),
    });

    test_wit_type_implementation::<Branch>(ExpectedMetadata {
        size: 56,
        alignment: 8,
        flat_layout_length: 7,
        declaration: concat!(
            "    record branch {\n",
            "        tag: u16,\n",
            "        first-leaf: leaf,\n",
            "        second-leaf: leaf,\n",
            "    }\n\n",
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n"
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for an `enum` type.
#[test]
fn test_enum_type() {
    test_wit_type_implementation::<Enum>(ExpectedMetadata {
        size: 24,
        alignment: 8,
        flat_layout_length: 11,
        declaration: concat!(
            "    variant enum {\n",
            "        empty,\n",
            "        large-variant-with-loose-alignment(\
                        tuple<s8, s8, s8, s8, s8, s8, s8, s8, s8, s8>\
                     ),\n",
            "        smaller-variant-with-strict-alignment(u64),\n",
            "    }\n",
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for a specialized generic
/// `struct` type.
#[test]
fn test_specialized_generic_struct() {
    test_wit_type_implementation::<SpecializedGenericStruct<u8, i16>>(ExpectedMetadata {
        size: 12,
        alignment: 4,
        flat_layout_length: 4,
        declaration: concat!(
            "    record specialized-generic-struct {\n",
            "        first: u8,\n",
            "        second: s16,\n",
            "        both: list<tuple<u8, s16>>,\n",
            "    }\n",
        ),
    });
}

/// Checks the memory size, layout and WIT type declaration derived for a specialized generic `enum`
/// type.
#[test]
fn test_specialized_generic_enum_type() {
    test_wit_type_implementation::<SpecializedGenericEnum<Option<bool>, u32>>(ExpectedMetadata {
        size: 12,
        alignment: 4,
        flat_layout_length: 3,
        declaration: concat!(
            "    variant specialized-generic-enum {\n",
            "        none,\n",
            "        first(option<bool>),\n",
            "        maybe-second(option<u32>),\n",
            "    }\n",
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for a complex type that has heap
/// allocated fields.
#[test]
fn test_heap_allocated_fields() {
    test_wit_type_implementation::<StructWithHeapFields>(ExpectedMetadata {
        size: 56,
        alignment: 8,
        flat_layout_length: 15,
        declaration: concat!(
            "    variant enum {\n",
            "        empty,\n",
            "        large-variant-with-loose-alignment(\
                        tuple<s8, s8, s8, s8, s8, s8, s8, s8, s8, s8>\
                     ),\n",
            "        smaller-variant-with-strict-alignment(u64),\n",
            "    }\n\n",
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    record simple-wrapper {\n",
            "        inner0: bool,\n",
            "    }\n\n",
            "    record struct-with-heap-fields {\n",
            "        boxed: simple-wrapper,\n",
            "        rced: leaf,\n",
            "        arced: enum,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n",
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for a slice type.
#[test]
fn test_slice() {
    test_wit_type_implementation::<[SimpleWrapper]>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record simple-wrapper {\n",
            "        inner0: bool,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for a [`Vec`] type.
#[test]
fn test_vec() {
    test_wit_type_implementation::<Vec<SimpleWrapper>>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record simple-wrapper {\n",
            "        inner0: bool,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for a boxed slice type.
#[test]
fn test_boxed_slice() {
    test_wit_type_implementation::<Box<[TupleWithPadding]>>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record tuple-with-padding {\n",
            "        inner0: u16,\n",
            "        inner1: u32,\n",
            "        inner2: s64,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for an rc-ed slice type.
#[test]
fn test_rced_slice() {
    test_wit_type_implementation::<Rc<[Leaf]>>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n"
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for an arc-ed slice type.
#[test]
fn test_arced_slice() {
    test_wit_type_implementation::<Arc<[RecordWithDoublePadding]>>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record record-with-double-padding {\n",
            "        first: u16,\n",
            "        second: u32,\n",
            "        third: s8,\n",
            "        fourth: s64,\n",
            "    }\n"
        ),
    });
}

/// Check the memory size, layout and WIT declaration derived for a type that has a slice
/// field.
#[test]
fn test_slice_field() {
    test_wit_type_implementation::<SliceWrapper>(ExpectedMetadata {
        size: 8,
        alignment: 4,
        flat_layout_length: 2,
        declaration: concat!(
            "    record slice-wrapper {\n",
            "        inner0: list<tuple-without-padding>,\n",
            "    }\n\n",
            "    record tuple-without-padding {\n",
            "        inner0: u64,\n",
            "        inner1: s32,\n",
            "        inner2: s16,\n",
            "    }\n"
        ),
    });
}

/// Checks the memory size, layout and WIT declaration derived for a type that has list
/// fields.
#[test]
fn test_list_fields() {
    test_wit_type_implementation::<StructWithLists>(ExpectedMetadata {
        size: 32,
        alignment: 4,
        flat_layout_length: 8,
        declaration: concat!(
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    record record-with-double-padding {\n",
            "        first: u16,\n",
            "        second: u32,\n",
            "        third: s8,\n",
            "        fourth: s64,\n",
            "    }\n\n",
            "    record simple-wrapper {\n",
            "        inner0: bool,\n",
            "    }\n\n",
            "    record struct-with-lists {\n",
            "        vec: list<simple-wrapper>,\n",
            "        boxed-slice: list<tuple-with-padding>,\n",
            "        rced-slice: list<leaf>,\n",
            "        arced-slice: list<record-with-double-padding>,\n",
            "    }\n\n",
            "    record tuple-with-padding {\n",
            "        inner0: u16,\n",
            "        inner1: u32,\n",
            "        inner2: s64,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n"
        ),
    });
}

/// Helper type to make visible what each metadata value is.
#[derive(Clone, Copy, Debug)]
struct ExpectedMetadata {
    size: u32,
    alignment: u32,
    flat_layout_length: usize,
    declaration: &'static str,
}

/// Tests that a type `T` and wrapped versions of it have the `expected` [`WitType`] metadata in
/// their implementations.
fn test_wit_type_implementation<T>(expected: ExpectedMetadata)
where
    T: WitType + ?Sized + 'static,
    Box<T>: WitType,
    Rc<T>: WitType,
    Arc<T>: WitType,
{
    test_single_wit_type_implementation::<&'static T>(expected);
    test_single_wit_type_implementation::<Box<T>>(expected);
    test_single_wit_type_implementation::<Rc<T>>(expected);
    test_single_wit_type_implementation::<Arc<T>>(expected);
}

/// Tests that a type `T` has the `expected` [`WitType`] metadata in its implementation.
fn test_single_wit_type_implementation<T>(expected: ExpectedMetadata)
where
    T: WitType,
{
    assert_eq!(T::SIZE, expected.size);
    assert_eq!(<T as WitType>::Layout::ALIGNMENT, expected.alignment);
    assert_eq!(
        <<T as WitType>::Layout as Layout>::Flat::LEN,
        expected.flat_layout_length
    );
    assert_eq!(wit_type_declaration_of::<T>(), expected.declaration);
}

/// Returns the WIT snippet with the type declarations for `T`.
fn wit_type_declaration_of<T>() -> String
where
    T: WitType,
{
    let mut wit_types = BTreeMap::new();

    <HList![T] as RegisterWitTypes>::register_wit_types(&mut wit_types);

    wit_types
        .into_values()
        .filter(|declaration| !declaration.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}
