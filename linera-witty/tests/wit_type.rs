// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitType` derive macro.

#[path = "common/types.rs"]
mod types;

use std::collections::{BTreeMap, HashMap};

use linera_witty::{HList, Layout, RegisterWitTypes, WitType};

use self::types::{
    Branch, Enum, Leaf, RecordWithDoublePadding, SimpleWrapper, SpecializedGenericEnum,
    SpecializedGenericStruct, TupleWithPadding, TupleWithoutPadding,
};

/// Check the memory size, layout and WIT type declaration derived for a wrapper type.
#[test]
fn test_simple_bool_wrapper() {
    assert_eq!(SimpleWrapper::SIZE, 1);
    assert_eq!(<SimpleWrapper as WitType>::Layout::ALIGNMENT, 1);
    assert_eq!(<<SimpleWrapper as WitType>::Layout as Layout>::Flat::LEN, 1);
    assert_eq!(
        wit_type_declaration_of::<SimpleWrapper>(),
        "    record simple-wrapper {\n        inner0: bool,\n    }\n"
    );
}

/// Check the memory size, layout and WIT type declaration derived for a type with multiple fields
/// ordered in a way that doesn't require any padding.
#[test]
fn test_tuple_struct_without_padding() {
    assert_eq!(TupleWithoutPadding::SIZE, 16);
    assert_eq!(<TupleWithoutPadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<TupleWithoutPadding as WitType>::Layout as Layout>::Flat::LEN,
        3
    );
    assert_eq!(
        wit_type_declaration_of::<TupleWithoutPadding>(),
        concat!(
            "    record tuple-without-padding {\n",
            "        inner0: u64,\n",
            "        inner1: s32,\n",
            "        inner2: s16,\n",
            "    }\n"
        )
    );
}

/// Check the memory size, layout and WIT type declaration derived for a type with multiple fields
/// ordered in a way that requires padding between all fields.
#[test]
fn test_tuple_struct_with_padding() {
    assert_eq!(TupleWithPadding::SIZE, 16);
    assert_eq!(<TupleWithPadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<TupleWithPadding as WitType>::Layout as Layout>::Flat::LEN,
        3
    );
    assert_eq!(
        wit_type_declaration_of::<TupleWithPadding>(),
        concat!(
            "    record tuple-with-padding {\n",
            "        inner0: u16,\n",
            "        inner1: u32,\n",
            "        inner2: s64,\n",
            "    }\n"
        )
    );
}

/// Check the memory size, layout and WIT type declaration derived for a type with multiple named
/// fields ordered in a way that requires padding before two fields.
#[test]
fn test_named_struct_with_double_padding() {
    assert_eq!(RecordWithDoublePadding::SIZE, 24);
    assert_eq!(<RecordWithDoublePadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<RecordWithDoublePadding as WitType>::Layout as Layout>::Flat::LEN,
        4
    );
    assert_eq!(
        wit_type_declaration_of::<RecordWithDoublePadding>(),
        concat!(
            "    record record-with-double-padding {\n",
            "        first: u16,\n",
            "        second: u32,\n",
            "        third: s8,\n",
            "        fourth: s64,\n",
            "    }\n"
        )
    );
}

/// Check the memory size, layout and WIT type declarations derived for a type that contains a
/// field with a type that also has `WitType` derived for it.
#[test]
fn test_nested_types() {
    assert_eq!(Leaf::SIZE, 24);
    assert_eq!(<Leaf as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Leaf as WitType>::Layout as Layout>::Flat::LEN, 3);
    assert_eq!(
        wit_type_declaration_of::<Leaf>(),
        concat!(
            "    record leaf {\n",
            "        first: bool,\n",
            "        second: u128,\n",
            "    }\n\n",
            "    type u128 = tuple<u64, u64>;\n"
        )
    );

    assert_eq!(Branch::SIZE, 56);
    assert_eq!(<Branch as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Branch as WitType>::Layout as Layout>::Flat::LEN, 7);
    assert_eq!(
        wit_type_declaration_of::<Branch>(),
        concat!(
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
        )
    );
}

/// Check the memory size, layout and WIT type declaration derived for an `enum` type.
#[test]
fn test_enum_type() {
    assert_eq!(Enum::SIZE, 24);
    assert_eq!(<Enum as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Enum as WitType>::Layout as Layout>::Flat::LEN, 11);
    assert_eq!(
        wit_type_declaration_of::<Enum>(),
        concat!(
            "    variant enum {\n",
            "        empty,\n",
            "        large-variant-with-loose-alignment(\
                        tuple<s8, s8, s8, s8, s8, s8, s8, s8, s8, s8>\
                     ),\n",
            "        smaller-variant-with-strict-alignment(u64),\n",
            "    }\n",
        )
    );
}

/// Check the memory size, layout and WIT type declaration derived for a specialized generic
/// `struct` type.
#[test]
fn test_specialized_generic_struct() {
    assert_eq!(SpecializedGenericStruct::SIZE, 12);
    assert_eq!(
        <SpecializedGenericStruct<u8, i16> as WitType>::Layout::ALIGNMENT,
        4
    );
    assert_eq!(
        <<SpecializedGenericStruct<u8, i16> as WitType>::Layout as Layout>::Flat::LEN,
        4
    );
    assert_eq!(
        wit_type_declaration_of::<SpecializedGenericStruct<u8, i16>>(),
        concat!(
            "    record specialized-generic-struct {\n",
            "        first: u8,\n",
            "        second: s16,\n",
            "        both: list<tuple<u8, s16>>,\n",
            "    }\n",
        )
    );
}

/// Check the memory size, layout and WIT type declaration derived for a specialized generic `enum`
/// type.
#[test]
fn test_specialized_generic_enum_type() {
    assert_eq!(SpecializedGenericEnum::SIZE, 12);
    assert_eq!(
        <SpecializedGenericEnum<Option<bool>, u32> as WitType>::Layout::ALIGNMENT,
        4
    );
    assert_eq!(
        <<SpecializedGenericEnum<Option<bool>, u32> as WitType>::Layout as Layout>::Flat::LEN,
        3
    );
    assert_eq!(
        wit_type_declaration_of::<SpecializedGenericEnum<Option<bool>, u32>>(),
        concat!(
            "    variant specialized-generic-enum {\n",
            "        none,\n",
            "        first(option<bool>),\n",
            "        maybe-second(option<u32>),\n",
            "    }\n",
        )
    );
}

/// Returns the WIT snippet with the type declarations for `T`.
fn wit_type_declaration_of<T>() -> String
where
    T: WitType,
{
    let mut wit_types = HashMap::new();

    <HList![T] as RegisterWitTypes>::register_wit_types(&mut wit_types);

    let sorted_wit_types = wit_types
        .into_iter()
        .filter(|(_, declaration)| !declaration.is_empty())
        .collect::<BTreeMap<_, _>>();

    sorted_wit_types
        .into_values()
        .collect::<Vec<_>>()
        .join("\n")
}
