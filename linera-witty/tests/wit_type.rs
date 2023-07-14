// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitType` derive macro.

use linera_witty::{HList, Layout, WitType};

/// Check the memory size and layout derived for a wrapper type.
#[test]
fn simple_bool_wrapper() {
    #[derive(WitType)]
    struct MyWrapper(bool);

    assert_eq!(MyWrapper::SIZE, 1);
    assert_eq!(<MyWrapper as WitType>::Layout::ALIGNMENT, 1);
    assert_eq!(<<MyWrapper as WitType>::Layout as Layout>::Flat::LEN, 1);
}

/// Check the memory size and layout derived for a type with multiple fields ordered in a way that
/// doesn't require any padding.
#[test]
fn tuple_struct_without_padding() {
    #[derive(WitType)]
    struct TupleWithoutPadding(u64, i32, i16);

    assert_eq!(TupleWithoutPadding::SIZE, 14);
    assert_eq!(<TupleWithoutPadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<TupleWithoutPadding as WitType>::Layout as Layout>::Flat::LEN,
        3
    );
}

/// Check the memory size and layout derived for a type with multiple fields ordered in a way that
/// requires padding between all fields.
#[test]
fn tuple_struct_with_padding() {
    #[derive(WitType)]
    struct TupleWithPadding(u16, u32, i64);

    assert_eq!(TupleWithPadding::SIZE, 16);
    assert_eq!(<TupleWithPadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<TupleWithPadding as WitType>::Layout as Layout>::Flat::LEN,
        3
    );
}

/// Check the memory size and layout derived for a type with multiple named fields ordered in a way
/// that requires padding before two fields.
#[test]
fn named_struct_with_double_padding() {
    #[allow(dead_code)]
    #[derive(WitType)]
    struct RecordWithDoublePadding {
        first: u16,
        second: u32,
        third: i8,
        fourth: i64,
    }

    assert_eq!(RecordWithDoublePadding::SIZE, 24);
    assert_eq!(<RecordWithDoublePadding as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(
        <<RecordWithDoublePadding as WitType>::Layout as Layout>::Flat::LEN,
        4
    );
}

/// Check the memory size and layout derived for a type that contains a field with a type that also
/// has `WitType` derived for it.
#[test]
fn nested_types() {
    #[allow(dead_code)]
    #[derive(WitType)]
    struct Leaf {
        first: bool,
        second: u128,
    }

    assert_eq!(Leaf::SIZE, 24);
    assert_eq!(<Leaf as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Leaf as WitType>::Layout as Layout>::Flat::LEN, 3);

    #[allow(dead_code)]
    #[derive(WitType)]
    struct Branch {
        tag: u16,
        first_leaf: Leaf,
        second_leaf: Leaf,
    }

    assert_eq!(Branch::SIZE, 56);
    assert_eq!(<Branch as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Branch as WitType>::Layout as Layout>::Flat::LEN, 7);
}
