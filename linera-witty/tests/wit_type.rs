// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `WitType` derive macro.

#[path = "common/types.rs"]
mod types;

use self::types::{
    Branch, Leaf, RecordWithDoublePadding, SimpleWrapper, TupleWithPadding, TupleWithoutPadding,
};
use linera_witty::{HList, Layout, WitType};

/// Check the memory size and layout derived for a wrapper type.
#[test]
fn simple_bool_wrapper() {
    assert_eq!(SimpleWrapper::SIZE, 1);
    assert_eq!(<SimpleWrapper as WitType>::Layout::ALIGNMENT, 1);
    assert_eq!(<<SimpleWrapper as WitType>::Layout as Layout>::Flat::LEN, 1);
}

/// Check the memory size and layout derived for a type with multiple fields ordered in a way that
/// doesn't require any padding.
#[test]
fn tuple_struct_without_padding() {
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
    assert_eq!(Leaf::SIZE, 24);
    assert_eq!(<Leaf as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Leaf as WitType>::Layout as Layout>::Flat::LEN, 3);

    assert_eq!(Branch::SIZE, 56);
    assert_eq!(<Branch as WitType>::Layout::ALIGNMENT, 8);
    assert_eq!(<<Branch as WitType>::Layout as Layout>::Flat::LEN, 7);
}
