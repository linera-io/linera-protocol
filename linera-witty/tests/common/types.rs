// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Dummy types used in tests.

use linera_witty::WitType;

/// A type that wraps a simple type.
#[derive(WitType)]
pub struct SimpleWrapper(bool);

/// A tuple struct that doesn't need any internal padding in its memory layout.
#[derive(WitType)]
pub struct TupleWithoutPadding(u64, i32, i16);

/// A tuple struct that requires internal padding in its memory layout between all of its fields.
#[derive(WitType)]
pub struct TupleWithPadding(u16, u32, i64);

/// A struct with named fields that requires padding in two locations in its memory layout.
#[allow(dead_code)]
#[derive(WitType)]
pub struct RecordWithDoublePadding {
    first: u16,
    second: u32,
    third: i8,
    fourth: i64,
}

/// A simple struct with named fields to be used inside a more complex struct.
#[allow(dead_code)]
#[derive(WitType)]
pub struct Leaf {
    first: bool,
    second: u128,
}

/// A struct that contains fields with custom types that also have derived trait implementations.
#[allow(dead_code)]
#[derive(WitType)]
pub struct Branch {
    tag: u16,
    first_leaf: Leaf,
    second_leaf: Leaf,
}
