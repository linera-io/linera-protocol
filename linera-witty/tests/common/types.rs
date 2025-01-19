// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Dummy types used in tests.

use std::{rc::Rc, sync::Arc};

use linera_witty::{WitLoad, WitStore, WitType};

/// A type that wraps a simple type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct SimpleWrapper(pub bool);

/// A tuple struct that doesn't need any internal padding in its memory layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct TupleWithoutPadding(pub u64, pub i32, pub i16);

/// A tuple struct that requires internal padding in its memory layout between two of its fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct TupleWithPadding(pub u16, pub u32, pub i64);

/// A struct with named fields that requires padding in two locations in its memory layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct RecordWithDoublePadding {
    pub first: u16,
    pub second: u32,
    pub third: i8,
    pub fourth: i64,
}

/// A simple struct with named fields to be used inside a more complex struct.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct Leaf {
    pub first: bool,
    pub second: u128,
}

/// A struct that contains fields with custom types that also have derived trait implementations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct Branch {
    pub tag: u16,
    pub first_leaf: Leaf,
    pub second_leaf: Leaf,
}

/// An enum that has its alignment obtained from one variant and its size from another.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub enum Enum {
    Empty,
    LargeVariantWithLooseAlignment(i8, i8, i8, i8, i8, i8, i8, i8, i8, i8),
    SmallerVariantWithStrictAlignment { inner: u64 },
}

/// A generic struct with some specialized fields.
#[derive(Clone, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
#[witty_specialize_with(A = u8, B = i16)]
pub struct SpecializedGenericStruct<A, B> {
    pub first: A,
    pub second: B,
    pub both: Vec<(A, B)>,
}

/// A generic enum with some specialized fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
#[witty_specialize_with(A = Option<bool>)]
#[witty_specialize_with(B = u32)]
pub enum SpecializedGenericEnum<A, B> {
    None,
    First(A),
    MaybeSecond { maybe: Option<B> },
}

/// A struct that contains fields that are wrapped in smart pointer types.
#[derive(Clone, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct StructWithHeapFields {
    pub boxed: Box<SimpleWrapper>,
    pub rced: Rc<Leaf>,
    pub arced: Arc<Enum>,
}

/// A struct that contains fields that should all be represented as lists.
#[derive(Clone, Debug, Eq, PartialEq, WitType, WitLoad, WitStore)]
pub struct StructWithLists {
    pub vec: Vec<SimpleWrapper>,
    pub boxed_slice: Box<[TupleWithPadding]>,
    pub rced_slice: Rc<[Leaf]>,
    pub arced_slice: Arc<[RecordWithDoublePadding]>,
}

/// A type that wraps a slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq, WitType, WitStore)]
pub struct SliceWrapper<'slice>(pub &'slice [TupleWithoutPadding]);
