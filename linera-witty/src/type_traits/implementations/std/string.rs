// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`String`] type.

use crate::WitType;
use frunk::HList;

impl WitType for String {
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
}
