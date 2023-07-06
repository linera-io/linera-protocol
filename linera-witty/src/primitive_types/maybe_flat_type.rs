// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A representation of either a [`FlatType`] or nothing, represented by the unit (`()`) type.

use super::flat_type::FlatType;

/// A marker trait for [`FlatType`]s and the unit type, which uses no storage space.
pub trait MaybeFlatType: Sized {}

impl MaybeFlatType for () {}

impl<AnyFlatType> MaybeFlatType for AnyFlatType where AnyFlatType: FlatType {}
