// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Joining of flat layouts of different variants of a `variant` type.
//!
//! When flattening `variant` types, a single flat layout must be obtained for the type by joining
//! the flat layout of each variant. This means finding a flat type for each layout element to
//! represent the flat type of any of the variants. See [`crate::primitive_types::JoinFlatTypes`]
//! for more information on how flat types are joined.

use either::Either;
use frunk::{HCons, HNil};

use crate::primitive_types::{FlatType, JoinFlatTypes};

/// Allows converting between the current flat layout and the joined `Target` flat layout, which
/// may be longer or have some elements wider than the current elements.
pub trait JoinFlatLayouts<Target> {
    /// Converts the current flat layout into a the joined `Target` flat layout.
    fn into_joined(self) -> Target;

    /// Converts from the joined `Target` flat layout into the current flat layout.
    fn from_joined(joined: Target) -> Self;
}

impl JoinFlatLayouts<HNil> for HNil {
    fn into_joined(self) -> HNil {
        HNil
    }

    fn from_joined(_joined: HNil) -> Self {
        HNil
    }
}

impl<TargetHead, TargetTail> JoinFlatLayouts<HCons<TargetHead, TargetTail>> for HNil
where
    TargetHead: Default,
    HNil: JoinFlatLayouts<TargetTail>,
{
    fn into_joined(self) -> HCons<TargetHead, TargetTail> {
        HCons {
            head: TargetHead::default(),
            tail: HNil.into_joined(),
        }
    }

    fn from_joined(_joined: HCons<TargetHead, TargetTail>) -> Self {
        HNil
    }
}

impl<SourceHead, SourceTail, TargetHead, TargetTail> JoinFlatLayouts<HCons<TargetHead, TargetTail>>
    for HCons<SourceHead, SourceTail>
where
    SourceHead: FlatType,
    TargetHead: FlatType,
    Either<SourceHead, TargetHead>: JoinFlatTypes<Flat = TargetHead>,
    SourceTail: JoinFlatLayouts<TargetTail>,
{
    fn into_joined(self) -> HCons<TargetHead, TargetTail> {
        HCons {
            head: Either::Left(self.head).join(),
            tail: self.tail.into_joined(),
        }
    }

    fn from_joined(joined: HCons<TargetHead, TargetTail>) -> Self {
        HCons {
            head: joined.head.split_into(),
            tail: SourceTail::from_joined(joined.tail),
        }
    }
}
