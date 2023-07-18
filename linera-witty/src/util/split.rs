// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compile time splitting of heterogeneous lists.

use frunk::{hlist, HCons, HNil};

/// Compile time splitting of heterogeneous lists.
///
/// Allows splitting a heterogeneous list at a certain point, determined by the `Target` type,
/// which should match this list type until a certain element. The list after that point is
/// returned as the `Remainder` type.
pub trait Split<Target> {
    /// The tail of remaining elements after splitting up the list.
    type Remainder;

    /// Splits the current heterogeneous list in two.
    fn split(self) -> (Target, Self::Remainder);
}

impl<AnyTail> Split<HNil> for AnyTail {
    type Remainder = AnyTail;

    fn split(self) -> (HNil, Self::Remainder) {
        (hlist![], self)
    }
}

impl<Head, SourceTail, TargetTail> Split<HCons<Head, TargetTail>> for HCons<Head, SourceTail>
where
    SourceTail: Split<TargetTail>,
{
    type Remainder = <SourceTail as Split<TargetTail>>::Remainder;

    fn split(self) -> (HCons<Head, TargetTail>, Self::Remainder) {
        let (tail, remainder) = self.tail.split();

        (
            HCons {
                head: self.head,
                tail,
            },
            remainder,
        )
    }
}
