// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Debug, PartialEq, Eq, derive_more::Deref, derive_more::DerefMut)]
pub struct Dirty(
    #[deref]
    #[deref_mut]
    bool,
);

impl Dirty {
    pub fn new(dirty: bool) -> Self {
        Self(dirty)
    }
}

impl Drop for Dirty {
    fn drop(&mut self) {
        assert!(!self.0, "object dropped while dirty")
    }
}
