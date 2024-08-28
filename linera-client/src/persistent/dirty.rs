// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dirty(bool);

impl Dirty {
    pub fn new(dirty: bool) -> Self {
        Self(dirty)
    }
}

impl std::ops::Deref for Dirty {
    type Target = bool;
    fn deref(&self) -> &bool { &self.0 }
}

impl std::ops::DerefMut for Dirty {
    fn deref_mut(&mut self) -> &mut bool { &mut self.0 }
}

impl Drop for Dirty {
    fn drop(&mut self) {
        assert!(!self.0, "object dropped while dirty")
    }
}
