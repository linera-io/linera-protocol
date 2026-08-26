// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A provenance-tracking wrapper around [`std::sync::Arc`].
//!
//! [`Arc<T>`] is identical to [`std::sync::Arc<T>`] at runtime but has no
//! public constructor. The only way to obtain one is through
//! [`ValueCache::insert`], [`ValueCache::insert_hashed`], or
//! [`ValueCache::get`]. This makes the "one allocation per content" invariant
//! structurally enforced: callers cannot bypass the cache by calling
//! `Arc::new` directly.

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc as StdArc,
};

/// A reference-counted pointer that can only be constructed through a
/// [`crate::ValueCache`].
///
/// `Arc<T>` wraps [`std::sync::Arc<T>`] and implements `Deref<Target = T>`,
/// `Clone`, `Debug`, `Display`, `PartialEq`, `Eq`, and `Hash` identically.
///
/// Use [`Arc::into_std`] or [`Arc::as_std`] to interoperate with APIs that
/// require [`std::sync::Arc<T>`] explicitly.
pub struct Arc<T>(pub(crate) StdArc<T>);

impl<T> Arc<T> {
    /// Returns a reference to the underlying [`std::sync::Arc<T>`].
    pub fn as_std(&self) -> &StdArc<T> {
        &self.0
    }

    /// Converts into the underlying [`std::sync::Arc<T>`].
    pub fn into_std(self) -> StdArc<T> {
        self.0
    }

    /// Unwraps the inner value if this is the only strong reference;
    /// otherwise clones it.
    pub fn unwrap_or_clone(this: Self) -> T
    where
        T: Clone,
    {
        StdArc::unwrap_or_clone(this.0)
    }

    /// Returns `true` if two `Arc`s point to the same allocation.
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        StdArc::ptr_eq(&a.0, &b.0)
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: fmt::Debug> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl<T: fmt::Display> fmt::Display for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl<T: PartialEq> PartialEq for Arc<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq> Eq for Arc<T> {}

impl<T: Hash> Hash for Arc<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> From<Arc<T>> for StdArc<T> {
    fn from(arc: Arc<T>) -> Self {
        arc.0
    }
}

impl<T> AsRef<StdArc<T>> for Arc<T> {
    fn as_ref(&self) -> &StdArc<T> {
        &self.0
    }
}
