// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Utilities for building traits that work in both single-threaded and multi-threaded
contexts.
*/

/// A trait that extends `Send` and `Sync` if not compiling for the Web.
#[cfg(web)]
pub trait AutoTraits: 'static {}
#[cfg(web)]
impl<T: 'static> AutoTraits for T {}

#[cfg(not(web))]
trait_set::trait_set! {
    /// A trait that extends `Send` and `Sync` if not compiling for the Web.
    pub trait AutoTraits = Send + Sync + 'static;
}

trait_set::trait_set! {
    /// Precomposed `std::error::Error + AutoTraits`, for use in type-erased error
    /// objects.
    pub trait DynError = std::error::Error + AutoTraits;
}
