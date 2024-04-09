// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of WIT files.

pub use crate::type_traits::RegisterWitTypes;

/// Generates WIT snippets for an interface.
pub trait WitInterface {
    /// The [`WitType`][`crate::WitType`]s that this interface uses.
    type Dependencies: RegisterWitTypes;

    /// The name of the package the interface belongs to.
    fn wit_package() -> &'static str;

    /// The name of the interface.
    fn wit_name() -> &'static str;

    /// The WIT definitions of each function in this interface.
    fn wit_functions() -> Vec<String>;
}
