// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for using [`linera_views`] to store application state.

mod conversions_from_wit;
mod system_api;

pub use self::system_api::{KeyValueStore, ViewStorageContext};

// Import the views system interface.
wit_bindgen_guest_rust::import!("view_system_api.wit");
