// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains boilerplate necessary for the application to interface with the host runtime.
//!
//! Ideally, this code should be exported from [`linera-sdk`], but that's currently impossible due
//! to how [`wit_bindgen_guest_rust`] works. It expects concrete types to be available in its parent
//! module (which in this case is this module), so it has to exist in every application
//! implementation.
//!
//! This should be fixable with a few changes to [`wit-bindgen`], but an alternative is to generate
//! the code with a procedural macro. For now, this module should be included by all implemented
//! applications.

// Export the application interface.
wit_bindgen_guest_rust::export!("service.wit");

// Import the system interface.
wit_bindgen_guest_rust::import!("queryable_system.wit");

mod conversions_from_wit;
mod conversions_to_wit;
mod exported_futures;
mod state_management;

use self::exported_futures::QueryApplication;
use super::ServiceState as Service;

/// Mark the service type to be exported.
impl service::Service for Service {}
