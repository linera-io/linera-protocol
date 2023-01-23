// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains boilerplate necessary for the service to interface with the host runtime.
//!
//! Ideally, this code should be exported from [`linera-sdk`], but that's currently impossible due
//! to how [`wit_bindgen_guest_rust`] works. It expects concrete types to be available in its parent
//! module (which in this case is this module), so it has to exist in every service
//! implementation.
//!
//! This should be fixable with a few changes to [`wit-bindgen`], but an alternative is to generate
//! the code with a procedural macro. For now, this module should be included by all implemented
//! services.

// Export the service interface.
wit_bindgen_guest_rust::export!("service.wit");

// Import the system interface.
wit_bindgen_guest_rust::import!("queryable_system.wit");

mod conversions_from_wit;
mod conversions_to_wit;
mod exported_futures;
pub mod system_api;

use self::exported_futures::QueryApplication;
use super::ApplicationState as Service;

/// Mark the service type to be exported.
impl service::Service for Service {}
