// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(missing_docs)]

pub use self::contract_system_api::*;

// Import the system interface.
wit_bindgen_guest_rust::import!("contract_system_api.wit");
