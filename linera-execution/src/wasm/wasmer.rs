// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Wasmer](https://wasmer.io/) runtime.

// Import the interface implemented by a user application.
wit_bindgen_host_wasmer_rust::import!("../linera-sdk/application.wit");
