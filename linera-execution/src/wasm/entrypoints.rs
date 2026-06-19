// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm entrypoints for contracts and services.

// The `#[wit_import]` macro generates items (a struct and associated functions) that cannot
// carry doc comments, so we allow missing docs for this whole module of WIT bindings.
#![allow(missing_docs)]

use linera_base::data_types::StreamUpdate;
use linera_witty::wit_import;

/// WIT entrypoints for application contracts.
#[wit_import(package = "linera:app")]
pub trait ContractEntrypoints {
    fn instantiate(argument: Vec<u8>);
    fn execute_operation(operation: Vec<u8>) -> Vec<u8>;
    fn execute_message(message: Vec<u8>);
    fn process_streams(streams: Vec<StreamUpdate>);
    fn finalize();
}

/// WIT entrypoints for application services.
#[wit_import(package = "linera:app")]
pub trait ServiceEntrypoints {
    fn handle_query(argument: Vec<u8>) -> Vec<u8>;
}
