// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm entrypoints for contracts and services.

use linera_witty::wit_import;

/// WIT entrypoints for application contracts.
#[wit_import(package = "linera:app")]
pub trait ContractEntrypoints {
    fn instantiate(argument: Vec<u8>) -> Result<(), String>;
    fn execute_operation(operation: Vec<u8>) -> Result<Vec<u8>, String>;
    fn execute_message(message: Vec<u8>) -> Result<(), String>;
    fn finalize() -> Result<(), String>;
}

/// WIT entrypoints for application services.
#[wit_import(package = "linera:app")]
pub trait ServiceEntrypoints {
    fn handle_query(argument: Vec<u8>) -> Result<Vec<u8>, String>;
}
