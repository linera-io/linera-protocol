// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A wallet and keystore for the Linera client, persisted as JSON files on disk.

#![deny(missing_docs)]

pub mod display;
pub mod keystore;
pub mod paths;
pub mod wallet;

pub use keystore::Keystore;
pub use wallet::PersistentWallet;
