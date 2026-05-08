// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Built-in applications executed natively by `linera-execution` (no bytecode).

pub mod fungible;

use linera_base::data_types::NativeApplicationKind;

use crate::{UserContractCode, UserServiceCode};

/// Returns the native [`UserContractCode`] for the given kind.
pub fn user_contract_code(kind: NativeApplicationKind) -> UserContractCode {
    match kind {
        NativeApplicationKind::Fungible => fungible::NativeFungibleContractModule.into(),
    }
}

/// Returns the native [`UserServiceCode`] for the given kind.
pub fn user_service_code(kind: NativeApplicationKind) -> UserServiceCode {
    match kind {
        NativeApplicationKind::Fungible => fungible::NativeFungibleServiceModule.into(),
    }
}
