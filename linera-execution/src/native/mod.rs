// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Built-in applications executed natively by `linera-execution` (no bytecode).

pub mod fungible;

use linera_base::data_types::NativeApplicationKind;

use crate::{
    runtime::{ContractSyncRuntimeHandle, ServiceSyncRuntimeHandle},
    ExecutionError, UserContractInstance, UserServiceInstance,
};

pub(crate) fn instantiate_contract(
    kind: NativeApplicationKind,
    runtime: ContractSyncRuntimeHandle,
) -> Result<UserContractInstance, ExecutionError> {
    match kind {
        NativeApplicationKind::Fungible => fungible::instantiate_contract(runtime),
    }
}

pub(crate) fn instantiate_service(
    kind: NativeApplicationKind,
    runtime: ServiceSyncRuntimeHandle,
) -> Result<UserServiceInstance, ExecutionError> {
    match kind {
        NativeApplicationKind::Fungible => fungible::instantiate_service(runtime),
    }
}
