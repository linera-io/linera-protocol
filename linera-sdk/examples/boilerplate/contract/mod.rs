// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contains boilerplate necessary for the contract to interface with the host runtime.
//!
//! Ideally, this code should be exported from [`linera-sdk`], but that's currently impossible due
//! to how [`wit_bindgen_guest_rust`] works. It expects concrete types to be available in its parent
//! module (which in this case is this module), so it has to exist in every contract
//! implementation.
//!
//! This should be fixable with a few changes to [`wit-bindgen`], but an alternative is to generate
//! the code with a procedural macro. For now, this module should be included by all implemented
//! contracts.

// Export the contract interface.
linera_sdk::export_contract!(Contract);

use super::ApplicationState as Contract;

/// Mark the contract type to be exported.
impl linera_sdk::contract::Contract for Contract {
    type CallApplication = CallApplication;
    type CallSession = CallSession;
    type ExecuteEffect = ExecuteEffect;
    type ExecuteOperation = ExecuteOperation;
    type Initialize = Initialize;
}

linera_sdk::instance_exported_future! {
    contract::Initialize<Contract>(
        context: linera_sdk::contract::OperationContext,
        argument: Vec<u8>,
    ) -> PollExecutionResult
}

linera_sdk::instance_exported_future! {
    contract::ExecuteOperation<Contract>(
        context: linera_sdk::contract::OperationContext,
        operation: Vec<u8>,
    ) -> PollExecutionResult
}

linera_sdk::instance_exported_future! {
    contract::ExecuteEffect<Contract>(
        context: linera_sdk::contract::EffectContext,
        effect: Vec<u8>,
    ) -> PollExecutionResult
}

linera_sdk::instance_exported_future! {
    contract::CallApplication<Contract>(
        context: linera_sdk::contract::CalleeContext,
        argument: Vec<u8>,
        forwarded_sessions: Vec<linera_sdk::contract::SessionId>,
    ) -> PollCallApplication
}

linera_sdk::instance_exported_future! {
    contract::CallSession<Contract>(
        context: linera_sdk::contract::CalleeContext,
        session: linera_sdk::contract::Session,
        argument: Vec<u8>,
        forwarded_sessions: Vec<linera_sdk::contract::SessionId>,
    ) -> PollCallSession
}
