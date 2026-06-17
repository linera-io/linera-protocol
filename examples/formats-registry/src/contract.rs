// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::ComplexObject;
use formats_registry::{FormatsRegistryAbi, Message, Operation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::FormatsRegistryState;

linera_sdk::contract!(FormatsRegistryContract);

pub struct FormatsRegistryContract {
    state: FormatsRegistryState,
    runtime: ContractRuntime<Self>,
}

impl WithContractAbi for FormatsRegistryContract {
    type Abi = FormatsRegistryAbi;
}

impl Contract for FormatsRegistryContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FormatsRegistryState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FormatsRegistryContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        // Authenticate that the declared owner really signed (or is the caller of)
        // this operation.
        let owner = operation.owner();
        self.runtime
            .check_account_permission(owner)
            .expect("Failed to authenticate the owner of the operation");

        let message = operation.into_message();
        let creator_chain_id = self.runtime.application_creator_chain_id();
        if self.runtime.chain_id() == creator_chain_id {
            self.execute_locally(message).await;
        } else {
            // The registry only mutates state on its creation chain; forward the
            // request there so the admin policy is applied in a single place.
            self.runtime
                .prepare_message(message)
                .send_to(creator_chain_id);
        }
    }

    async fn execute_message(&mut self, message: Message) {
        assert_eq!(
            self.runtime.chain_id(),
            self.runtime.application_creator_chain_id(),
            "Registry messages can only be executed on the application's creation chain"
        );
        self.execute_locally(message).await;
    }

    async fn store(self) {
        self.state
            .save_and_drop()
            .await
            .expect("Failed to save state");
    }
}

impl FormatsRegistryContract {
    /// Applies the admin security policy and then performs the requested mutation.
    /// This always runs on the application's creation chain.
    async fn execute_locally(&mut self, message: Message) {
        let owner = message.owner();
        if let Some(admins) = self.state.admins.get() {
            assert!(
                admins.contains(&owner),
                "Operation can only be executed by an authorized admin account. Got {owner}"
            );
        } else {
            // No admin set has been configured yet: everyone is allowed, but only
            // locally. Remote requests are refused until admins are configured.
            assert!(
                self.runtime.message_origin_chain_id().is_none(),
                "Refusing to execute a remote operation before any admin is configured",
            );
        }

        match message {
            Message::Write {
                module_id, value, ..
            } => {
                let existing = self.state.formats.get(&module_id).await.expect("storage");
                assert!(
                    existing.is_none(),
                    "formats are already registered for this module"
                );
                self.state
                    .formats
                    .insert(&module_id, value)
                    .expect("storage");
            }
            Message::SetAdmins { admins, .. } => {
                self.state
                    .admins
                    .set(admins.map(|admins| admins.into_iter().collect()));
            }
        }
    }
}

/// This implementation is only nonempty in the service.
#[ComplexObject]
impl FormatsRegistryState {}
