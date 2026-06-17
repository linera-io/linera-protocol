// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Formats Registry Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{AccountOwner, ContractAbi, ModuleId, ServiceAbi},
};
use serde::{Deserialize, Serialize};

/// The ABI of the formats-registry example application.
pub struct FormatsRegistryAbi;

impl ContractAbi for FormatsRegistryAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for FormatsRegistryAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Operations accepted by the contract.
///
/// `Write` is kept as the first variant with exactly the fields of
/// [`linera_sdk::abis::formats_registry::Operation::Write`], so the operation
/// produced by `linera publish-module-with-formats` decodes correctly here. Extra,
/// implementation-specific admin commands are appended after it.
#[derive(Clone, Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Register `value` for `module_id`, on behalf of `owner`. A given `module_id`
    /// may only be written once.
    Write {
        owner: AccountOwner,
        module_id: ModuleId,
        value: Vec<u8>,
    },
    /// Set the admin accounts authorized to run admin commands (including remote
    /// `Write`s). Passing `None` clears the set, restoring creation-chain-only
    /// access.
    SetAdmins {
        owner: AccountOwner,
        admins: Option<Vec<AccountOwner>>,
    },
}

/// Messages exchanged between chains of the same application instance. Each variant
/// mirrors the corresponding [`Operation`] and is sent to the application's creation
/// chain to be executed there.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Message {
    /// Remote counterpart of [`Operation::Write`].
    Write {
        owner: AccountOwner,
        module_id: ModuleId,
        value: Vec<u8>,
    },
    /// Remote counterpart of [`Operation::SetAdmins`].
    SetAdmins {
        owner: AccountOwner,
        admins: Option<Vec<AccountOwner>>,
    },
}

impl Operation {
    /// The account on whose behalf the operation is executed.
    pub fn owner(&self) -> AccountOwner {
        match self {
            Operation::Write { owner, .. } | Operation::SetAdmins { owner, .. } => *owner,
        }
    }

    /// Converts the operation into the cross-chain message that performs it on the
    /// creation chain.
    pub fn into_message(self) -> Message {
        match self {
            Operation::Write {
                owner,
                module_id,
                value,
            } => Message::Write {
                owner,
                module_id,
                value,
            },
            Operation::SetAdmins { owner, admins } => Message::SetAdmins { owner, admins },
        }
    }
}

impl Message {
    /// The account on whose behalf the message is executed.
    pub fn owner(&self) -> AccountOwner {
        match self {
            Message::Write { owner, .. } | Message::SetAdmins { owner, .. } => *owner,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::{
        formats::{BcsApplication, Formats},
        linera_base_types::{AccountOwner, ModuleId, VmRuntime},
    };
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{FormatsRegistryAbi, Message, Operation};

    /// The Formats Registry application.
    pub struct FormatsRegistryApplication;

    impl BcsApplication for FormatsRegistryApplication {
        type Abi = FormatsRegistryAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<Operation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<()>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<AccountOwner>(&samples)?;
            tracer.trace_type::<ModuleId>(&samples)?;
            tracer.trace_type::<VmRuntime>(&samples)?;

            let registry = tracer.registry()?;

            Ok(Formats {
                registry,
                operation,
                response,
                message,
                event_value,
            })
        }
    }
}
