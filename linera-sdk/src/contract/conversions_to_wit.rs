// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types declared in [`linera-sdk`] to types generated by [`wit-bindgen`].

use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, ApplicationPermissions, BlockHeight, Resources, SendMessageRequest, TimeDelta,
    },
    identifiers::{
        Account, AccountOwner, ApplicationId, BytecodeId, ChainId, ChannelName, Destination,
        MessageId, Owner, StreamName,
    },
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
};
use linera_views::batch::WriteOperation;

use super::wit::contract_runtime_api as wit_contract_api;

impl From<CryptoHash> for wit_contract_api::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(crypto_hash);

        wit_contract_api::CryptoHash {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<ChainId> for wit_contract_api::CryptoHash {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<Owner> for wit_contract_api::Owner {
    fn from(owner: Owner) -> Self {
        wit_contract_api::Owner {
            inner0: owner.0.into(),
        }
    }
}

impl From<Amount> for wit_contract_api::Amount {
    fn from(host: Amount) -> Self {
        wit_contract_api::Amount {
            inner0: (host.lower_half(), host.upper_half()),
        }
    }
}

impl From<Account> for wit_contract_api::Account {
    fn from(account: Account) -> Self {
        wit_contract_api::Account {
            chain_id: account.chain_id.into(),
            owner: account.owner.map(|owner| owner.into()),
        }
    }
}

impl From<AccountOwner> for wit_contract_api::AccountOwner {
    fn from(account_owner: AccountOwner) -> Self {
        match account_owner {
            AccountOwner::User(owner) => wit_contract_api::AccountOwner::User(owner.into()),
            AccountOwner::Application(application_id) => {
                wit_contract_api::AccountOwner::Application(application_id.into())
            }
        }
    }
}

impl From<ChainId> for wit_contract_api::ChainId {
    fn from(chain_id: ChainId) -> Self {
        wit_contract_api::ChainId {
            inner0: chain_id.0.into(),
        }
    }
}

impl From<BlockHeight> for wit_contract_api::BlockHeight {
    fn from(block_height: BlockHeight) -> Self {
        wit_contract_api::BlockHeight {
            inner0: block_height.0,
        }
    }
}

impl From<BytecodeId> for wit_contract_api::BytecodeId {
    fn from(bytecode_id: BytecodeId) -> Self {
        wit_contract_api::BytecodeId {
            contract_blob_hash: bytecode_id.contract_blob_hash.into(),
            service_blob_hash: bytecode_id.service_blob_hash.into(),
            vm_runtime: bytecode_id.vm_runtime.into(),
        }
    }
}

impl From<VmRuntime> for wit_system_api::VmRuntime {
    fn from(vm_runtime: VmRuntime) -> Self {
        match vm_runtime {
            VmRuntime::Wasm => wit_system_api::VmRuntime::Wasm,
            VmRuntime::Evm => wit_system_api::VmRuntime::Evm,
        }
    }
}

impl From<MessageId> for wit_contract_api::MessageId {
    fn from(message_id: MessageId) -> Self {
        wit_contract_api::MessageId {
            chain_id: message_id.chain_id.into(),
            height: message_id.height.into(),
            index: message_id.index,
        }
    }
}

impl From<ApplicationId> for wit_contract_api::ApplicationId {
    fn from(application_id: ApplicationId) -> Self {
        wit_contract_api::ApplicationId {
            bytecode_id: application_id.bytecode_id.into(),
            creation: application_id.creation.into(),
        }
    }
}

impl From<Resources> for wit_contract_api::Resources {
    fn from(resources: Resources) -> Self {
        wit_contract_api::Resources {
            fuel: resources.fuel,
            read_operations: resources.read_operations,
            write_operations: resources.write_operations,
            bytes_to_read: resources.bytes_to_read,
            bytes_to_write: resources.bytes_to_write,
            messages: resources.messages,
            message_size: resources.message_size,
            storage_size_delta: resources.storage_size_delta,
        }
    }
}

impl From<ChannelName> for wit_contract_api::ChannelName {
    fn from(name: ChannelName) -> Self {
        wit_contract_api::ChannelName {
            inner0: name.into_bytes(),
        }
    }
}

impl From<Destination> for wit_contract_api::Destination {
    fn from(destination: Destination) -> Self {
        match destination {
            Destination::Recipient(chain_id) => {
                wit_contract_api::Destination::Recipient(chain_id.into())
            }
            Destination::Subscribers(subscription) => {
                wit_contract_api::Destination::Subscribers(subscription.into())
            }
        }
    }
}

impl From<SendMessageRequest<Vec<u8>>> for wit_contract_api::SendMessageRequest {
    fn from(message: SendMessageRequest<Vec<u8>>) -> Self {
        Self {
            destination: message.destination.into(),
            authenticated: message.authenticated,
            is_tracked: message.is_tracked,
            grant: message.grant.into(),
            message: message.message,
        }
    }
}

impl From<StreamName> for wit_contract_api::StreamName {
    fn from(name: StreamName) -> Self {
        wit_contract_api::StreamName {
            inner0: name.into_bytes(),
        }
    }
}

impl From<TimeDelta> for wit_contract_api::TimeDelta {
    fn from(delta: TimeDelta) -> Self {
        Self {
            inner0: delta.as_micros(),
        }
    }
}

impl From<TimeoutConfig> for wit_contract_api::TimeoutConfig {
    fn from(config: TimeoutConfig) -> Self {
        let TimeoutConfig {
            fast_round_duration,
            base_timeout,
            timeout_increment,
            fallback_duration,
        } = config;
        Self {
            fast_round_duration: fast_round_duration.map(Into::into),
            base_timeout: base_timeout.into(),
            timeout_increment: timeout_increment.into(),
            fallback_duration: fallback_duration.into(),
        }
    }
}

impl From<ApplicationPermissions> for wit_contract_api::ApplicationPermissions {
    fn from(permissions: ApplicationPermissions) -> Self {
        let ApplicationPermissions {
            execute_operations,
            mandatory_applications,
            close_chain,
            change_application_permissions,
        } = permissions;
        Self {
            execute_operations: execute_operations
                .map(|app_ids| app_ids.into_iter().map(Into::into).collect()),
            mandatory_applications: mandatory_applications.into_iter().map(Into::into).collect(),
            close_chain: close_chain.into_iter().map(Into::into).collect(),
            change_application_permissions: change_application_permissions
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<ChainOwnership> for wit_contract_api::ChainOwnership {
    fn from(ownership: ChainOwnership) -> Self {
        let ChainOwnership {
            super_owners,
            owners,
            multi_leader_rounds,
            open_multi_leader_rounds,
            timeout_config,
        } = ownership;
        Self {
            super_owners: super_owners.into_iter().map(Into::into).collect(),
            owners: owners
                .into_iter()
                .map(|(owner, weight)| (owner.into(), weight))
                .collect(),
            multi_leader_rounds,
            open_multi_leader_rounds,
            timeout_config: timeout_config.into(),
        }
    }
}

impl From<WriteOperation> for wit_contract_api::WriteOperation {
    fn from(write_operation: WriteOperation) -> Self {
        match write_operation {
            WriteOperation::Delete { key } => wit_contract_api::WriteOperation::Delete(key),
            WriteOperation::DeletePrefix { key_prefix } => {
                wit_contract_api::WriteOperation::DeletePrefix(key_prefix)
            }
            WriteOperation::Put { key, value } => {
                wit_contract_api::WriteOperation::Put((key, value))
            }
        }
    }
}
