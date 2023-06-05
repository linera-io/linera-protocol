// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions to types generated by `wit-bindgen`.
//!
//! Allows converting types used in `linera-execution` to types that can be sent to the guest WASM
//! module.

#![allow(clippy::duplicate_mod)]

use super::{contract, contract_system_api, service, service_system_api};
use crate::{
    CallResult, CalleeContext, MessageContext, MessageId, OperationContext, QueryContext,
    SessionId, UserApplicationId,
};
use linera_base::{crypto::CryptoHash, data_types::Amount, identifiers::ChainId};

impl From<OperationContext> for contract::OperationContext {
    fn from(host: OperationContext) -> Self {
        contract::OperationContext {
            chain_id: host.chain_id.into(),
            authenticated_signer: host.authenticated_signer.map(|owner| owner.0.into()),
            height: host.height.0,
            index: host.index,
        }
    }
}

impl From<MessageContext> for contract::MessageContext {
    fn from(host: MessageContext) -> Self {
        contract::MessageContext {
            chain_id: host.chain_id.into(),
            authenticated_signer: host.authenticated_signer.map(|owner| owner.0.into()),
            height: host.height.0,
            message_id: host.message_id.into(),
        }
    }
}

impl From<MessageId> for service_system_api::MessageId {
    fn from(host: MessageId) -> Self {
        service_system_api::MessageId {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host.index,
        }
    }
}

impl From<MessageId> for contract_system_api::MessageId {
    fn from(host: MessageId) -> Self {
        contract_system_api::MessageId {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host.index,
        }
    }
}

impl From<MessageId> for contract::MessageId {
    fn from(host: MessageId) -> Self {
        contract::MessageId {
            chain_id: host.chain_id.into(),
            height: host.height.0,
            index: host.index,
        }
    }
}

impl From<CalleeContext> for contract::CalleeContext {
    fn from(host: CalleeContext) -> Self {
        contract::CalleeContext {
            chain_id: host.chain_id.into(),
            authenticated_signer: host.authenticated_signer.map(|owner| owner.0.into()),
            authenticated_caller_id: host
                .authenticated_caller_id
                .map(contract::ApplicationId::from),
        }
    }
}

impl From<QueryContext> for service::QueryContext {
    fn from(host: QueryContext) -> Self {
        service::QueryContext {
            chain_id: host.chain_id.into(),
        }
    }
}

impl From<SessionId> for contract::SessionId {
    fn from(host: SessionId) -> Self {
        contract::SessionId {
            application_id: host.application_id.into(),
            index: host.index,
        }
    }
}

impl From<SessionId> for contract_system_api::SessionId {
    fn from(host: SessionId) -> Self {
        contract_system_api::SessionId {
            application_id: host.application_id.into(),
            index: host.index,
        }
    }
}

impl From<UserApplicationId> for contract::ApplicationId {
    fn from(host: UserApplicationId) -> Self {
        contract::ApplicationId {
            bytecode_id: host.bytecode_id.message_id.into(),
            creation: host.creation.into(),
        }
    }
}

impl From<UserApplicationId> for service_system_api::ApplicationId {
    fn from(host: UserApplicationId) -> Self {
        service_system_api::ApplicationId {
            bytecode_id: host.bytecode_id.message_id.into(),
            creation: host.creation.into(),
        }
    }
}

impl From<UserApplicationId> for contract_system_api::ApplicationId {
    fn from(host: UserApplicationId) -> Self {
        contract_system_api::ApplicationId {
            bytecode_id: host.bytecode_id.message_id.into(),
            creation: host.creation.into(),
        }
    }
}

impl From<ChainId> for service_system_api::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<ChainId> for contract_system_api::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<ChainId> for contract::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<ChainId> for service::ChainId {
    fn from(chain_id: ChainId) -> Self {
        chain_id.0.into()
    }
}

impl From<CryptoHash> for contract::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.as_bytes();

        contract::CryptoHash {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
        }
    }
}

impl From<CryptoHash> for service::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.as_bytes();

        service::CryptoHash {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
        }
    }
}

impl From<CryptoHash> for service_system_api::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.as_bytes();

        service_system_api::CryptoHash {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
        }
    }
}

impl From<CryptoHash> for contract_system_api::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.as_bytes();

        contract_system_api::CryptoHash {
            part1: u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices")),
            part2: u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices")),
            part3: u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices")),
            part4: u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices")),
        }
    }
}

impl From<CallResult> for contract_system_api::CallResult {
    fn from(host: CallResult) -> Self {
        contract_system_api::CallResult {
            value: host.value,
            sessions: host
                .sessions
                .into_iter()
                .map(contract_system_api::SessionId::from)
                .collect(),
        }
    }
}

impl From<Amount> for service_system_api::Amount {
    fn from(host: Amount) -> Self {
        service_system_api::Amount {
            lower_half: host.lower_half(),
            upper_half: host.upper_half(),
        }
    }
}

impl From<Amount> for contract_system_api::Amount {
    fn from(host: Amount) -> Self {
        contract_system_api::Amount {
            lower_half: host.lower_half(),
            upper_half: host.upper_half(),
        }
    }
}
