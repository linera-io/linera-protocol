// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types generated by `wit-bindgen`.
//!
//! Allows converting types returned from a Wasm module into types that can be used with the rest
//! of the crate.

#![allow(clippy::duplicate_mod)]

use super::{contract, contract_system_api, service_system_api};
use crate::{
    ApplicationCallOutcome, ChannelName, Destination, MessageKind, RawExecutionOutcome,
    RawOutgoingMessage, SessionCallOutcome, SessionId, UserApplicationId,
};
use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{BytecodeId, ChainId, MessageId},
};

impl From<contract::SessionCallOutcome> for (SessionCallOutcome, Vec<u8>) {
    fn from(outcome: contract::SessionCallOutcome) -> Self {
        let session_call_outcome = SessionCallOutcome {
            inner: outcome.inner.into(),
            close_session: outcome.new_state.is_some(),
        };

        let updated_session_state = outcome.new_state.unwrap_or_default();

        (session_call_outcome, updated_session_state)
    }
}

impl From<contract::ApplicationCallOutcome> for ApplicationCallOutcome {
    fn from(outcome: contract::ApplicationCallOutcome) -> Self {
        ApplicationCallOutcome {
            create_sessions: outcome.create_sessions,
            execution_outcome: outcome.execution_outcome.into(),
            value: outcome.value,
        }
    }
}

impl From<contract::OutgoingMessage> for RawOutgoingMessage<Vec<u8>> {
    fn from(message: contract::OutgoingMessage) -> Self {
        Self {
            destination: message.destination.into(),
            authenticated: message.authenticated,
            grant: crate::Amount::ZERO, // TODO
            kind: if message.is_tracked {
                MessageKind::Tracked
            } else {
                MessageKind::Simple
            },
            message: message.message,
        }
    }
}

impl From<contract::ExecutionOutcome> for RawExecutionOutcome<Vec<u8>> {
    fn from(outcome: contract::ExecutionOutcome) -> Self {
        let messages = outcome
            .messages
            .into_iter()
            .map(RawOutgoingMessage::from)
            .collect();

        let subscribe = outcome
            .subscribe
            .into_iter()
            .map(|(subscription, chain_id)| (subscription.into(), chain_id.into()))
            .collect();

        let unsubscribe = outcome
            .unsubscribe
            .into_iter()
            .map(|(subscription, chain_id)| (subscription.into(), chain_id.into()))
            .collect();

        RawExecutionOutcome {
            authenticated_signer: None,
            refund_grant_to: None,
            messages,
            subscribe,
            unsubscribe,
        }
    }
}

impl From<contract::Destination> for Destination {
    fn from(guest: contract::Destination) -> Self {
        match guest {
            contract::Destination::Recipient(chain_id) => Destination::Recipient(chain_id.into()),
            contract::Destination::Subscribers(subscription) => {
                Destination::Subscribers(subscription.into())
            }
        }
    }
}

impl From<contract::ChannelName> for ChannelName {
    fn from(guest: contract::ChannelName) -> Self {
        guest.name.into()
    }
}

impl From<contract::CryptoHash> for CryptoHash {
    fn from(guest: contract::CryptoHash) -> Self {
        let integers = [guest.part1, guest.part2, guest.part3, guest.part4];
        CryptoHash::from(integers)
    }
}

impl From<contract::ChainId> for ChainId {
    fn from(guest: contract::ChainId) -> Self {
        ChainId(guest.into())
    }
}

impl From<contract_system_api::SessionId> for SessionId {
    fn from(guest: contract_system_api::SessionId) -> Self {
        SessionId {
            application_id: guest.application_id.into(),
            index: guest.index,
        }
    }
}

impl From<contract_system_api::ApplicationId> for UserApplicationId {
    fn from(guest: contract_system_api::ApplicationId) -> Self {
        UserApplicationId {
            bytecode_id: guest.bytecode_id.into(),
            creation: guest.creation.into(),
        }
    }
}

impl From<contract_system_api::MessageId> for BytecodeId {
    fn from(guest: contract_system_api::MessageId) -> Self {
        BytecodeId::new(guest.into())
    }
}

impl From<contract_system_api::MessageId> for MessageId {
    fn from(guest: contract_system_api::MessageId) -> Self {
        MessageId {
            chain_id: guest.chain_id.into(),
            height: BlockHeight(guest.height),
            index: guest.index,
        }
    }
}

impl From<contract_system_api::CryptoHash> for ChainId {
    fn from(guest: contract_system_api::CryptoHash) -> Self {
        ChainId(guest.into())
    }
}

impl From<contract_system_api::CryptoHash> for CryptoHash {
    fn from(guest: contract_system_api::CryptoHash) -> Self {
        let integers = [guest.part1, guest.part2, guest.part3, guest.part4];
        CryptoHash::from(integers)
    }
}

impl From<service_system_api::ApplicationId> for UserApplicationId {
    fn from(guest: service_system_api::ApplicationId) -> Self {
        UserApplicationId {
            bytecode_id: guest.bytecode_id.into(),
            creation: guest.creation.into(),
        }
    }
}

impl From<service_system_api::MessageId> for BytecodeId {
    fn from(guest: service_system_api::MessageId) -> Self {
        BytecodeId::new(guest.into())
    }
}

impl From<service_system_api::MessageId> for MessageId {
    fn from(guest: service_system_api::MessageId) -> Self {
        MessageId {
            chain_id: guest.chain_id.into(),
            height: BlockHeight(guest.height),
            index: guest.index,
        }
    }
}

impl From<service_system_api::CryptoHash> for ChainId {
    fn from(guest: service_system_api::CryptoHash) -> Self {
        ChainId(guest.into())
    }
}

impl From<service_system_api::CryptoHash> for CryptoHash {
    fn from(guest: service_system_api::CryptoHash) -> Self {
        let integers = [guest.part1, guest.part2, guest.part3, guest.part4];
        CryptoHash::from(integers)
    }
}
