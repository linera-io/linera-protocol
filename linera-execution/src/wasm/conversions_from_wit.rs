// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types generated by `wit-bindgen`.
//!
//! Allows converting types returned from a Wasm module into types that can be used with the rest
//! of the crate.

#![allow(clippy::duplicate_mod)]

use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, BlockHeight, Resources, SendMessageRequest, TimeDelta},
    identifiers::{Account, BytecodeId, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};

use super::{contract_system_api, service_system_api};
use crate::{ChannelName, Destination, UserApplicationId};

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

impl From<contract_system_api::PublicKey> for PublicKey {
    fn from(guest: contract_system_api::PublicKey) -> PublicKey {
        let contract_system_api::PublicKey {
            part1,
            part2,
            part3,
            part4,
        } = guest;
        [part1, part2, part3, part4].into()
    }
}

impl From<contract_system_api::TimeoutConfig> for TimeoutConfig {
    fn from(guest: contract_system_api::TimeoutConfig) -> TimeoutConfig {
        let contract_system_api::TimeoutConfig {
            fast_round_duration_us,
            base_timeout_us,
            timeout_increment_us,
        } = guest;
        TimeoutConfig {
            fast_round_duration: fast_round_duration_us.map(TimeDelta::from_micros),
            base_timeout: TimeDelta::from_micros(base_timeout_us),
            timeout_increment: TimeDelta::from_micros(timeout_increment_us),
        }
    }
}

impl<'a> From<contract_system_api::ChainOwnershipParam<'a>> for ChainOwnership {
    fn from(guest: contract_system_api::ChainOwnershipParam<'a>) -> ChainOwnership {
        let contract_system_api::ChainOwnershipParam {
            super_owners,
            owners,
            multi_leader_rounds,
            timeout_config,
        } = guest;
        let super_owners = super_owners.iter().map(|le| {
            let pub_key = PublicKey::from(le.get());
            (Owner::from(pub_key), pub_key)
        });
        let owners = owners.iter().map(|le| {
            let (pub_key, weight) = le.get();
            let pub_key = PublicKey::from(pub_key);
            (Owner::from(pub_key), (pub_key, weight))
        });
        ChainOwnership {
            super_owners: super_owners.collect(),
            owners: owners.collect(),
            multi_leader_rounds,
            timeout_config: timeout_config.into(),
        }
    }
}

impl From<contract_system_api::CryptoHash> for Owner {
    fn from(guest: contract_system_api::CryptoHash) -> Self {
        let integers = [guest.part1, guest.part2, guest.part3, guest.part4];
        Owner(CryptoHash::from(integers))
    }
}

impl From<contract_system_api::Account> for Account {
    fn from(account: contract_system_api::Account) -> Self {
        Account {
            chain_id: account.chain_id.into(),
            owner: account.owner.map(|owner| owner.into()),
        }
    }
}

impl From<contract_system_api::Amount> for Amount {
    fn from(amount: contract_system_api::Amount) -> Self {
        let value = ((amount.upper_half as u128) << 64) | (amount.lower_half as u128);
        Amount::from_attos(value)
    }
}

impl<'a> From<contract_system_api::SendMessageRequest<'a>> for SendMessageRequest<Vec<u8>> {
    fn from(message: contract_system_api::SendMessageRequest<'a>) -> Self {
        Self {
            destination: message.destination.into(),
            authenticated: message.authenticated,
            is_tracked: message.is_tracked,
            grant: message.resources.into(),
            message: message.message.to_vec(),
        }
    }
}

impl From<contract_system_api::Resources> for Resources {
    fn from(value: contract_system_api::Resources) -> Self {
        Self {
            fuel: value.fuel,
            read_operations: value.read_operations,
            write_operations: value.write_operations,
            bytes_to_read: value.bytes_to_read,
            bytes_to_write: value.bytes_to_write,
            messages: value.messages,
            message_size: value.message_size,
            storage_size_delta: value.storage_size_delta,
        }
    }
}

impl<'a> From<contract_system_api::Destination<'a>> for Destination {
    fn from(guest: contract_system_api::Destination<'a>) -> Self {
        match guest {
            contract_system_api::Destination::Recipient(chain_id) => {
                Destination::Recipient(chain_id.into())
            }
            contract_system_api::Destination::Subscribers(subscription) => {
                Destination::Subscribers(subscription.into())
            }
        }
    }
}

impl<'a> From<contract_system_api::ChannelName<'a>> for ChannelName {
    fn from(guest: contract_system_api::ChannelName<'a>) -> Self {
        guest.name.to_vec().into()
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

impl From<service_system_api::CryptoHash> for Owner {
    fn from(guest: service_system_api::CryptoHash) -> Self {
        let integers = [guest.part1, guest.part2, guest.part3, guest.part4];
        Owner(CryptoHash::from(integers))
    }
}
