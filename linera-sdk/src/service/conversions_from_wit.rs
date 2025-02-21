// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types generated by [`wit-bindgen-guest-rust`] to types declared in [`linera-sdk`].

use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Timestamp},
    http,
    identifiers::{AccountOwner, ApplicationId, BytecodeId, ChainId, MessageId, Owner},
};

use super::wit::service_system_api as wit_system_api;

impl From<wit_system_api::CryptoHash> for ChainId {
    fn from(hash_value: wit_system_api::CryptoHash) -> Self {
        ChainId(hash_value.into())
    }
}

impl From<wit_system_api::AccountOwner> for AccountOwner {
    fn from(account_owner: wit_system_api::AccountOwner) -> Self {
        match account_owner {
            wit_system_api::AccountOwner::User(owner) => AccountOwner::User(owner.into()),
            wit_system_api::AccountOwner::Application(owner) => {
                AccountOwner::Application(owner.into())
            }
        }
    }
}

impl From<wit_system_api::Owner> for Owner {
    fn from(owner: wit_system_api::Owner) -> Self {
        Owner(owner.inner0.into())
    }
}

impl From<wit_system_api::Amount> for Amount {
    fn from(balance: wit_system_api::Amount) -> Self {
        let (lower_half, upper_half) = balance.inner0;
        let value = ((upper_half as u128) << 64) | (lower_half as u128);
        Amount::from_attos(value)
    }
}

impl From<wit_system_api::BlockHeight> for BlockHeight {
    fn from(block_height: wit_system_api::BlockHeight) -> Self {
        BlockHeight(block_height.inner0)
    }
}

impl From<wit_system_api::ChainId> for ChainId {
    fn from(chain_id: wit_system_api::ChainId) -> Self {
        ChainId(chain_id.inner0.into())
    }
}

impl From<wit_system_api::CryptoHash> for CryptoHash {
    fn from(hash_value: wit_system_api::CryptoHash) -> Self {
        CryptoHash::from([
            hash_value.part1,
            hash_value.part2,
            hash_value.part3,
            hash_value.part4,
        ])
    }
}

impl From<wit_system_api::MessageId> for MessageId {
    fn from(message_id: wit_system_api::MessageId) -> Self {
        MessageId {
            chain_id: message_id.chain_id.into(),
            height: message_id.height.into(),
            index: message_id.index,
        }
    }
}

impl From<wit_system_api::ApplicationId> for ApplicationId {
    fn from(application_id: wit_system_api::ApplicationId) -> Self {
        ApplicationId {
            bytecode_id: application_id.bytecode_id.into(),
            creation: application_id.creation.into(),
        }
    }
}

impl From<wit_system_api::BytecodeId> for BytecodeId {
    fn from(bytecode_id: wit_system_api::BytecodeId) -> Self {
        BytecodeId::new(
            bytecode_id.contract_blob_hash.into(),
            bytecode_id.service_blob_hash.into(),
        )
    }
}

impl From<wit_system_api::Timestamp> for Timestamp {
    fn from(timestamp: wit_system_api::Timestamp) -> Self {
        Timestamp::from(timestamp.inner0)
    }
}

impl From<wit_system_api::HttpResponse> for http::Response {
    fn from(response: wit_system_api::HttpResponse) -> http::Response {
        http::Response {
            status: response.status,
            headers: response
                .headers
                .into_iter()
                .map(http::Header::from)
                .collect(),
            body: response.body,
        }
    }
}

impl From<wit_system_api::HttpHeader> for http::Header {
    fn from(header: wit_system_api::HttpHeader) -> http::Header {
        http::Header::new(header.name, header.value)
    }
}
