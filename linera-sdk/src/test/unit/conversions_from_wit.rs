// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from WIT types to the original types.

use super::wit;
use linera_base::{
    crypto::CryptoHash,
    identifiers::{ApplicationId, BytecodeId, ChainId, EffectId},
};
use linera_views::batch::WriteOperation;

impl From<wit::WriteOperation> for WriteOperation {
    fn from(operation: wit::WriteOperation) -> Self {
        match operation {
            wit::WriteOperation::Delete(key) => WriteOperation::Delete { key },
            wit::WriteOperation::Deleteprefix(key_prefix) => {
                WriteOperation::DeletePrefix { key_prefix }
            }
            wit::WriteOperation::Put((key, value)) => WriteOperation::Put { key, value },
        }
    }
}

impl From<wit::ApplicationId> for ApplicationId {
    fn from(application_id: wit::ApplicationId) -> Self {
        ApplicationId {
            bytecode_id: application_id.bytecode_id.into(),
            creation: application_id.creation.into(),
        }
    }
}

impl From<wit::EffectId> for BytecodeId {
    fn from(effect_id: wit::EffectId) -> Self {
        EffectId::from(effect_id).into()
    }
}

impl From<wit::EffectId> for EffectId {
    fn from(effect_id: wit::EffectId) -> Self {
        EffectId {
            chain_id: effect_id.chain_id.into(),
            height: effect_id.height.into(),
            index: effect_id.index,
        }
    }
}

impl From<wit::CryptoHash> for ChainId {
    fn from(crypto_hash: wit::CryptoHash) -> Self {
        ChainId(crypto_hash.into())
    }
}

impl From<wit::CryptoHash> for CryptoHash {
    fn from(crypto_hash: wit::CryptoHash) -> Self {
        CryptoHash::from([
            crypto_hash.part1,
            crypto_hash.part2,
            crypto_hash.part3,
            crypto_hash.part4,
        ])
    }
}
