// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from WIT types to the original types.

use linera_base::{
    crypto::CryptoHash,
    data_types::BlockHeight,
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId},
};
use linera_views::batch::WriteOperation;

use super::wit;

impl From<wit::WriteOperation> for WriteOperation {
    fn from(operation: wit::WriteOperation) -> Self {
        match operation {
            wit::WriteOperation::Delete(key) => WriteOperation::Delete { key },
            wit::WriteOperation::DeletePrefix(key_prefix) => {
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

impl From<wit::BytecodeId> for BytecodeId {
    fn from(bytecode_id: wit::BytecodeId) -> Self {
        BytecodeId::new(bytecode_id.message_id.into())
    }
}

impl From<wit::MessageId> for MessageId {
    fn from(message_id: wit::MessageId) -> Self {
        MessageId {
            chain_id: message_id.chain_id.into(),
            height: BlockHeight(message_id.height.inner0),
            index: message_id.index,
        }
    }
}

impl From<wit::ChainId> for ChainId {
    fn from(chain_id: wit::ChainId) -> Self {
        ChainId(chain_id.inner0.into())
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
