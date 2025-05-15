// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bincode::Error;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlobContent},
    identifiers::BlobType,
};
use linera_chain::types::ConfirmedBlockCertificate;

use super::indexer_api::{self, element::Payload, Blob as IndexerBlob, Block, Element};

impl TryFrom<Arc<ConfirmedBlockCertificate>> for Element {
    type Error = Error;

    fn try_from(value: Arc<ConfirmedBlockCertificate>) -> Result<Self, Self::Error> {
        let bytes = bincode::serialize(value.as_ref())?;
        let element = Element {
            payload: Some(Payload::Block(Block { bytes })),
        };

        Ok(element)
    }
}

impl From<Arc<Blob>> for Element {
    fn from(value: Arc<Blob>) -> Self {
        let blob = IndexerBlob {
            hash: Some(value.id().hash.into()),
            content: Some(value.content().into()),
        };

        Element {
            payload: Some(Payload::Blob(blob)),
        }
    }
}

impl From<CryptoHash> for indexer_api::CryptoHash {
    fn from(value: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(value);
        Self {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<indexer_api::CryptoHash> for CryptoHash {
    fn from(value: indexer_api::CryptoHash) -> Self {
        let arr = [value.part1, value.part2, value.part3, value.part4];
        arr.into()
    }
}

impl TryFrom<indexer_api::Block> for ConfirmedBlockCertificate {
    type Error = Error;

    fn try_from(value: indexer_api::Block) -> Result<ConfirmedBlockCertificate, Self::Error> {
        bincode::deserialize(&value.bytes)
    }
}

impl From<&BlobContent> for indexer_api::BlobContent {
    fn from(value: &BlobContent) -> Self {
        let blob_type = match value.blob_type() {
            BlobType::Data => 0,
            BlobType::ContractBytecode => 1,
            BlobType::ServiceBytecode => 2,
            BlobType::EvmBytecode => 3,
            BlobType::ApplicationDescription => 4,
            BlobType::Committee => 5,
            BlobType::ChainDescription => 6,
        };

        Self {
            blob_type,
            bytes: value.bytes().to_vec(),
        }
    }
}

impl From<indexer_api::Blob> for Blob {
    fn from(value: indexer_api::Blob) -> Self {
        Blob::new(value.content.unwrap().into())
    }
}

impl From<indexer_api::BlobContent> for BlobContent {
    fn from(value: indexer_api::BlobContent) -> Self {
        let blob_type = match value.blob_type {
            0 => BlobType::Data,
            1 => BlobType::ContractBytecode,
            2 => BlobType::ServiceBytecode,
            3 => BlobType::EvmBytecode,
            4 => BlobType::ApplicationDescription,
            5 => BlobType::Committee,
            6 => BlobType::ChainDescription,
            _ => unreachable!(),
        };

        BlobContent::new(blob_type, value.bytes)
    }
}
