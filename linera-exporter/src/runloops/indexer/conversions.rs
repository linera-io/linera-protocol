// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use bincode::Error;
use linera_base::data_types::Blob;
use linera_chain::types::ConfirmedBlockCertificate;

use super::indexer_api::{self, element::Payload, Block, Element};

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

impl TryFrom<Arc<Blob>> for Element {
    type Error = Error;

    fn try_from(value: Arc<Blob>) -> Result<Self, Self::Error> {
        let bytes = bincode::serialize(value.as_ref())?;
        let element = Element {
            payload: Some(Payload::Blob(indexer_api::Blob { bytes })),
        };

        Ok(element)
    }
}

impl TryFrom<indexer_api::Block> for ConfirmedBlockCertificate {
    type Error = Error;

    fn try_from(value: indexer_api::Block) -> Result<ConfirmedBlockCertificate, Self::Error> {
        bincode::deserialize(&value.bytes)
    }
}

impl TryFrom<indexer_api::Blob> for Blob {
    type Error = Error;

    fn try_from(value: indexer_api::Blob) -> Result<Self, Self::Error> {
        bincode::deserialize(&value.bytes)
    }
}
