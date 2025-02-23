// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*!
Common definitions for the Linera faucet.
*/

use linera_base::{
    crypto::CryptoHash,
    identifiers::{ChainId, MessageId},
};

/// The result of a successful `claim` mutation.
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
#[serde_with::serde_as]
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimOutcome {
    /// The ID of the message that created the new chain.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub message_id: MessageId,
    /// The ID of the new chain.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}
