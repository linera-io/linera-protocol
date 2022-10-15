// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    committee::Committee,
    crypto::{BcsSignable, HashValue, KeyPair, Signature},
    error::Error,
    messages::{
        ApplicationId, BlockHeight, ChainDescription, ChainId, Epoch, Origin, ValidatorName,
    },
};
use linera_chain::{
    messages::{Certificate, MessageGroup},
    ChainManager, ChainStateView, ChainStateViewContext,
};
use linera_execution::{system::Balance, ChainRuntimeContext};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;

/// A range of block heights as used in ChainInfoQuery.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Eq, PartialEq))]
pub struct BlockHeightRange {
    /// Starting point
    pub start: BlockHeight,
    /// Optional limit on the number of elements.
    pub limit: Option<usize>,
}

/// Message to obtain information on a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary, Eq, PartialEq))]
pub struct ChainInfoQuery {
    /// The chain id
    pub chain_id: ChainId,
    /// Optionally test that the block height is the one expected.
    pub test_next_block_height: Option<BlockHeight>,
    /// Query the current committees.
    pub request_committees: bool,
    /// Query the received messages that are waiting be picked in the next block.
    pub request_pending_messages: bool,
    /// Query a range of certificates sent from the chain.
    pub request_sent_certificates_in_range: Option<BlockHeightRange>,
    /// Query new certificates received from the chain.
    pub request_received_certificates_excluding_first_nth: Option<usize>,
}

impl ChainInfoQuery {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_committees: false,
            request_pending_messages: false,
            request_sent_certificates_in_range: None,
            request_received_certificates_excluding_first_nth: None,
        }
    }

    pub fn test_next_block_height(mut self, height: BlockHeight) -> Self {
        self.test_next_block_height = Some(height);
        self
    }

    pub fn with_committees(mut self) -> Self {
        self.request_committees = true;
        self
    }

    pub fn with_pending_messages(mut self) -> Self {
        self.request_pending_messages = true;
        self
    }

    pub fn with_sent_certificates_in_range(mut self, range: BlockHeightRange) -> Self {
        self.request_sent_certificates_in_range = Some(range);
        self
    }

    pub fn with_received_certificates_excluding_first_nth(mut self, n: usize) -> Self {
        self.request_received_certificates_excluding_first_nth = Some(n);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainInfo {
    /// The chain id.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Option<Epoch>,
    /// The chain description.
    pub description: Option<ChainDescription>,
    /// The state of the chain authentication.
    pub manager: ChainManager,
    /// The current balance.
    pub system_balance: Balance,
    /// The last block hash, if any.
    pub block_hash: Option<HashValue>,
    /// The height after the latest block in the chain.
    pub next_block_height: BlockHeight,
    /// The hash of the current execution state.
    pub state_hash: Option<HashValue>,
    /// The current committees.
    pub requested_committees: Option<BTreeMap<Epoch, Committee>>,
    /// The received messages that are waiting be picked in the next block (if requested).
    pub requested_pending_messages: Vec<MessageGroup>,
    /// The response to `request_sent_certificates_in_range`
    pub requested_sent_certificates: Vec<Certificate>,
    /// The current number of received certificates (useful for `request_received_certificates_excluding_first_nth`)
    pub count_received_certificates: usize,
    /// The response to `request_received_certificates_excluding_first_nth`
    pub requested_received_certificates: Vec<Certificate>,
}

/// The response to an `ChainInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainInfoResponse {
    pub info: ChainInfo,
    pub signature: Option<Signature>,
}

/// An internal message between chains within a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
#[allow(clippy::large_enum_variant)]
pub enum CrossChainRequest {
    /// Communicate a number of confirmed blocks from the sender to the recipient.
    /// Blocks must be given by increasing heights.
    UpdateRecipient {
        application_id: ApplicationId,
        origin: Origin,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    },
    /// Acknowledge the height of the highest confirmed block communicated with `UpdateRecipient`.
    ConfirmUpdatedRecipient {
        application_id: ApplicationId,
        origin: Origin,
        recipient: ChainId,
        height: BlockHeight,
    },
}

impl CrossChainRequest {
    /// Where to send the cross-chain request.
    pub fn target_chain_id(&self) -> ChainId {
        use CrossChainRequest::*;
        match self {
            UpdateRecipient { recipient, .. } => *recipient,
            ConfirmUpdatedRecipient { origin, .. } => origin.chain_id,
        }
    }
}

impl<C> From<&ChainStateView<C>> for ChainInfo
where
    C: ChainStateViewContext<Extra = ChainRuntimeContext>,
    Error: From<C::Error>,
{
    fn from(view: &ChainStateView<C>) -> Self {
        let manager = view.manager.get().clone();
        let system_state = &view.execution_state.system;
        let tip_state = view.tip_state.get();
        ChainInfo {
            chain_id: view.chain_id(),
            epoch: *system_state.epoch.get(),
            description: *system_state.description.get(),
            manager,
            system_balance: *system_state.balance.get(),
            block_hash: tip_state.block_hash,
            next_block_height: tip_state.next_block_height,
            state_hash: *view.execution_state_hash.get(),
            requested_committees: None,
            requested_pending_messages: Vec::new(),
            requested_sent_certificates: Vec::new(),
            count_received_certificates: view.received_log.count(),
            requested_received_certificates: Vec::new(),
        }
    }
}

impl ChainInfoResponse {
    pub fn new(info: impl Into<ChainInfo>, key_pair: Option<&KeyPair>) -> Self {
        let info = info.into();
        let signature = key_pair.map(|kp| Signature::new(&info, kp));
        Self { info, signature }
    }

    pub fn check(&self, name: ValidatorName) -> Result<(), Error> {
        match self.signature {
            Some(sig) => sig.check(&self.info, name.0),
            None => Err(Error::InvalidChainInfoResponse),
        }
    }
}

impl BcsSignable for ChainInfo {}
