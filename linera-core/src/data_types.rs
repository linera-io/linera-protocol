// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::node::NodeError;
use linera_base::{
    crypto::{BcsSignable, CryptoHash, KeyPair, Signature},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_chain::{
    data_types::{Certificate, ChainAndHeight, HashedValue, IncomingMessage, Medium},
    ChainManagerInfo, ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    ExecutionRuntimeContext,
};
use linera_storage::ChainRuntimeContext;
use linera_views::{common::Context, views::ViewError};
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
    pub limit: Option<u64>,
}

impl BlockHeightRange {
    /// Creates a range containing only the single specified block height.
    pub fn single(start: BlockHeight) -> BlockHeightRange {
        let limit = Some(1);
        BlockHeightRange { start, limit }
    }
}

/// Request information about a chain.
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
    /// Query new certificate sender chain IDs and block heights received from the chain.
    pub request_received_log_excluding_first_nth: Option<u64>,
    /// Query values from the chain manager, not just votes.
    pub request_manager_values: bool,
    /// Query a value that contains a binary blob (e.g. bytecode) required by this chain.
    pub request_blob: Option<CryptoHash>,
}

impl ChainInfoQuery {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_committees: false,
            request_pending_messages: false,
            request_sent_certificates_in_range: None,
            request_received_log_excluding_first_nth: None,
            request_manager_values: false,
            request_blob: None,
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

    pub fn with_received_log_excluding_first_nth(mut self, n: u64) -> Self {
        self.request_received_log_excluding_first_nth = Some(n);
        self
    }

    pub fn with_manager_values(mut self) -> Self {
        self.request_manager_values = true;
        self
    }

    pub fn with_blob(mut self, hash: CryptoHash) -> Self {
        self.request_blob = Some(hash);
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
    pub manager: ChainManagerInfo,
    /// The current balance.
    pub system_balance: Amount,
    /// The last block hash, if any.
    pub block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    pub timestamp: Timestamp,
    /// The height after the latest block in the chain.
    pub next_block_height: BlockHeight,
    /// The hash of the current execution state.
    pub state_hash: Option<CryptoHash>,
    /// The current committees.
    pub requested_committees: Option<BTreeMap<Epoch, Committee>>,
    /// The received messages that are waiting be picked in the next block (if requested).
    pub requested_pending_messages: Vec<IncomingMessage>,
    /// The response to `request_sent_certificates_in_range`
    pub requested_sent_certificates: Vec<Certificate>,
    /// The current number of received certificates (useful for `request_received_log_excluding_first_nth`)
    pub count_received_log: usize,
    /// The response to `request_received_certificates_excluding_first_nth`
    pub requested_received_log: Vec<ChainAndHeight>,
    /// The requested blob, if any.
    pub requested_blob: Option<HashedValue>,
}

/// The response to an `ChainInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ChainInfoResponse {
    pub info: ChainInfo,
    pub signature: Option<Signature>,
}

/// An internal request between chains within a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum CrossChainRequest {
    /// Communicate a number of confirmed blocks from the sender to the recipient.
    /// Blocks must be given by increasing heights.
    UpdateRecipient {
        height_map: Vec<(Medium, Vec<BlockHeight>)>,
        sender: ChainId,
        recipient: ChainId,
        certificates: Vec<Certificate>,
    },
    /// Acknowledge the height of the highest confirmed blocks communicated with `UpdateRecipient`.
    ConfirmUpdatedRecipient {
        sender: ChainId,
        recipient: ChainId,
        latest_heights: Vec<(Medium, BlockHeight)>,
    },
}

impl CrossChainRequest {
    /// Where to send the cross-chain request.
    pub fn target_chain_id(&self) -> ChainId {
        use CrossChainRequest::*;
        match self {
            UpdateRecipient { recipient, .. } => *recipient,
            ConfirmUpdatedRecipient { sender, .. } => *sender,
        }
    }

    /// Returns true if the cross-chain request has messages lower or equal than `height`.
    pub fn has_messages_lower_or_equal_than(&self, height: BlockHeight) -> bool {
        match self {
            CrossChainRequest::UpdateRecipient { height_map, .. } => {
                height_map.iter().any(|(_, heights)| {
                    debug_assert!(heights.windows(2).all(|w| w[0] <= w[1]));
                    matches!(heights.get(0), Some(h) if *h <= height)
                })
            }
            _ => false,
        }
    }
}

impl<C, S> From<&ChainStateView<C>> for ChainInfo
where
    C: Context<Extra = ChainRuntimeContext<S>> + Clone + Send + Sync + 'static,
    ChainRuntimeContext<S>: ExecutionRuntimeContext,
    ViewError: From<C::Error>,
{
    fn from(view: &ChainStateView<C>) -> Self {
        let system_state = &view.execution_state.system;
        let tip_state = view.tip_state.get();
        ChainInfo {
            chain_id: view.chain_id(),
            epoch: *system_state.epoch.get(),
            description: *system_state.description.get(),
            manager: ChainManagerInfo::from(view.manager.get()),
            system_balance: *system_state.balance.get(),
            block_hash: tip_state.block_hash,
            next_block_height: tip_state.next_block_height,
            timestamp: *view.execution_state.system.timestamp.get(),
            state_hash: *view.execution_state_hash.get(),
            requested_committees: None,
            requested_pending_messages: Vec::new(),
            requested_sent_certificates: Vec::new(),
            count_received_log: view.received_log.count(),
            requested_received_log: Vec::new(),
            requested_blob: None,
        }
    }
}

impl ChainInfoResponse {
    pub fn new(info: impl Into<ChainInfo>, key_pair: Option<&KeyPair>) -> Self {
        let info = info.into();
        let signature = key_pair.map(|kp| Signature::new(&info, kp));
        Self { info, signature }
    }

    #[allow(clippy::result_large_err)]
    pub fn check(&self, name: ValidatorName) -> Result<(), NodeError> {
        match self.signature {
            Some(sig) => Ok(sig.check(&self.info, name.0)?),
            None => Err(NodeError::InvalidChainInfoResponse),
        }
    }

    /// Returns the committee in the latest epoch.
    pub fn latest_committee(&self) -> Option<&Committee> {
        let committees = self.info.requested_committees.as_ref()?;
        committees.get(&self.info.epoch?)
    }
}

impl BcsSignable for ChainInfo {}
