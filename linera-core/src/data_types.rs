// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ops::Not};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::{BcsSignable, CryptoError, CryptoHash, KeyPair, Signature},
    data_types::{Amount, BlockHeight, Round, Timestamp},
    identifiers::{AccountOwner, ChainDescription, ChainId},
};
use linera_chain::{
    data_types::{ChainAndHeight, IncomingBundle, Medium, MessageBundle},
    manager::ChainManagerInfo,
    ChainStateView,
};
use linera_execution::{
    committee::{Committee, Epoch, ValidatorName},
    ExecutionRuntimeContext,
};
use linera_storage::ChainRuntimeContext;
use linera_views::context::Context;
use serde::{Deserialize, Serialize};

use crate::client::ChainClientError;

/// A range of block heights as used in ChainInfoQuery.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary, Eq, PartialEq))]
pub struct BlockHeightRange {
    /// Starting point
    pub start: BlockHeight,
    /// Optional limit on the number of elements.
    #[debug(skip_if = Option::is_none)]
    pub limit: Option<u64>,
}

impl BlockHeightRange {
    /// Creates a range containing only the single specified block height.
    pub fn single(start: BlockHeight) -> BlockHeightRange {
        let limit = Some(1);
        BlockHeightRange { start, limit }
    }

    /// Creates a range starting at the specified block height and containing up to `limit` elements.
    pub fn multi(start: BlockHeight, limit: u64) -> BlockHeightRange {
        BlockHeightRange {
            start,
            limit: Some(limit),
        }
    }

    /// Returns the highest block height in the range.
    pub fn highest(&self) -> BlockHeight {
        self.limit
            .map_or(self.start, |limit| BlockHeight(self.start.0 + limit - 1))
    }
}

/// Request information about a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary, Eq, PartialEq))]
pub struct ChainInfoQuery {
    /// The chain ID.
    pub chain_id: ChainId,
    /// Optionally test that the block height is the one expected.
    #[debug(skip_if = Option::is_none)]
    pub test_next_block_height: Option<BlockHeight>,
    /// Request the balance of a given [`AccountOwner`].
    #[debug(skip_if = Option::is_none)]
    pub request_owner_balance: Option<AccountOwner>,
    /// Query the current committees.
    #[debug(skip_if = Not::not)]
    pub request_committees: bool,
    /// Query the received messages that are waiting be picked in the next block.
    #[debug(skip_if = Not::not)]
    pub request_pending_message_bundles: bool,
    /// Query a range of certificate hashes sent from the chain.
    #[debug(skip_if = Option::is_none)]
    pub request_sent_certificate_hashes_in_range: Option<BlockHeightRange>,
    /// Query new certificate sender chain IDs and block heights received from the chain.
    #[debug(skip_if = Option::is_none)]
    pub request_received_log_excluding_first_n: Option<u64>,
    /// Query values from the chain manager, not just votes.
    #[debug(skip_if = Not::not)]
    pub request_manager_values: bool,
    /// Include a timeout vote for the current round, if appropriate.
    #[debug(skip_if = Not::not)]
    pub request_leader_timeout: bool,
    /// Include a vote to switch to fallback mode, if appropriate.
    #[debug(skip_if = Not::not)]
    pub request_fallback: bool,
}

impl ChainInfoQuery {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_committees: false,
            request_owner_balance: None,
            request_pending_message_bundles: false,
            request_sent_certificate_hashes_in_range: None,
            request_received_log_excluding_first_n: None,
            request_manager_values: false,
            request_leader_timeout: false,
            request_fallback: false,
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

    pub fn with_owner_balance(mut self, owner: AccountOwner) -> Self {
        self.request_owner_balance = Some(owner);
        self
    }

    pub fn with_pending_message_bundles(mut self) -> Self {
        self.request_pending_message_bundles = true;
        self
    }

    pub fn with_sent_certificate_hashes_in_range(mut self, range: BlockHeightRange) -> Self {
        self.request_sent_certificate_hashes_in_range = Some(range);
        self
    }

    pub fn with_received_log_excluding_first_n(mut self, n: u64) -> Self {
        self.request_received_log_excluding_first_n = Some(n);
        self
    }

    pub fn with_manager_values(mut self) -> Self {
        self.request_manager_values = true;
        self
    }

    pub fn with_timeout(mut self) -> Self {
        self.request_leader_timeout = true;
        self
    }

    pub fn with_fallback(mut self) -> Self {
        self.request_fallback = true;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ChainInfo {
    /// The chain ID.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    #[debug(skip_if = Option::is_none)]
    pub epoch: Option<Epoch>,
    /// The chain description.
    #[debug(skip_if = Option::is_none)]
    pub description: Option<ChainDescription>,
    /// The state of the chain authentication.
    pub manager: Box<ChainManagerInfo>,
    /// The current balance.
    pub chain_balance: Amount,
    /// The last block hash, if any.
    #[debug(skip_if = Option::is_none)]
    pub block_hash: Option<CryptoHash>,
    /// The earliest possible timestamp for the next block.
    pub timestamp: Timestamp,
    /// The height after the latest block in the chain.
    pub next_block_height: BlockHeight,
    /// The hash of the current execution state.
    #[debug(skip_if = Option::is_none)]
    pub state_hash: Option<CryptoHash>,
    /// The requested owner balance, if any.
    #[debug(skip_if = Option::is_none)]
    pub requested_owner_balance: Option<Amount>,
    /// The current committees.
    #[debug(skip_if = Option::is_none)]
    pub requested_committees: Option<BTreeMap<Epoch, Committee>>,
    /// The received messages that are waiting be picked in the next block (if requested).
    #[debug(skip_if = Vec::is_empty)]
    pub requested_pending_message_bundles: Vec<IncomingBundle>,
    /// The response to `request_sent_certificate_hashes_in_range`
    #[debug(skip_if = Vec::is_empty)]
    pub requested_sent_certificate_hashes: Vec<CryptoHash>,
    /// The current number of received certificates (useful for `request_received_log_excluding_first_n`)
    pub count_received_log: usize,
    /// The response to `request_received_certificates_excluding_first_n`
    #[debug(skip_if = Vec::is_empty)]
    pub requested_received_log: Vec<ChainAndHeight>,
}

impl ChainInfo {
    /// Returns the `RoundTimeout` value for the current round, or `None` if the current round
    /// does not time out.
    pub fn round_timeout(&self) -> Option<RoundTimeout> {
        // TODO(#1424): The local timeout might not match the validators' exactly.
        Some(RoundTimeout {
            timestamp: self.manager.round_timeout?,
            current_round: self.manager.current_round,
            next_block_height: self.next_block_height,
        })
    }
}

/// The response to an `ChainInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ChainInfoResponse {
    pub info: Box<ChainInfo>,
    pub signature: Option<Signature>,
}

/// An internal request between chains within a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub enum CrossChainRequest {
    /// Communicate a number of confirmed blocks from the sender to the recipient.
    /// Blocks must be given by increasing heights.
    UpdateRecipient {
        sender: ChainId,
        recipient: ChainId,
        bundle_vecs: Vec<(Medium, Vec<(Epoch, MessageBundle)>)>,
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
            CrossChainRequest::UpdateRecipient { bundle_vecs, .. } => {
                bundle_vecs.iter().any(|(_, bundles)| {
                    debug_assert!(bundles.windows(2).all(|w| w[0].1.height <= w[1].1.height));
                    matches!(bundles.first(), Some((_, h)) if h.height <= height)
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
{
    fn from(view: &ChainStateView<C>) -> Self {
        let system_state = &view.execution_state.system;
        let tip_state = view.tip_state.get();
        ChainInfo {
            chain_id: view.chain_id(),
            epoch: *system_state.epoch.get(),
            description: *system_state.description.get(),
            manager: Box::new(ChainManagerInfo::from(&view.manager)),
            chain_balance: *system_state.balance.get(),
            block_hash: tip_state.block_hash,
            next_block_height: tip_state.next_block_height,
            timestamp: *view.execution_state.system.timestamp.get(),
            state_hash: *view.execution_state_hash.get(),
            requested_committees: None,
            requested_owner_balance: None,
            requested_pending_message_bundles: Vec::new(),
            requested_sent_certificate_hashes: Vec::new(),
            count_received_log: view.received_log.count(),
            requested_received_log: Vec::new(),
        }
    }
}

impl ChainInfoResponse {
    pub fn new(info: impl Into<ChainInfo>, key_pair: Option<&KeyPair>) -> Self {
        let info = Box::new(info.into());
        let signature = key_pair.map(|kp| Signature::new(&*info, kp));
        Self { info, signature }
    }

    /// Signs the [`ChainInfo`] stored inside this [`ChainInfoResponse`] with the provided
    /// [`KeyPair`].
    pub fn sign(&mut self, key_pair: &KeyPair) {
        self.signature = Some(Signature::new(&*self.info, key_pair));
    }

    pub fn check(&self, name: &ValidatorName) -> Result<(), CryptoError> {
        Signature::check_optional_signature(self.signature.as_ref(), &*self.info, &name.0)
    }

    /// Returns the committee in the latest epoch.
    pub fn latest_committee(&self) -> Option<&Committee> {
        let committees = self.info.requested_committees.as_ref()?;
        committees.get(&self.info.epoch?)
    }
}

impl<'de> BcsSignable<'de> for ChainInfo {}

/// The outcome of trying to commit a list of operations to the chain.
#[derive(Debug)]
pub enum ClientOutcome<T> {
    /// The operations were committed successfully.
    Committed(T),
    /// We are not the round leader and cannot do anything. Try again at the specified time or
    /// or whenever the round or block height changes.
    WaitForTimeout(RoundTimeout),
}

#[derive(Debug)]
pub struct RoundTimeout {
    pub timestamp: Timestamp,
    pub current_round: Round,
    pub next_block_height: BlockHeight,
}

impl<T> ClientOutcome<T> {
    #[cfg(with_testing)]
    pub fn unwrap(self) -> T {
        match self {
            ClientOutcome::Committed(t) => t,
            ClientOutcome::WaitForTimeout(_) => panic!(),
        }
    }

    pub fn expect(self, msg: &'static str) -> T {
        match self {
            ClientOutcome::Committed(t) => t,
            ClientOutcome::WaitForTimeout(_) => panic!("{}", msg),
        }
    }

    pub fn map<F, S>(self, f: F) -> ClientOutcome<S>
    where
        F: FnOnce(T) -> S,
    {
        match self {
            ClientOutcome::Committed(t) => ClientOutcome::Committed(f(t)),
            ClientOutcome::WaitForTimeout(timeout) => ClientOutcome::WaitForTimeout(timeout),
        }
    }

    pub fn try_map<F, S>(self, f: F) -> Result<ClientOutcome<S>, ChainClientError>
    where
        F: FnOnce(T) -> Result<S, ChainClientError>,
    {
        match self {
            ClientOutcome::Committed(t) => Ok(ClientOutcome::Committed(f(t)?)),
            ClientOutcome::WaitForTimeout(timeout) => Ok(ClientOutcome::WaitForTimeout(timeout)),
        }
    }
}
