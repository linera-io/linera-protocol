// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt, ops::Not};

use custom_debug_derive::Debug;
use linera_base::{
    crypto::{
        BcsSignable, CryptoError, CryptoHash, ValidatorPublicKey, ValidatorSecretKey,
        ValidatorSignature,
    },
    data_types::{Amount, BlockHeight, ChainDescription, Epoch, Round, Timestamp},
    identifiers::{AccountOwner, ChainId, StreamId},
};
use linera_chain::{
    data_types::{ChainAndHeight, IncomingBundle, MessageBundle},
    manager::ChainManagerInfo,
    types::ConfirmedBlockCertificate,
    ChainStateView,
};
use linera_execution::{committee::Committee, ExecutionRuntimeContext};
use linera_storage::ChainRuntimeContext;
use linera_views::context::Context;
use serde::{Deserialize, Serialize};

use crate::client::chain_client;

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
    pub request_owner_balance: AccountOwner,
    /// Query the current committees.
    #[debug(skip_if = Not::not)]
    pub request_committees: bool,
    /// Query the received messages that are waiting to be picked in the next block.
    #[debug(skip_if = Not::not)]
    pub request_pending_message_bundles: bool,
    /// Query new certificate sender chain IDs and block heights received from the chain.
    #[debug(skip_if = Option::is_none)]
    pub request_received_log_excluding_first_n: Option<u64>,
    /// Query values from the chain manager, not just votes.
    #[debug(skip_if = Not::not)]
    pub request_manager_values: bool,
    /// Include a timeout vote for the specified round, if appropriate.
    #[debug(skip_if = Option::is_none)]
    pub request_leader_timeout: Option<(BlockHeight, Round)>,
    /// Include a vote to switch to fallback mode, if appropriate.
    #[debug(skip_if = Not::not)]
    pub request_fallback: bool,
    /// Query for certificate hashes at block heights.
    #[debug(skip_if = Vec::is_empty)]
    pub request_sent_certificate_hashes_by_heights: Vec<BlockHeight>,
    /// Query the previous event blocks for specific streams.
    #[debug(skip_if = Vec::is_empty)]
    #[cfg_attr(with_testing, strategy(proptest::strategy::Just(Vec::new())))]
    pub request_previous_event_blocks: Vec<StreamId>,
    #[serde(default = "default_true")]
    pub create_network_actions: bool,
}

// Default value for create_network_actions.
// Default for bool returns false.
fn default_true() -> bool {
    true
}

impl ChainInfoQuery {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_committees: false,
            request_owner_balance: AccountOwner::CHAIN,
            request_pending_message_bundles: false,
            request_received_log_excluding_first_n: None,
            request_manager_values: false,
            request_leader_timeout: None,
            request_fallback: false,
            request_sent_certificate_hashes_by_heights: Vec::new(),
            request_previous_event_blocks: Vec::new(),
            create_network_actions: false,
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
        self.request_owner_balance = owner;
        self
    }

    pub fn with_pending_message_bundles(mut self) -> Self {
        self.request_pending_message_bundles = true;
        self
    }

    pub fn with_sent_certificate_hashes_by_heights(mut self, heights: Vec<BlockHeight>) -> Self {
        self.request_sent_certificate_hashes_by_heights = heights;
        self
    }

    pub fn with_previous_event_blocks(mut self, stream_ids: Vec<StreamId>) -> Self {
        self.request_previous_event_blocks = stream_ids;
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

    pub fn with_timeout(mut self, height: BlockHeight, round: Round) -> Self {
        self.request_leader_timeout = Some((height, round));
        self
    }

    pub fn with_fallback(mut self) -> Self {
        self.request_fallback = true;
        self
    }

    pub fn with_network_actions(mut self) -> Self {
        self.create_network_actions = true;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ChainInfo {
    /// The chain ID.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Epoch,
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
    /// The response to `request_sent_certificate_hashes_by_heights`.
    #[debug(skip_if = Vec::is_empty)]
    pub requested_sent_certificate_hashes: Vec<CryptoHash>,
    /// The current number of received certificates (useful for `request_received_log_excluding_first_n`)
    pub count_received_log: usize,
    /// The response to `request_received_certificates_excluding_first_n`
    #[debug(skip_if = Vec::is_empty)]
    pub requested_received_log: Vec<ChainAndHeight>,
    /// The response to `request_previous_event_blocks`.
    #[debug(skip_if = BTreeMap::is_empty)]
    pub requested_previous_event_blocks: BTreeMap<StreamId, (BlockHeight, CryptoHash)>,
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
    pub signature: Option<ValidatorSignature>,
}

/// Information about shard allocation for a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ShardInfo {
    /// The shard ID that will process this chain.
    pub shard_id: usize,
    /// The total number of shards in the validator.
    pub total_shards: usize,
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
        bundles: Vec<(Epoch, MessageBundle)>,
        /// The height of the sender's previous block that sent messages to this
        /// recipient (before the first bundle in this request). `None` if the first
        /// bundle is the first message ever sent to this recipient.
        previous_height: Option<BlockHeight>,
    },
    /// Acknowledge the height of the highest confirmed blocks communicated with `UpdateRecipient`.
    ConfirmUpdatedRecipient {
        sender: ChainId,
        recipient: ChainId,
        latest_height: BlockHeight,
    },
    /// Request the sender to revert a previous confirmation and resend bundles
    /// starting from the given height. This is used to recover from state
    /// inconsistencies where the recipient lost persisted state after a
    /// confirmation was sent.
    RevertConfirm {
        sender: ChainId,
        recipient: ChainId,
        retransmit_from: BlockHeight,
    },
}

impl CrossChainRequest {
    /// Where to send the cross-chain request.
    pub fn target_chain_id(&self) -> ChainId {
        use CrossChainRequest::*;
        match self {
            UpdateRecipient { recipient, .. } => *recipient,
            ConfirmUpdatedRecipient { sender, .. } => *sender,
            RevertConfirm { sender, .. } => *sender,
        }
    }

    /// Returns true if the cross-chain request has messages lower or equal than `height`.
    pub fn has_messages_lower_or_equal_than(&self, height: BlockHeight) -> bool {
        match self {
            CrossChainRequest::UpdateRecipient { bundles, .. } => {
                debug_assert!(bundles.windows(2).all(|w| w[0].1.height <= w[1].1.height));
                matches!(bundles.first(), Some((_, h)) if h.height <= height)
            }
            _ => false,
        }
    }
}

impl<C, S> From<&ChainStateView<C>> for ChainInfo
where
    C: Context<Extra = ChainRuntimeContext<S>> + Clone + 'static,
    ChainRuntimeContext<S>: ExecutionRuntimeContext,
{
    fn from(view: &ChainStateView<C>) -> Self {
        let system_state = &view.execution_state.system;
        let tip_state = view.tip_state.get();
        ChainInfo {
            chain_id: view.chain_id(),
            epoch: *system_state.epoch.get(),
            description: system_state.description.get().clone(),
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
            requested_previous_event_blocks: BTreeMap::new(),
        }
    }
}

impl ChainInfoResponse {
    pub fn new(info: impl Into<ChainInfo>, key_pair: Option<&ValidatorSecretKey>) -> Self {
        let info = Box::new(info.into());
        let signature = key_pair.map(|kp| ValidatorSignature::new(&*info, kp));
        Self { info, signature }
    }

    /// Signs the [`ChainInfo`] stored inside this [`ChainInfoResponse`] with the provided
    /// [`ValidatorSecretKey`].
    pub fn sign(&mut self, key_pair: &ValidatorSecretKey) {
        self.signature = Some(ValidatorSignature::new(&*self.info, key_pair));
    }

    pub fn check(&self, public_key: ValidatorPublicKey) -> Result<(), CryptoError> {
        match self.signature.as_ref() {
            Some(sig) => sig.check(&*self.info, public_key),
            None => Err(CryptoError::MissingValidatorSignature),
        }
    }
}

impl BcsSignable<'_> for ChainInfo {}

/// Request for downloading certificates by heights.
#[derive(Debug, Clone)]
pub struct CertificatesByHeightRequest {
    pub chain_id: ChainId,
    pub heights: Vec<BlockHeight>,
}

/// The outcome of trying to commit a list of operations to the chain.
#[derive(Debug)]
pub enum ClientOutcome<T> {
    /// The operations were committed successfully.
    Committed(T),
    /// We are not the round leader and cannot do anything. Try again at the specified time
    /// or whenever the round or block height changes.
    WaitForTimeout(RoundTimeout),
    /// A different block was committed at the current block height.
    Conflict(Box<ConfirmedBlockCertificate>),
}

#[derive(Debug)]
pub struct RoundTimeout {
    pub timestamp: Timestamp,
    pub current_round: Round,
    pub next_block_height: BlockHeight,
}

impl fmt::Display for RoundTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at height {} times out at {}",
            self.current_round, self.next_block_height, self.timestamp
        )
    }
}

impl<T> ClientOutcome<T> {
    #[cfg(with_testing)]
    pub fn unwrap(self) -> T {
        match self {
            ClientOutcome::Committed(t) => t,
            ClientOutcome::WaitForTimeout(timeout) => panic!("unexpected timeout: {timeout}"),
            ClientOutcome::Conflict(certificate) => {
                panic!("unexpected conflict: {}", certificate.hash())
            }
        }
    }

    pub fn expect(self, msg: &'static str) -> T {
        match self {
            ClientOutcome::Committed(t) => t,
            ClientOutcome::WaitForTimeout(_) | ClientOutcome::Conflict(_) => panic!("{}", msg),
        }
    }

    pub fn map<F, S>(self, f: F) -> ClientOutcome<S>
    where
        F: FnOnce(T) -> S,
    {
        match self {
            ClientOutcome::Committed(t) => ClientOutcome::Committed(f(t)),
            ClientOutcome::WaitForTimeout(timeout) => ClientOutcome::WaitForTimeout(timeout),
            ClientOutcome::Conflict(certificate) => ClientOutcome::Conflict(certificate),
        }
    }

    pub fn try_map<F, S>(self, f: F) -> Result<ClientOutcome<S>, chain_client::Error>
    where
        F: FnOnce(T) -> Result<S, chain_client::Error>,
    {
        match self {
            ClientOutcome::Committed(t) => Ok(ClientOutcome::Committed(f(t)?)),
            ClientOutcome::WaitForTimeout(timeout) => Ok(ClientOutcome::WaitForTimeout(timeout)),
            ClientOutcome::Conflict(certificate) => Ok(ClientOutcome::Conflict(certificate)),
        }
    }
}
