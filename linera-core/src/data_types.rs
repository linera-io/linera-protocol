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
    data_types::{
        Amount, BlockHeight, ChainDescription, Epoch, Round, SignedCommitmentManifest, Timestamp,
    },
    identifiers::{AccountOwner, ChainId, StreamId},
};
use linera_chain::{
    data_types::{ChainAndHeight, IncomingBundle, MessageBundle},
    manager::ChainManagerInfo,
    types::ConfirmedBlockCertificate,
    ChainStateView,
};
use linera_execution::ExecutionRuntimeContext;
use linera_storage::ChainRuntimeContext;
use linera_views::{context::Context, ViewError};
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
    #[debug(skip_if = Vec::is_empty, with = "debug_compressed_heights")]
    pub request_sent_certificate_hashes_by_heights: Vec<BlockHeight>,
    /// Query the previous event blocks for specific streams.
    #[debug(skip_if = Vec::is_empty)]
    #[cfg_attr(with_testing, strategy(proptest::strategy::Just(Vec::new())))]
    pub request_previous_event_blocks: Vec<StreamId>,
    /// Query the height of the most recent block on this chain whose certificate
    /// records an [`OracleResponse::Checkpoint`][linera_base::data_types::OracleResponse]. A bootstrapping client uses this
    /// to skip downloading and replaying pre-checkpoint blocks.
    #[debug(skip_if = Not::not)]
    pub request_latest_checkpoint_height: bool,
    /// Query the validator's own pending epoch commitments, so the admin chain's
    /// proposer can register them.
    #[debug(skip_if = Not::not)]
    pub request_pending_commitments: bool,
}

impl ChainInfoQuery {
    /// Creates a query for the given chain that requests no optional information.
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            test_next_block_height: None,
            request_owner_balance: AccountOwner::CHAIN,
            request_pending_message_bundles: false,
            request_received_log_excluding_first_n: None,
            request_manager_values: false,
            request_leader_timeout: None,
            request_fallback: false,
            request_sent_certificate_hashes_by_heights: Vec::new(),
            request_previous_event_blocks: Vec::new(),
            request_latest_checkpoint_height: false,
            request_pending_commitments: false,
        }
    }

    /// Also requests the height of the most recent checkpoint block.
    pub fn with_latest_checkpoint_height(mut self) -> Self {
        self.request_latest_checkpoint_height = true;
        self
    }

    /// Also requests the validator's own pending epoch commitments.
    pub fn with_pending_commitments(mut self) -> Self {
        self.request_pending_commitments = true;
        self
    }

    /// Also requests the messages waiting to be picked in the next block.
    pub fn with_pending_message_bundles(mut self) -> Self {
        self.request_pending_message_bundles = true;
        self
    }

    /// Also requests the certificate hashes sent at the given block heights.
    pub fn with_sent_certificate_hashes_by_heights(mut self, heights: Vec<BlockHeight>) -> Self {
        self.request_sent_certificate_hashes_by_heights = heights;
        self
    }

    /// Also requests the previous event blocks for the given streams.
    pub fn with_previous_event_blocks(mut self, stream_ids: Vec<StreamId>) -> Self {
        self.request_previous_event_blocks = stream_ids;
        self
    }

    /// Also requests the received log entries, excluding the first `n`.
    pub fn with_received_log_excluding_first_n(mut self, n: u64) -> Self {
        self.request_received_log_excluding_first_n = Some(n);
        self
    }

    /// Also requests the values from the chain manager, not just the votes.
    pub fn with_manager_values(mut self) -> Self {
        self.request_manager_values = true;
        self
    }

    /// Also requests a timeout vote for the given height and round, if appropriate.
    pub fn with_timeout(mut self, height: BlockHeight, round: Round) -> Self {
        self.request_leader_timeout = Some((height, round));
        self
    }

    /// Also requests a vote to switch to fallback mode, if appropriate.
    #[cfg(with_testing)]
    pub fn with_fallback(mut self) -> Self {
        self.request_fallback = true;
        self
    }
}

/// Information about a chain, returned in response to a [`ChainInfoQuery`].
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
    /// The blob hash of the committee that signs the next block on this chain. `None` if the
    /// chain has not been initialized.
    #[debug(skip_if = Option::is_none)]
    pub committee_hash: Option<CryptoHash>,
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
    /// The response to `request_latest_checkpoint_height`: the height of the most
    /// recent block whose certificate records an [`OracleResponse::Checkpoint`][linera_base::data_types::OracleResponse], or
    /// `None` if no such block exists or the field was not requested.
    #[debug(skip_if = Option::is_none)]
    pub requested_latest_checkpoint_height: Option<BlockHeight>,
    /// The response to `request_pending_commitments`: the queried validator's own
    /// signed commitment manifests awaiting publication on the admin chain.
    #[debug(skip_if = Vec::is_empty)]
    pub requested_pending_commitments: Vec<SignedCommitmentManifest>,
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
    /// The information about the chain.
    pub info: Box<ChainInfo>,
    /// The validator's signature over the chain information, if signed.
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
#[allow(missing_docs)]
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

impl ChainInfo {
    /// Builds a [`ChainInfo`] from the given chain state view.
    pub async fn from_chain_view<C, S>(view: &mut ChainStateView<C>) -> Result<Self, ViewError>
    where
        C: Context<Extra = ChainRuntimeContext<S>> + Clone + 'static,
        ChainRuntimeContext<S>: ExecutionRuntimeContext,
    {
        let system_state = &view.execution_state.system;
        let tip_state = view.tip_state.get();
        Ok(ChainInfo {
            chain_id: view.chain_id(),
            epoch: *system_state.epoch.get(),
            description: system_state.description.get().await?.clone(),
            manager: Box::new(ChainManagerInfo::from(&view.manager)),
            chain_balance: *system_state.balance.get(),
            block_hash: tip_state.block_hash,
            next_block_height: tip_state.next_block_height,
            timestamp: view.execution_state.system.progress.get().timestamp,
            state_hash: Some(view.execution_state.crypto_hash_mut().await?),
            committee_hash: *view.execution_state.system.committee_hash.get(),
            requested_owner_balance: None,
            requested_pending_message_bundles: Vec::new(),
            requested_sent_certificate_hashes: Vec::new(),
            count_received_log: view.received_log.count(),
            requested_received_log: Vec::new(),
            requested_previous_event_blocks: BTreeMap::new(),
            requested_latest_checkpoint_height: None,
            requested_pending_commitments: Vec::new(),
        })
    }
}

impl ChainInfoResponse {
    /// Creates a response from the given [`ChainInfo`], signing it if a key pair is provided.
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

    /// Verifies that the response is correctly signed by the given validator.
    pub fn check(&self, public_key: ValidatorPublicKey) -> Result<(), CryptoError> {
        match self.signature.as_ref() {
            Some(sig) => sig.check(&*self.info, public_key),
            None => Err(CryptoError::MissingValidatorSignature),
        }
    }
}

impl BcsSignable<'_> for ChainInfo {}

/// Request for downloading certificates by heights.
#[derive(Clone)]
pub struct CertificatesByHeightRequest {
    /// The chain whose certificates are requested.
    pub chain_id: ChainId,
    /// The block heights of the requested certificates.
    pub heights: Vec<BlockHeight>,
}

/// Wrapper for displaying a sorted slice of [`BlockHeight`] as compressed ranges.
///
/// Contiguous heights are shown as `start..end` (inclusive), with gaps producing
/// comma-separated entries: `[14810..15309, 15311, 15320..15400]`.
pub(crate) struct CompressedHeights<'a>(pub(crate) &'a [BlockHeight]);

/// Formats a `Vec<BlockHeight>` as compressed ranges for use with `#[debug(with = "...")]`.
#[expect(clippy::ptr_arg)]
pub(crate) fn debug_compressed_heights(
    heights: &Vec<BlockHeight>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    fmt::Debug::fmt(&CompressedHeights(heights), f)
}

impl fmt::Debug for CompressedHeights<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let heights = self.0;
        write!(f, "[")?;
        let mut index = 0;
        while index < heights.len() {
            if index > 0 {
                write!(f, ", ")?;
            }
            let range_start = u64::from(heights[index]);
            let mut range_end = range_start;
            while index + 1 < heights.len() && u64::from(heights[index + 1]) == range_end + 1 {
                index += 1;
                range_end = u64::from(heights[index]);
            }
            if range_start == range_end {
                write!(f, "{range_start}")?;
            } else {
                write!(f, "{range_start}..{range_end}")?;
            }
            index += 1;
        }
        write!(f, "]")
    }
}

impl fmt::Debug for CertificatesByHeightRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificatesByHeightRequest")
            .field("chain_id", &self.chain_id)
            .field("heights", &CompressedHeights(&self.heights))
            .finish()
    }
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

/// The time at which the current round times out, and the round and height it applies to.
#[derive(Debug)]
pub struct RoundTimeout {
    /// The timestamp at which the current round times out.
    pub timestamp: Timestamp,
    /// The round that this timeout applies to.
    pub current_round: Round,
    /// The height of the next block to be added to the chain.
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
    /// Returns the committed value, panicking on a timeout or conflict.
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

    /// Returns the committed value, panicking with `msg` on a timeout or conflict.
    pub fn expect(self, msg: &'static str) -> T {
        match self {
            ClientOutcome::Committed(t) => t,
            ClientOutcome::WaitForTimeout(_) | ClientOutcome::Conflict(_) => panic!("{}", msg),
        }
    }

    /// Applies `f` to the committed value, leaving other outcomes unchanged.
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

    /// Applies the fallible `f` to the committed value, leaving other outcomes unchanged.
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

#[cfg(test)]
mod tests {
    use linera_base::data_types::BlockHeight;

    use super::CompressedHeights;

    #[test]
    fn test_compressed_heights_empty() {
        let heights: Vec<BlockHeight> = vec![];
        assert_eq!(format!("{:?}", CompressedHeights(&heights)), "[]");
    }

    #[test]
    fn test_compressed_heights_single() {
        let heights = vec![BlockHeight::from(5)];
        assert_eq!(format!("{:?}", CompressedHeights(&heights)), "[5]");
    }

    #[test]
    fn test_compressed_heights_contiguous() {
        let heights: Vec<BlockHeight> = (100..=105).map(BlockHeight::from).collect();
        assert_eq!(format!("{:?}", CompressedHeights(&heights)), "[100..105]");
    }

    #[test]
    fn test_compressed_heights_with_gaps() {
        let heights = vec![
            BlockHeight::from(1),
            BlockHeight::from(2),
            BlockHeight::from(3),
            BlockHeight::from(5),
            BlockHeight::from(7),
            BlockHeight::from(8),
            BlockHeight::from(9),
            BlockHeight::from(10),
        ];
        assert_eq!(
            format!("{:?}", CompressedHeights(&heights)),
            "[1..3, 5, 7..10]"
        );
    }

    #[test]
    fn test_compressed_heights_all_isolated() {
        let heights = vec![
            BlockHeight::from(1),
            BlockHeight::from(5),
            BlockHeight::from(10),
        ];
        assert_eq!(format!("{:?}", CompressedHeights(&heights)), "[1, 5, 10]");
    }
}
