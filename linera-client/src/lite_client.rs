// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A storage-free client that proposes blocks directly to validators.
//!
//! Unlike [`ChainClient`](linera_core::client::chain_client::ChainClient) this keeps no
//! local storage and executes nothing: it tracks just enough chain state to keep
//! proposing valid blocks. That makes it cheap enough that a load generator stops being
//! part of what a benchmark measures, at the cost of three round trips per block instead
//! of two.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context as _, Result};
use futures::future::join_all;
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::{Epoch, Round, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_cache::ValueCache;
use linera_chain::{
    data_types::{BlockProposal, IncomingBundle, ProposedBlock, Transaction},
    justification::JustificationChain,
    types::{
        CertificateKind, CertificateValue as _, ConfirmedBlock, ConfirmedBlockCertificate,
        GenericCertificate,
    },
};
use linera_core::{
    client::Client as CoreClient,
    data_types::ChainInfoQuery,
    environment::Environment,
    node::{CrossChainMessageDelivery, ValidatorNode},
    remote_node::RemoteNode,
};
use linera_execution::{committee::Committee, Operation};
use linera_rpc::Client;
use tokio::sync::Mutex;
use tracing::warn;

use crate::benchmark::{BenchmarkClient, BenchmarkError};

/// Tracks just enough state about one chain to keep proposing valid blocks, without any
/// local storage or execution.
pub struct LiteChainClient<Env: Environment> {
    chain_id: ChainId,
    owner: AccountOwner,
    epoch: Epoch,
    height: linera_base::data_types::BlockHeight,
    previous_block_hash: Option<CryptoHash>,
    nodes: Vec<(ValidatorPublicKey, Client)>,
    committee: Committee,
    /// Shared with every other chain this process drives; only its signer is used.
    client: Arc<CoreClient<Env>>,
    value_cache: ValueCache<CryptoHash, ConfirmedBlockCertificate>,
    /// The incoming message bundles to drain into the *next* block, computed as a side effect
    /// of the previous block's confirmed-value fetch (see `propose_and_commit`). Held across
    /// blocks so that draining costs no extra round trip; empty before the first block and in
    /// `independent` mode.
    pending_bundles: Vec<IncomingBundle>,
    /// Whether to broadcast the confirmed certificate in its compact, value-free form where
    /// possible (see `--light-certificates`).
    light_certificates: bool,
}

impl<Env: Environment> LiteChainClient<Env> {
    /// Seeds the client's state for `chain_id` from the first validator that answers.
    pub async fn seed(
        chain_id: ChainId,
        owner: AccountOwner,
        nodes: Vec<(ValidatorPublicKey, Client)>,
        committee: Committee,
        client: Arc<CoreClient<Env>>,
        light_certificates: bool,
    ) -> Result<Self> {
        for (public_key, node) in &nodes {
            let query = ChainInfoQuery::new(chain_id);
            match node.handle_chain_info_query(query).await {
                Ok(response) => {
                    let info = response.info;
                    return Ok(Self {
                        chain_id,
                        owner,
                        epoch: info.epoch,
                        height: info.next_block_height,
                        previous_block_hash: info.block_hash,
                        nodes,
                        committee,
                        client,
                        value_cache: ValueCache::new("lite-benchmark", 64, 60),
                        pending_bundles: Vec::new(),
                        light_certificates,
                    });
                }
                Err(error) => {
                    warn!(%public_key, %error, "validator did not answer the initial chain info query");
                }
            }
        }
        bail!("no validator answered the initial chain info query");
    }

    /// Builds, signs, and submits a block with the given operations, then drives it to a
    /// committed certificate. Uses `Round::Fast`, so this only works on chains owned by a
    /// single super owner.
    ///
    /// If `process_messages` is set, the block first drains up to `bundle_cap` incoming message
    /// bundles from this chain's inboxes (as `Transaction::ReceiveMessages`, before the
    /// operations), so the inboxes don't grow without bound in cross-chain traffic modes.
    /// Returns the number of bundles that were included.
    ///
    /// The bundles come from `self.pending_bundles`, which the *previous* block's confirmed-value
    /// fetch computed for us -- so draining costs no extra round trip. This is sound because a
    /// block only removes its bundles from the validators' inboxes once it commits: the bundles
    /// we carried over are still pending (nothing else drains this chain), and still present in
    /// the validators that reported them, so the proposal is accepted. `self.pending_bundles` is
    /// only refreshed after this block commits, so a failed block simply retries the same set.
    pub async fn propose_and_commit(
        &mut self,
        operations: Vec<linera_execution::Operation>,
        process_messages: bool,
        bundle_cap: usize,
    ) -> Result<usize> {
        let bundles: Vec<IncomingBundle> = if process_messages {
            self.pending_bundles
                .iter()
                .take(bundle_cap)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let num_bundles = bundles.len();
        // The bundles this block consumes, so the next block's pending set can exclude them (the
        // confirmed-value fetch below sees them still in the inboxes, since our certificate has
        // not been broadcast yet).
        let consumed: HashSet<_> = bundles
            .iter()
            .map(|bundle| (bundle.origin, bundle.bundle.cursor()))
            .collect();
        let transactions = bundles
            .into_iter()
            .map(Transaction::ReceiveMessages)
            .chain(operations.into_iter().map(Transaction::ExecuteOperation))
            .collect();
        let block = ProposedBlock {
            chain_id: self.chain_id,
            epoch: self.epoch,
            transactions,
            height: self.height,
            timestamp: Timestamp::now(),
            authenticated_owner: Some(self.owner),
            previous_block_hash: self.previous_block_hash,
        };
        let proposal =
            BlockProposal::new_initial(self.owner, Round::Fast, block, self.client.signer())
                .await
                .map_err(|error| anyhow!("failed to sign the block proposal: {error}"))?;

        // Broadcast the proposal to every validator and collect their `ConfirmedBlock` votes.
        let responses = join_all(self.nodes.iter().map(|(public_key, node)| {
            let proposal = proposal.clone();
            let public_key = *public_key;
            let node = node.clone();
            async move { (public_key, node.handle_block_proposal(proposal).await) }
        }))
        .await;
        let votes = responses
            .into_iter()
            .filter_map(|(public_key, result)| match result {
                Ok(response) => response.info.manager.pending.map(|vote| (public_key, vote)),
                Err(error) => {
                    warn!(%public_key, %error, "validator rejected the block proposal");
                    None
                }
            });
        let (value_hash, signatures) =
            find_confirming_quorum(self.chain_id, votes, &self.committee)
                .context("no quorum of validators voted to confirm the proposed block")?;

        // Fetch the confirmed value (with its real execution outcome) instead of executing the
        // block ourselves, and -- folded into the same round trip -- the inboxes' pending
        // bundles, from which we compute the set to drain into the *next* block.
        let (confirmed_block, next_pending) = self
            .fetch_confirmed_and_pending(value_hash, process_messages, &consumed)
            .await?;

        // The vote's `first_round` attestation must be reproduced exactly, since it is part of
        // what every signature covers (see `Vote::new_with_first_round`); a single super owner's
        // `Round::Fast` is always the chain's designated first round, so this is always `true`.
        let quorum = GenericCertificate::new_with_payload(
            confirmed_block,
            Round::Fast,
            None,
            true,
            None,
            signatures,
        );
        let certificate =
            ConfirmedBlockCertificate::from_parts(quorum, JustificationChain::default());
        // Hoisted so the certificate can be moved into the cache: cloning it here deep-copies
        // the whole confirmed block on every single block.
        let certificate_hash = certificate.hash();
        let cached_certificate = self.value_cache.insert(&certificate_hash, certificate);

        // Broadcast the certificate so every validator commits the block. Only advance our own
        // state once at least one validator actually accepted it, so we don't get out of sync
        // with the chain if the certificate is rejected everywhere.
        //
        // With --light-certificates, prefer sending each validator just the certificate's hash
        // and signatures (no block value) via RemoteNode::handle_optimized_confirmed_certificate
        // -- every validator here voted on this block in the first round trip, so it already has
        // the value cached and can reconstruct the full certificate locally. A validator that
        // fell behind and forgot the value it signed gets a transparent fallback to the full
        // certificate (see that method's doc comment). This only shrinks this round trip's
        // payload; it doesn't remove it.
        let light_certificates = self.light_certificates;
        let results = join_all(self.nodes.iter().map(|(public_key, node)| {
            let node = node.clone();
            let cached_certificate = cached_certificate.clone();
            async move {
                if light_certificates {
                    let remote_node = RemoteNode {
                        public_key: *public_key,
                        node,
                    };
                    remote_node
                        .handle_optimized_confirmed_certificate(
                            &cached_certificate,
                            CrossChainMessageDelivery::NonBlocking,
                        )
                        .await
                        .map(|_| ())
                } else {
                    node.handle_confirmed_certificate(
                        cached_certificate,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await
                    .map(|_| ())
                }
            }
        }))
        .await;
        let mut committed = false;
        for result in results {
            if let Err(error) = result {
                warn!(%error, "validator failed to process the confirmed certificate");
            } else {
                committed = true;
            }
        }
        anyhow::ensure!(committed, "no validator accepted the confirmed certificate");

        self.previous_block_hash = Some(certificate_hash);
        self.height = self.height.try_add_one()?;
        // Only now that the block committed (so its bundles are being removed from the inboxes)
        // do we adopt the next pending set. On a failed block we keep `self.pending_bundles` as
        // it was, so the next attempt retries the same, still-pending bundles.
        self.pending_bundles = next_pending;
        Ok(num_bundles)
    }

    /// In one parallel round trip to every validator, fetches the confirmed block value for
    /// `value_hash` (from any validator that has it) and, if `process_messages` is set, the
    /// bundles to drain into the *next* block.
    ///
    /// The next pending set is the per-origin prefix that *every* responding validator agrees
    /// on, minus `consumed` (the bundles this block is about to remove, which are still in the
    /// inboxes at query time since our certificate has not been broadcast yet). We take only the
    /// agreed prefix because certificates are delivered non-blocking, so the validators' inboxes
    /// are not in lockstep: a bundle one validator already holds may not have reached another. A
    /// proposal is rejected wholesale if it receives a bundle a validator lacks
    /// (`MissingCrossChainUpdate`), and a given origin's bundles must be consumed in cursor order
    /// (`IncorrectOrder`), so anything not yet everywhere is simply left for a later block. No
    /// validator response is trusted for anything but which bundles exist; they are copied
    /// verbatim into the block. The result is not capped here -- the cap is applied when the
    /// bundles are actually included, so a backlog beyond one block's cap carries forward.
    async fn fetch_confirmed_and_pending(
        &self,
        value_hash: CryptoHash,
        process_messages: bool,
        consumed: &HashSet<(ChainId, linera_base::data_types::Cursor)>,
    ) -> Result<(ConfirmedBlock, Vec<IncomingBundle>)> {
        let responses = join_all(self.nodes.iter().map(|(public_key, node)| {
            let node = node.clone();
            let mut query = ChainInfoQuery::new(self.chain_id);
            // Only `manager.requested_confirmed` is read below, but the flag is all-or-
            // nothing on the wire (`add_values` also attaches the proposed and locking
            // blocks), so each validator returns roughly two extra block-sized payloads per
            // block. Narrowing it needs a new query field, not a change here.
            query.request_manager_values = true;
            if process_messages {
                query = query.with_pending_message_bundles();
            }
            let public_key = *public_key;
            async move {
                match node.handle_chain_info_query(query).await {
                    Ok(response) => Some(response.info),
                    Err(error) => {
                        warn!(%public_key, %error, "validator did not answer the confirmed-value query");
                        None
                    }
                }
            }
        }))
        .await;

        let mut confirmed_block: Option<ConfirmedBlock> = None;
        let mut per_node: Vec<Vec<IncomingBundle>> = Vec::new();
        for info in responses.into_iter().flatten() {
            if process_messages {
                per_node.push(info.requested_pending_message_bundles);
            }
            if confirmed_block.is_none() {
                if let Some(value) = info.manager.requested_confirmed {
                    if value.hash() == value_hash {
                        confirmed_block = Some(*value);
                    }
                }
            }
        }
        let confirmed_block =
            confirmed_block.context("could not fetch the confirmed block value")?;

        let next_pending = if process_messages {
            common_prefix_bundles(per_node)
                .into_iter()
                .filter(|bundle| !consumed.contains(&(bundle.origin, bundle.bundle.cursor())))
                .collect()
        } else {
            Vec::new()
        };
        Ok((confirmed_block, next_pending))
    }
}

/// Given each responding validator's list of pending incoming bundles, returns the bundles that
/// appear -- as an in-order per-origin prefix -- in *every* list. Bundles from one origin are
/// FIFO by cursor, so for each origin this compares the lists element by element and keeps the
/// longest common leading run; an origin missing from any list contributes nothing. Origins are
/// visited in a deterministic (sorted) order. See `fetch_confirmed_and_pending` for why only this
/// safe intersection is used.
fn common_prefix_bundles(per_node: Vec<Vec<IncomingBundle>>) -> Vec<IncomingBundle> {
    let Some((first, rest)) = per_node.split_first() else {
        return Vec::new();
    };
    // Group each node's bundles by origin, preserving each origin's cursor order.
    let group = |bundles: &[IncomingBundle]| -> BTreeMap<ChainId, Vec<IncomingBundle>> {
        let mut by_origin: BTreeMap<ChainId, Vec<IncomingBundle>> = BTreeMap::new();
        for bundle in bundles {
            by_origin
                .entry(bundle.origin)
                .or_default()
                .push(bundle.clone());
        }
        by_origin
    };
    let base = group(first);
    let others: Vec<_> = rest.iter().map(|node| group(node)).collect();
    let mut result = Vec::new();
    for (origin, base_bundles) in base {
        let mut prefix_len = base_bundles.len();
        for other in &others {
            let other_bundles = other.get(&origin).map_or(&[][..], Vec::as_slice);
            let matching = base_bundles
                .iter()
                .zip(other_bundles)
                .take_while(|(a, b)| a.bundle.cursor() == b.bundle.cursor())
                .count();
            prefix_len = prefix_len.min(matching);
            if prefix_len == 0 {
                break;
            }
        }
        result.extend(base_bundles.into_iter().take(prefix_len));
    }
    result
}

/// Groups the given validator votes by the `ConfirmedBlock` value hash they attest to, and
/// returns the first hash (and its signatures) whose combined committee weight reaches the
/// quorum threshold. Votes for the wrong chain or of the wrong kind are ignored. No signature
/// is verified here: the caller trusts every vote at face value.
fn find_confirming_quorum(
    chain_id: ChainId,
    votes: impl IntoIterator<Item = (ValidatorPublicKey, linera_chain::data_types::LiteVote)>,
    committee: &Committee,
) -> Option<(CryptoHash, Vec<(ValidatorPublicKey, ValidatorSignature)>)> {
    let mut signatures_by_hash: HashMap<CryptoHash, Vec<(ValidatorPublicKey, ValidatorSignature)>> =
        HashMap::new();
    let mut weight_by_hash: HashMap<CryptoHash, u64> = HashMap::new();
    for (public_key, vote) in votes {
        if vote.value.chain_id != chain_id || vote.value.kind != CertificateKind::Confirmed {
            continue;
        }
        let hash = vote.value.value_hash;
        signatures_by_hash
            .entry(hash)
            .or_default()
            .push((public_key, vote.signature));
        let weight = weight_by_hash.entry(hash).or_insert(0);
        *weight += committee.weight(&public_key);
        if *weight >= committee.quorum_threshold() {
            let signatures = signatures_by_hash
                .remove(&hash)
                .expect("just inserted above");
            return Some((hash, signatures));
        }
    }
    None
}

/// Adapts [`LiteChainClient`] to the shared benchmark harness.
///
/// The harness holds each client behind a shared reference and drives one chain per task, so
/// the mutable proposal state (height, previous hash, pending bundles) sits behind a mutex
/// that is only ever contended if a caller drives the same chain from two places -- which
/// would be a bug regardless, since block heights are sequential.
pub struct LiteBenchmarkClient<Env: Environment> {
    chain_id: ChainId,
    owner: AccountOwner,
    inner: Mutex<LiteChainClient<Env>>,
    process_messages: bool,
    bundle_cap: Option<usize>,
}

impl<Env: Environment> LiteBenchmarkClient<Env> {
    /// Wraps a seeded client. `bundle_cap` defaults to twice the block's operation count, so a
    /// backlog is drained over several blocks rather than one oversized one.
    pub fn new(
        client: LiteChainClient<Env>,
        process_messages: bool,
        bundle_cap: Option<usize>,
    ) -> Self {
        Self {
            chain_id: client.chain_id,
            owner: client.owner,
            inner: Mutex::new(client),
            process_messages,
            bundle_cap,
        }
    }
}

#[async_trait::async_trait]
impl<Env: Environment> BenchmarkClient for LiteBenchmarkClient<Env> {
    fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    async fn owner(&self) -> Result<AccountOwner, BenchmarkError> {
        Ok(self.owner)
    }

    async fn commit_operations(&self, operations: Vec<Operation>) -> Result<(), BenchmarkError> {
        let bundle_cap = self
            .bundle_cap
            .unwrap_or_else(|| operations.len().saturating_mul(2));
        self.inner
            .lock()
            .await
            .propose_and_commit(operations, self.process_messages, bundle_cap)
            .await
            .map_err(|error| BenchmarkError::LiteClient(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{AccountSecretKey, CryptoHash, ValidatorKeypair},
        data_types::BlockHeight,
    };
    use linera_chain::data_types::{LiteValue, LiteVote, MessageAction, MessageBundle};

    use super::*;

    /// A pending bundle from `origin` whose cursor is `(height, index)`. The message list is
    /// empty: `common_prefix_bundles` compares only cursors, so the contents are irrelevant.
    fn bundle(origin: ChainId, height: u64, index: u32) -> IncomingBundle {
        IncomingBundle {
            origin,
            bundle: MessageBundle {
                height: BlockHeight(height),
                timestamp: Timestamp::from(0),
                certificate_hash: CryptoHash::test_hash("cert"),
                transaction_index: index,
                messages: Vec::new(),
            },
            action: MessageAction::Accept,
        }
    }

    /// The bundles' cursors, sorted by (origin, height, index). Sorting makes comparisons
    /// insensitive to the order origins are emitted in (which is irrelevant, since each origin's
    /// inbox is drained independently) while still exposing any per-origin reordering, because
    /// within an origin the expected cursors are already ascending.
    fn cursors(bundles: &[IncomingBundle]) -> Vec<(ChainId, u64, u32)> {
        let mut cursors: Vec<_> = bundles
            .iter()
            .map(|b| (b.origin, b.bundle.height.0, b.bundle.transaction_index))
            .collect();
        cursors.sort();
        cursors
    }

    fn sorted(mut cursors: Vec<(ChainId, u64, u32)>) -> Vec<(ChainId, u64, u32)> {
        cursors.sort();
        cursors
    }

    #[test]
    fn common_prefix_takes_the_agreed_per_origin_prefix() {
        let a = ChainId(CryptoHash::test_hash("a"));
        let b = ChainId(CryptoHash::test_hash("b"));

        // No responders at all -> nothing to drain.
        assert!(common_prefix_bundles(Vec::new()).is_empty());

        // A single responder: everything it lists is included (grouped by origin, in order).
        let only = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(b, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![only.clone()])),
            sorted(vec![(a, 0, 0), (a, 1, 0), (b, 0, 0)]),
        );

        // Two responders agreeing fully: the whole thing survives.
        assert_eq!(
            common_prefix_bundles(vec![only.clone(), only.clone()]).len(),
            3
        );

        // One responder is one bundle behind on origin `a`: only the shared prefix of `a`
        // survives, and origin `b`, present in both, is kept.
        let ahead = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(b, 0, 0)];
        let behind = vec![bundle(a, 0, 0), bundle(b, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![ahead, behind])),
            sorted(vec![(a, 0, 0), (b, 0, 0)]),
        );

        // The lists diverge mid-origin (a different cursor at index 1): the prefix stops at the
        // divergence, and nothing past it is included even though later cursors happen to match.
        let left = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(a, 2, 0)];
        let right = vec![bundle(a, 0, 0), bundle(a, 5, 0), bundle(a, 2, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![left, right])),
            vec![(a, 0, 0)],
        );

        // An origin missing from one responder contributes nothing, but other shared origins
        // are unaffected.
        let with_b = vec![bundle(a, 0, 0), bundle(b, 0, 0)];
        let without_b = vec![bundle(a, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![with_b, without_b])),
            vec![(a, 0, 0)],
        );
    }

    fn committee_of(size: usize) -> (Committee, Vec<ValidatorPublicKey>) {
        let keys: Vec<_> = (0..size)
            .map(|_| {
                (
                    ValidatorKeypair::generate().public_key,
                    AccountSecretKey::generate().public(),
                )
            })
            .collect();
        let public_keys = keys.iter().map(|(key, _)| *key).collect();
        (Committee::make_simple(keys), public_keys)
    }

    fn vote(chain_id: ChainId, value_hash: CryptoHash) -> LiteVote {
        LiteVote {
            value: LiteValue {
                value_hash,
                chain_id,
                kind: CertificateKind::Confirmed,
            },
            round: Round::Fast,
            unlocking_round: None,
            first_round: true,
            justification_commitment: None,
            signature: ValidatorSignature::sign_prehash(
                &ValidatorKeypair::generate().secret_key,
                value_hash,
            ),
        }
    }

    #[test]
    fn quorum_is_reached_once_enough_weight_agrees() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let value_hash = CryptoHash::test_hash("confirmed-block");
        let (committee, keys) = committee_of(4);

        // Only 2 out of 4 equally-weighted validators agree: not a quorum yet.
        let votes = keys[..2]
            .iter()
            .map(|key| (*key, vote(chain_id, value_hash)));
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());

        // 3 out of 4 is enough.
        let votes = keys[..3]
            .iter()
            .map(|key| (*key, vote(chain_id, value_hash)));
        let (hash, signatures) = find_confirming_quorum(chain_id, votes, &committee)
            .expect("3 out of 4 equally-weighted validators should reach the quorum threshold");
        assert_eq!(hash, value_hash);
        assert_eq!(signatures.len(), 3);
    }

    #[test]
    fn votes_for_a_different_chain_are_ignored() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let other_chain_id = ChainId(CryptoHash::test_hash("other-chain"));
        let value_hash = CryptoHash::test_hash("confirmed-block");
        let (committee, keys) = committee_of(4);

        let votes = keys
            .iter()
            .map(|key| (*key, vote(other_chain_id, value_hash)));
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());
    }

    #[test]
    fn a_split_vote_never_reaches_quorum_on_either_side() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let hash_a = CryptoHash::test_hash("block-a");
        let hash_b = CryptoHash::test_hash("block-b");
        let (committee, keys) = committee_of(4);

        let votes = vec![
            (keys[0], vote(chain_id, hash_a)),
            (keys[1], vote(chain_id, hash_a)),
            (keys[2], vote(chain_id, hash_b)),
            (keys[3], vote(chain_id, hash_b)),
        ];
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());
    }
}
