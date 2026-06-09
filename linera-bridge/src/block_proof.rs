// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A light proof that a Linera block was confirmed.
//!
//! Instead of relaying an entire [`Block`](linera_chain::block::Block) to the EVM light
//! client, the relayer sends only the [`BlockHeader`] — which the validators sign and which
//! commits to every body field through its per-field hashes — together with the two body
//! fields the bridge needs: the transactions (for committee transitions) and the events (for
//! burns). Each is checked against the matching hash in the header; the rest of the body
//! stays off the wire.

use linera_base::{
    crypto::{CryptoHash, CryptoHashVec, ValidatorPublicKey, ValidatorSignature},
    data_types::{Event, Round},
};
use linera_chain::{
    block::{BlockBodyField, BlockHeader},
    data_types::Transaction,
    types::ConfirmedBlockCertificate,
};
use serde::{Deserialize, Serialize};

/// A confirmed block reduced to what the EVM bridge needs in order to verify it: the header,
/// the transactions (for committee transitions), the events (for burns), the round, and the
/// validator signatures. The rest of the body is dropped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockProof {
    /// The block header. Its hash is the value the validators signed, and it commits to the
    /// whole body via its per-field hashes.
    pub header: BlockHeader,
    /// The block's transactions. Checked against `header.transactions_hash`.
    pub transactions: Vec<Transaction>,
    /// The block's events, one inner vector per transaction. Checked against
    /// `header.events_hash`.
    pub events: Vec<Vec<Event>>,
    /// The round in which the block was confirmed.
    pub round: Round,
    /// The validator signatures over the block hash.
    pub signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

impl BlockProof {
    /// Builds a proof from a confirmed-block certificate, dropping every body field except the
    /// transactions and the events.
    pub fn from_certificate(certificate: &ConfirmedBlockCertificate) -> Self {
        let block = certificate.block();
        BlockProof {
            header: block.header.clone(),
            transactions: block.body.transactions.clone(),
            events: block.body.events.clone(),
            round: certificate.round,
            signatures: certificate.signatures().clone(),
        }
    }

    /// Builds a header-only proof: the header, round, and signatures with empty body fields.
    /// Enough to verify the block's signatures and register its `events_hash`, without shipping
    /// any body. The header's per-field hashes are authenticated by the signatures, so the
    /// dropped fields can be proven against it later via inclusion proofs.
    pub fn header_only(certificate: &ConfirmedBlockCertificate) -> Self {
        let block = certificate.block();
        BlockProof {
            header: block.header.clone(),
            transactions: Vec::new(),
            events: Vec::new(),
            round: certificate.round,
            signatures: certificate.signatures().clone(),
        }
    }

    /// Returns the block hash this proof commits to, computed from the header alone. This is
    /// the value the certificate's signatures are over.
    pub fn block_hash(&self) -> CryptoHash {
        CryptoHash::new(&self.header)
    }

    /// Returns whether the carried transactions are the ones the header commits to.
    pub fn transactions_match_header(&self) -> bool {
        self.header
            .verifies(&BlockBodyField::Transactions(self.transactions.clone()))
    }

    /// Returns whether the carried events are the ones the header commits to.
    pub fn events_match_header(&self) -> bool {
        self.header
            .verifies(&BlockBodyField::Events(self.events.clone()))
    }
}

/// Linera's `hash_vec` over one transaction's events: `CryptoHash::new(&CryptoHashVec([leaf_i]))`
/// where `leaf_i = CryptoHash::new(&event_i)`. This is the inner level of the block's
/// `events_hash` (`hash_vec_vec`).
fn hash_events_of_transaction(events: &[Event]) -> CryptoHash {
    CryptoHash::new(&CryptoHashVec(events.iter().map(CryptoHash::new).collect()))
}

/// An inclusion proof that the events at `positions` within transaction `tx_index` of a block are
/// part of the block's `events_hash`. The verifier recomputes each proven event's leaf hash and
/// folds it with the supplied sibling hashes through Linera's two-level `hash_vec_vec`, then checks
/// the result against the (signed) `events_hash` in the header. This lets the bridge release a
/// subset of a block's burns without re-hashing every event in the block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventInclusionProof {
    /// Index of the transaction whose events are being proven.
    pub tx_index: u32,
    /// Number of transactions in the block (length of the outer `events` vector).
    pub num_txs: u32,
    /// Number of events in transaction `tx_index` (length of its inner vector).
    pub num_events_in_tx: u32,
    /// Positions (ascending) of the proven events within transaction `tx_index`.
    pub positions: Vec<u32>,
    /// Leaf hashes of the events in transaction `tx_index` that are NOT proven, in position order.
    /// With the proven events' recomputed leaves they reconstruct the transaction's event hash.
    pub inner_siblings: Vec<CryptoHash>,
    /// Per-transaction event hashes for every transaction other than `tx_index`, in transaction
    /// order. With the proven transaction's recomputed hash they reconstruct `events_hash`.
    pub outer_siblings: Vec<CryptoHash>,
}

impl EventInclusionProof {
    /// Builds the proof for the events at `positions` (ascending) within transaction `tx_index` of
    /// `events` (the block body's `Vec<Vec<Event>>`).
    pub fn new(events: &[Vec<Event>], tx_index: usize, positions: &[u32]) -> Self {
        let inner = &events[tx_index];
        let num_events_in_tx = u32::try_from(inner.len()).expect("event count exceeds u32");
        let num_txs = u32::try_from(events.len()).expect("transaction count exceeds u32");
        let proven: std::collections::BTreeSet<u32> = positions.iter().copied().collect();
        let inner_siblings = (0..num_events_in_tx)
            .filter(|p| !proven.contains(p))
            .map(|p| CryptoHash::new(&inner[p as usize]))
            .collect();
        let outer_siblings = events
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != tx_index)
            .map(|(_, tx_events)| hash_events_of_transaction(tx_events))
            .collect();
        EventInclusionProof {
            tx_index: u32::try_from(tx_index).expect("tx index exceeds u32"),
            num_txs,
            num_events_in_tx,
            positions: positions.to_vec(),
            inner_siblings,
            outer_siblings,
        }
    }

    /// Recomputes the `events_hash` this proof folds to, given the leaf hashes of the proven events
    /// in `positions` order. A block's header commits to this value, so comparing the result to
    /// `header.events_hash` proves the events belong to the block.
    pub fn fold(&self, proven_leaves: &[CryptoHash]) -> CryptoHash {
        // Inner level: rebuild transaction `tx_index`'s event leaves, taking proven positions from
        // `proven_leaves` and the rest from `inner_siblings`.
        let mut inner = Vec::with_capacity(self.num_events_in_tx as usize);
        let mut proven = self
            .positions
            .iter()
            .copied()
            .zip(proven_leaves.iter().copied());
        let mut siblings = self.inner_siblings.iter().copied();
        let mut next_proven = proven.next();
        for p in 0..self.num_events_in_tx {
            match next_proven {
                Some((pos, leaf)) if pos == p => {
                    inner.push(leaf);
                    next_proven = proven.next();
                }
                _ => inner.push(siblings.next().expect("missing inner sibling")),
            }
        }
        let tx_hash = CryptoHash::new(&CryptoHashVec(inner));

        // Outer level: rebuild the per-transaction hashes, taking `tx_index` from the recomputed
        // hash and the rest from `outer_siblings`.
        let mut outer = Vec::with_capacity(self.num_txs as usize);
        let mut siblings = self.outer_siblings.iter().copied();
        for j in 0..self.num_txs {
            if j == self.tx_index {
                outer.push(tx_hash);
            } else {
                outer.push(siblings.next().expect("missing outer sibling"));
            }
        }
        CryptoHash::new(&CryptoHashVec(outer))
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{AccountSecretKey, CryptoHash, Ed25519SecretKey, ValidatorKeypair},
        data_types::{BlockHeight, Event, Round},
        identifiers::{ChainId, GenericApplicationId, StreamId, StreamName},
    };
    use linera_chain::{
        block::ConfirmedBlock,
        data_types::{LiteValue, LiteVote, SignatureAggregator},
        test::BlockBuilder,
    };
    use linera_execution::committee::Committee;

    use super::{BlockProof, EventInclusionProof};

    /// The lighter scheme end to end: a confirmed block can be verified, and one of its
    /// events proven, from only the header, the events, and the signatures — no body.
    #[test]
    fn block_proof_verifies_block_and_events_from_header() {
        let validator = ValidatorKeypair::generate();
        let account = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
        let committee = Committee::make_simple(vec![(validator.public_key, account.public())]);

        let event = Event {
            stream_id: StreamId {
                application_id: GenericApplicationId::System,
                stream_name: StreamName(b"burns".to_vec()),
            },
            index: 0,
            value: b"burn".to_vec(),
        };
        let block = BlockBuilder::new(ChainId(CryptoHash::test_hash("chain")), BlockHeight(1))
            .with_events(vec![event.clone()])
            .build();
        let confirmed = ConfirmedBlock::new(block);

        let vote = LiteVote::new(
            LiteValue::new(&confirmed),
            Round::Fast,
            &validator.secret_key,
        );
        let mut aggregator = SignatureAggregator::new(confirmed, Round::Fast, &committee);
        let certificate = aggregator
            .append(validator.public_key, vote.signature)
            .unwrap()
            .unwrap();

        let proof = BlockProof::from_certificate(&certificate);

        // The header alone reproduces the value the validators signed.
        assert_eq!(proof.block_hash(), certificate.hash());
        // The signatures still verify against that hash — the body was never needed.
        assert!(certificate.check(&committee).is_ok());
        // The carried body fields are exactly the ones the header commits to.
        assert!(proof.transactions_match_header());
        assert!(proof.events_match_header());
        assert_eq!(proof.events, vec![vec![event]]);
    }

    /// An `EventInclusionProof` for any subset of a block's events folds back to the exact
    /// `events_hash` the header commits to, and a tampered leaf does not.
    #[test]
    fn event_inclusion_proof_folds_to_events_hash() {
        let make_event = |value: &[u8], index| Event {
            stream_id: StreamId {
                application_id: GenericApplicationId::System,
                stream_name: StreamName(b"burns".to_vec()),
            },
            index,
            value: value.to_vec(),
        };
        // Three transactions carrying 1, 2, and 1 events respectively.
        let block = BlockBuilder::new(ChainId(CryptoHash::test_hash("chain")), BlockHeight(1))
            .with_events(vec![make_event(b"a", 0)])
            .with_events(vec![make_event(b"b", 1), make_event(b"c", 2)])
            .with_events(vec![make_event(b"d", 3)])
            .build();
        let events = &block.body.events;
        let events_hash = block.header.events_hash;

        // Proving any single event reproduces the header's events_hash.
        for (tx_index, tx_events) in events.iter().enumerate() {
            for (pos, event) in tx_events.iter().enumerate() {
                let proof =
                    EventInclusionProof::new(events, tx_index, &[u32::try_from(pos).unwrap()]);
                let leaf = CryptoHash::new(event);
                assert_eq!(proof.fold(&[leaf]), events_hash);
            }
        }

        // Proving multiple events in one transaction folds correctly too.
        let proof = EventInclusionProof::new(events, 1, &[0, 1]);
        let leaves = [
            CryptoHash::new(&events[1][0]),
            CryptoHash::new(&events[1][1]),
        ];
        assert_eq!(proof.fold(&leaves), events_hash);

        // A tampered leaf must not fold to the header's events_hash.
        let proof = EventInclusionProof::new(events, 0, &[0]);
        let wrong = CryptoHash::new(&make_event(b"x", 9));
        assert_ne!(proof.fold(&[wrong]), events_hash);
    }
}
