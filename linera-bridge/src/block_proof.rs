// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A light proof that a Linera block was confirmed.
//!
//! Instead of relaying an entire [`Block`](linera_chain::block::Block) to the EVM light
//! client, the relayer sends only the [`BlockHeader`] — which the validators sign and which
//! commits to every body field through its per-field hashes — together with the block's
//! events. The events are the one body field the bridge needs, to release burns; they are
//! checked against the header's `events_hash`. Everything else in the body stays off the
//! wire.

use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey, ValidatorSignature},
    data_types::{Event, Round},
};
use linera_chain::{
    block::{BlockBodyField, BlockHeader},
    types::ConfirmedBlockCertificate,
};
use serde::{Deserialize, Serialize};

/// A confirmed block reduced to what the EVM bridge needs in order to verify it: the header,
/// the events, the round, and the validator signatures.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockProof {
    /// The block header. Its hash is the value the validators signed, and it commits to the
    /// whole body via its per-field hashes.
    pub header: BlockHeader,
    /// The block's events, one inner vector per transaction. Checked against
    /// `header.events_hash`.
    pub events: Vec<Vec<Event>>,
    /// The round in which the block was confirmed.
    pub round: Round,
    /// The validator signatures over the block hash.
    pub signatures: Vec<(ValidatorPublicKey, ValidatorSignature)>,
}

impl BlockProof {
    /// Builds a proof from a confirmed-block certificate, dropping every body field except
    /// the events.
    pub fn from_certificate(certificate: &ConfirmedBlockCertificate) -> Self {
        let block = certificate.block();
        BlockProof {
            header: block.header.clone(),
            events: block.body.events.clone(),
            round: certificate.round,
            signatures: certificate.signatures().clone(),
        }
    }

    /// Returns the block hash this proof commits to, computed from the header alone. This is
    /// the value the certificate's signatures are over.
    pub fn block_hash(&self) -> CryptoHash {
        CryptoHash::new(&self.header)
    }

    /// Returns whether the carried events are the ones the header commits to.
    pub fn events_match_header(&self) -> bool {
        self.header
            .verifies(&BlockBodyField::Events(self.events.clone()))
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

    use super::BlockProof;

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
        // The carried events are exactly the ones the header commits to.
        assert!(proof.events_match_header());
        assert_eq!(proof.events, vec![vec![event]]);
    }
}
