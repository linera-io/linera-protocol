// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;

use linera_base::{
    crypto::{
        AccountSecretKey, CryptoHash, ValidatorKeypair, ValidatorPublicKey, ValidatorSignature,
    },
    data_types::{BlockHeight, Epoch, Round, Timestamp},
    identifiers::ChainId,
};
use linera_execution::committee::Committee;

use super::{
    audit_confirmation, extract_double_validations, extract_equivocations, CommittedQuorum,
    EquivocationProof, JustificationChain, JustificationLink, JustifiedConfirmation,
    ValidatedQuorum,
};
use crate::{block::BlockHeader, data_types::VoteValue, types::CertificateKind, ChainError};

/// Generates `n` validator keypairs and an equally-weighted committee over them.
/// With 100 votes each, the quorum threshold is `ceil(2n/3 * 100) + ...`; for `n = 4` that
/// means any 3 of the 4 validators form a quorum.
fn setup(n: usize) -> (Committee, Vec<ValidatorKeypair>) {
    let keys: Vec<ValidatorKeypair> = (0..n).map(|_| ValidatorKeypair::generate()).collect();
    let committee = Committee::make_simple(
        keys.iter()
            .map(|kp| (kp.public_key, AccountSecretKey::generate().public()))
            .collect(),
    );
    (committee, keys)
}

/// Builds a distinct block header on the given chain, at the given height, distinguished by
/// `name` (which varies its content hashes). The header hashes to the value the votes sign.
fn header_at(chain: u32, height: u64, name: &str) -> BlockHeader {
    let h = CryptoHash::test_hash(name);
    BlockHeader {
        chain_id: ChainId(CryptoHash::test_hash(format!("chain{chain}"))),
        epoch: Epoch::ZERO,
        height: BlockHeight(height),
        timestamp: Timestamp::from(0),
        state_hash: h,
        previous_block_hash: None,
        authenticated_owner: None,
        transactions_hash: h,
        messages_hash: h,
        previous_message_blocks_hash: h,
        previous_event_blocks_hash: h,
        oracle_responses_hash: h,
        events_hash: h,
        blobs_hash: h,
        operation_results_hash: h,
    }
}

/// A block header on the default chain (1) at height 0, distinguished by `name`.
fn header(name: &str) -> BlockHeader {
    header_at(1, 0, name)
}

/// The hash of [`header(name)`] — the value that block's votes sign.
fn block(name: &str) -> CryptoHash {
    CryptoHash::new(&header(name))
}

/// The hash of [`header_at(chain, height, name)`].
fn block_at(chain: u32, height: u64, name: &str) -> CryptoHash {
    CryptoHash::new(&header_at(chain, height, name))
}

fn sign(
    block: CryptoHash,
    round: Round,
    kind: CertificateKind,
    unlocking_round: Option<Round>,
    first_round: bool,
    justification_commitment: Option<CryptoHash>,
    key: &ValidatorKeypair,
) -> (ValidatorPublicKey, ValidatorSignature) {
    let value = VoteValue(
        block,
        round,
        kind,
        unlocking_round,
        first_round,
        justification_commitment,
    );
    (
        key.public_key,
        ValidatorSignature::new(&value, &key.secret_key),
    )
}

/// Builds a justification chain for `block` with one link per `(round, signers)` entry, ordered
/// by increasing round. Each link's voters sign the unlocking round and justification commitment
/// derived from the chain below them, exactly as real voters do.
fn chain(block: CryptoHash, links: &[(Round, &[&ValidatorKeypair])]) -> JustificationChain {
    let mut result = JustificationChain::default();
    for (round, signers) in links {
        let unlocking_round = result.top_unlocking_round();
        let justification_commitment = result.commitment(block);
        let signatures = signers
            .iter()
            .map(|kp| {
                sign(
                    block,
                    *round,
                    CertificateKind::Validated,
                    unlocking_round,
                    false,
                    justification_commitment,
                    kp,
                )
            })
            .collect();
        result = result.append(*round, signatures);
    }
    result
}

/// A quorum of `ConfirmedBlock` signatures for `block` in `round`, carrying the first-round
/// attestation iff `first_round` is set and committing to the given justification chain.
fn confirmed_signatures(
    block: CryptoHash,
    round: Round,
    first_round: bool,
    justification: &JustificationChain,
    signers: &[&ValidatorKeypair],
) -> Vec<(ValidatorPublicKey, ValidatorSignature)> {
    let justification_commitment = justification.commitment(block);
    signers
        .iter()
        .map(|kp| {
            sign(
                block,
                round,
                CertificateKind::Confirmed,
                None,
                first_round,
                justification_commitment,
                kp,
            )
        })
        .collect()
}

/// Asserts that the proofs all verify and name exactly the expected validators, one proof each.
#[track_caller]
fn assert_proves(
    committee: &Committee,
    proofs: &[EquivocationProof],
    expected: &[&ValidatorKeypair],
) {
    for proof in proofs {
        proof.check(committee).expect("proof must verify");
    }
    let validators = proofs
        .iter()
        .map(EquivocationProof::validator)
        .collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .map(|kp| kp.public_key)
        .collect::<BTreeSet<_>>();
    assert_eq!(validators, expected);
    assert_eq!(proofs.len(), expected.len());
}

/// Builds a [`JustifiedConfirmation`] for the named block on the default chain, confirmed in
/// `round` by `signers`, carrying the given first-round attestation and justification chain.
fn confirmation(
    name: &str,
    round: Round,
    first_round: bool,
    signers: &[&ValidatorKeypair],
    justification: JustificationChain,
) -> JustifiedConfirmation {
    JustifiedConfirmation {
        header: header(name),
        round,
        first_round,
        confirmed_signatures: confirmed_signatures(
            block(name),
            round,
            first_round,
            &justification,
            signers,
        ),
        justification,
    }
}

// --- Chain verification -----------------------------------------------------------------

#[test]
fn verify_accepts_valid_chain_and_returns_commitment() {
    let (_committee, k) = setup(4);
    let b = block("B");
    // Link 0 at round 1 is the grounding link; link 1 at round 3 is justified by it.
    let chain = chain(
        b,
        &[
            (Round::SingleLeader(1), &[&k[0], &k[1], &k[2]]),
            (Round::SingleLeader(3), &[&k[0], &k[1], &k[2]]),
        ],
    );
    let commitment = chain.verify(b).expect("chain must verify");
    assert_eq!(commitment, chain.commitment(b));
    assert!(commitment.is_some());
    assert_eq!(chain.top_unlocking_round(), Some(Round::SingleLeader(3)));
    // The empty chain has no commitment.
    assert_eq!(JustificationChain::default().commitment(b), None);
}

#[test]
fn verify_rejects_non_increasing_rounds() {
    let b = block("B");
    // Round monotonicity is the one structural property checked locally; everything else about
    // the links is attested by the quorum that signed the chain's commitment.
    let chain = JustificationChain::new(vec![
        JustificationLink {
            round: Round::SingleLeader(1),
            signatures: Vec::new(),
        },
        JustificationLink {
            round: Round::SingleLeader(1),
            signatures: Vec::new(),
        },
    ]);
    assert!(matches!(
        chain.verify(b),
        Err(ChainError::JustificationRoundsNotIncreasing)
    ));
}

#[test]
fn commitment_binds_every_link() {
    let (_committee, k) = setup(4);
    let b = block("B");
    let original = chain(
        b,
        &[
            (Round::SingleLeader(1), &[&k[0], &k[1], &k[2]]),
            (Round::SingleLeader(3), &[&k[0], &k[1], &k[2]]),
        ],
    );
    let commitment = original.commitment(b);
    // Tampering with the *grounding* link changes the head commitment, even though only the top
    // link's hash directly includes it: the chain is hash-linked.
    let tampered = chain(
        b,
        &[
            (Round::SingleLeader(1), &[&k[1], &k[2], &k[3]]),
            (Round::SingleLeader(3), &[&k[0], &k[1], &k[2]]),
        ],
    );
    assert_ne!(commitment, tampered.commitment(b));
    // And the commitment depends on the block being justified.
    assert_ne!(commitment, original.commitment(block("A")));
}

// --- Equivocation extraction ------------------------------------------------------------

#[test]
fn extract_returns_none_for_non_conflicts() {
    let (_committee, k) = setup(4);
    // Two identical confirmations of the same block are not a conflict.
    let a = confirmation(
        "A",
        Round::SingleLeader(0),
        false,
        &[&k[0]],
        JustificationChain::default(),
    );
    let b = a.clone();
    assert!(extract_equivocations(&a, &b).is_empty());

    // Two conflicting blocks at *different heights* are not a conflict either: the locking rule
    // is per height, so a validator confirming A at height 0 and validating B at height 1 is not
    // equivocating — no proof must be produced, in either argument order.
    let a_hash = block_at(1, 0, "A");
    let b_hash = block_at(1, 1, "B");
    let a_justification = chain(a_hash, &[(Round::SingleLeader(0), &[&k[0], &k[1], &k[2]])]);
    let a = JustifiedConfirmation {
        header: header_at(1, 0, "A"),
        round: Round::SingleLeader(0),
        first_round: false,
        confirmed_signatures: confirmed_signatures(
            a_hash,
            Round::SingleLeader(0),
            false,
            &a_justification,
            &[&k[0], &k[1], &k[2]],
        ),
        justification: a_justification,
    };
    let b_justification = chain(b_hash, &[(Round::SingleLeader(1), &[&k[0], &k[2], &k[3]])]);
    let b = JustifiedConfirmation {
        header: header_at(1, 1, "B"),
        round: Round::SingleLeader(1),
        first_round: false,
        confirmed_signatures: confirmed_signatures(
            b_hash,
            Round::SingleLeader(1),
            false,
            &b_justification,
            &[&k[0], &k[2], &k[3]],
        ),
        justification: b_justification,
    };
    assert!(extract_equivocations(&a, &b).is_empty());
    assert!(extract_equivocations(&b, &a).is_empty());
}

#[test]
fn extract_is_independent_of_argument_order() {
    let (committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // A is freshly validated and confirmed in round 2. B is grounded in round 1 and
    // re-validated in round 5 (unlocking round 1), then confirmed there. `k[1]`/`k[2]` confirmed A
    // in round 2 and validated B in round 5 under unlocking round 1, whose window [1, 5) contains 2
    // — a genuine lock violation. Extraction must find that real fault for either argument order,
    // never a spurious one read off A's own chain at a round below the B-confirmation.
    let a = confirmation(
        "A",
        Round::SingleLeader(2),
        false,
        &[&k[0], &k[1], &k[2]],
        chain(a_hash, &[(Round::SingleLeader(2), &[&k[0], &k[1], &k[2]])]),
    );
    let b = confirmation(
        "B",
        Round::SingleLeader(5),
        false,
        &[&k[1], &k[2], &k[3]],
        chain(
            b_hash,
            &[
                (Round::SingleLeader(1), &[&k[1], &k[2], &k[3]]),
                (Round::SingleLeader(5), &[&k[1], &k[2], &k[3]]),
            ],
        ),
    );
    for (x, y) in [(&a, &b), (&b, &a)] {
        let proofs = extract_equivocations(x, y);
        for proof in &proofs {
            match proof {
                EquivocationProof::LockViolation {
                    confirmed_round,
                    validated_round,
                    ..
                } => assert!(
                    confirmed_round < validated_round,
                    "the confirmation must fall inside the unlocking-round window"
                ),
                other => panic!("expected a lock violation, got {other:?}"),
            }
        }
        assert_proves(&committee, &proofs, &[&k[1], &k[2]]);
    }
}

#[test]
fn extract_descends_to_grounding_link() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // A was confirmed in round 0; B's chain has its top link in round 3, justified by the
    // grounding link in round 1. Because A's confirmation round (0) is below the top link's
    // unlocking round (1), the walk must skip the top link and catch the violation in the
    // grounding link.
    let a = confirmation(
        "A",
        Round::SingleLeader(0),
        false,
        &[&k[0], &k[1], &k[2]],
        JustificationChain::default(),
    );
    let b = confirmation(
        "B",
        Round::SingleLeader(3),
        false,
        &[&k[1], &k[2], &k[3]],
        chain(
            b_hash,
            &[
                (Round::SingleLeader(1), &[&k[0], &k[2], &k[3]]),
                (Round::SingleLeader(3), &[&k[1], &k[2], &k[3]]),
            ],
        ),
    );
    let proofs = extract_equivocations(&a, &b);
    for proof in &proofs {
        match proof {
            EquivocationProof::LockViolation {
                validated_round,
                validated_unlocking_round,
                validated_commitment,
                ..
            } => {
                assert_eq!(*validated_round, Round::SingleLeader(1));
                assert_eq!(*validated_unlocking_round, None);
                assert_eq!(*validated_commitment, None);
            }
            other => panic!("expected a lock violation, got {other:?}"),
        }
    }
    assert_proves(&committee, &proofs, &[&k[0], &k[2]]);
}

#[test]
fn extract_finds_double_confirm_in_fast_round() {
    let (committee, k) = setup(4);
    // Both blocks confirmed in the fast round — the chain's first round — so both carry the
    // first-round attestation instead of a justification chain. The proof must rebuild the
    // attested vote values, or the genuine signatures would not verify.
    let a = confirmation(
        "A",
        Round::Fast,
        true,
        &[&k[0], &k[1], &k[2]],
        JustificationChain::default(),
    );
    let b = confirmation(
        "B",
        Round::Fast,
        true,
        &[&k[0], &k[2], &k[3]],
        JustificationChain::default(),
    );
    let proofs = extract_equivocations(&a, &b);
    assert!(proofs
        .iter()
        .all(|proof| matches!(proof, EquivocationProof::DoubleVote { .. })));
    assert_proves(&committee, &proofs, &[&k[0], &k[2]]);
}

#[test]
fn extract_finds_double_validation() {
    let (committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    let r = Round::SingleLeader(3);
    // `k[0]` and `k[2]` validated two different blocks in the same round, each under its own
    // (different) justification. That overlap is the double-validation fault. The commitments
    // the voters signed must travel into the proof for their signatures to verify.
    let a_below = chain(a_hash, &[(Round::SingleLeader(1), &[&k[0], &k[1], &k[2]])]);
    let a_commitment = a_below.commitment(a_hash);
    let a = ValidatedQuorum {
        header: header("A"),
        round: r,
        unlocking_round: a_below.top_unlocking_round(),
        justification_commitment: a_commitment,
        signatures: [&k[0], &k[1], &k[2]]
            .iter()
            .map(|kp| {
                sign(
                    a_hash,
                    r,
                    CertificateKind::Validated,
                    a_below.top_unlocking_round(),
                    false,
                    a_commitment,
                    kp,
                )
            })
            .collect(),
    };
    let b_below = chain(b_hash, &[(Round::SingleLeader(2), &[&k[0], &k[2], &k[3]])]);
    let b_commitment = b_below.commitment(b_hash);
    let b = ValidatedQuorum {
        header: header("B"),
        round: r,
        unlocking_round: b_below.top_unlocking_round(),
        justification_commitment: b_commitment,
        signatures: [&k[0], &k[2], &k[3]]
            .iter()
            .map(|kp| {
                sign(
                    b_hash,
                    r,
                    CertificateKind::Validated,
                    b_below.top_unlocking_round(),
                    false,
                    b_commitment,
                    kp,
                )
            })
            .collect(),
    };
    let proofs = extract_double_validations(&a, &b);
    for proof in &proofs {
        match proof {
            EquivocationProof::DoubleVote { kind, .. } => {
                assert_eq!(*kind, CertificateKind::Validated);
            }
            other => panic!("expected a double vote, got {other:?}"),
        }
    }
    assert_proves(&committee, &proofs, &[&k[0], &k[2]]);

    // Validating conflicting blocks in *different* rounds is not itself a fault.
    let a2 = ValidatedQuorum {
        header: header("A"),
        round: Round::SingleLeader(1),
        unlocking_round: None,
        justification_commitment: None,
        signatures: chain(a_hash, &[(Round::SingleLeader(1), &[&k[0], &k[1], &k[2]])]).links()[0]
            .signatures
            .clone(),
    };
    let b2 = ValidatedQuorum {
        header: header("B"),
        round: Round::SingleLeader(3),
        unlocking_round: None,
        justification_commitment: None,
        signatures: chain(b_hash, &[(Round::SingleLeader(3), &[&k[0], &k[2], &k[3]])]).links()[0]
            .signatures
            .clone(),
    };
    assert!(extract_double_validations(&a2, &b2).is_empty());
}

// --- Proof self-verification ------------------------------------------------------------

#[test]
fn proof_check_rejects_same_block() {
    let (committee, k) = setup(4);
    let a = block("A");
    let r = Round::SingleLeader(0);
    let (_, confirmed_signature) = sign(a, r, CertificateKind::Confirmed, None, false, None, &k[0]);
    let (_, validated_signature) = sign(a, r, CertificateKind::Validated, None, false, None, &k[0]);
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: r,
        confirmed_attested: false,
        confirmed_commitment: None,
        confirmed_signature,
        validated_header: header("A"),
        validated_round: r,
        validated_unlocking_round: None,
        validated_commitment: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofSameBlock)
    ));
}

#[test]
fn proof_check_rejects_non_violating_lock() {
    let (committee, k) = setup(4);
    let (a, b) = (block("A"), block("B"));

    // Case 1: `k[0]` validated B in round 2 (fresh, unlocking round `None`) and confirmed a
    // *different* block A in a *later* round 5. The round-2 validation's claim covers only rounds
    // before round 2, so confirming A in round 5 is a legitimate later switch, not a lock violation.
    let (_, validated_signature) = sign(
        b,
        Round::SingleLeader(2),
        CertificateKind::Validated,
        None,
        false,
        None,
        &k[0],
    );
    let (_, confirmed_signature) = sign(
        a,
        Round::SingleLeader(5),
        CertificateKind::Confirmed,
        None,
        false,
        None,
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: Round::SingleLeader(5),
        confirmed_attested: false,
        confirmed_commitment: None,
        confirmed_signature,
        validated_header: header("B"),
        validated_round: Round::SingleLeader(2),
        validated_unlocking_round: None,
        validated_commitment: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofNoLockViolation)
    ));

    // Case 2: the confirmation is in round 0, but the unlocking-round claim only covers rounds at
    // or above 2, so confirming a different block in round 0 is not actually a violation.
    let (_, confirmed_signature) = sign(
        a,
        Round::SingleLeader(0),
        CertificateKind::Confirmed,
        None,
        false,
        None,
        &k[0],
    );
    let commitment = Some(CryptoHash::test_hash("cited quorum"));
    let (_, validated_signature) = sign(
        b,
        Round::SingleLeader(3),
        CertificateKind::Validated,
        Some(Round::SingleLeader(2)),
        false,
        commitment,
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: Round::SingleLeader(0),
        confirmed_attested: false,
        confirmed_commitment: None,
        confirmed_signature,
        validated_header: header("B"),
        validated_round: Round::SingleLeader(3),
        validated_unlocking_round: Some(Round::SingleLeader(2)),
        validated_commitment: commitment,
        validated_signature,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofNoLockViolation)
    ));
}

#[test]
fn proof_check_rejects_forged_signature() {
    let (committee, k) = setup(4);
    let (a, b) = (block("A"), block("B"));
    let r = Round::SingleLeader(0);
    let (_, confirmed_signature) = sign(a, r, CertificateKind::Confirmed, None, false, None, &k[0]);
    // Signed by k[1], but the proof names k[0].
    let (_, validated_signature) = sign(b, r, CertificateKind::Validated, None, false, None, &k[1]);
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: r,
        confirmed_attested: false,
        confirmed_commitment: None,
        confirmed_signature,
        validated_header: header("B"),
        validated_round: r,
        validated_unlocking_round: None,
        validated_commitment: None,
        validated_signature,
    };
    assert!(proof.check(&committee).is_err());
}

#[test]
fn extract_finds_lock_violation_against_attested_confirmation() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // A was confirmed in the chain's first round, `MultiLeader(0)`, so its votes carry the
    // attestation and it has no justification chain. `k[0]` and `k[2]` then validated a fork B
    // in round 1 with unlocking round `None`, contradicting their own confirmation of A. The
    // extracted proof's confirmation vote is the attested one, and must verify as such.
    let a = confirmation(
        "A",
        Round::MultiLeader(0),
        true,
        &[&k[0], &k[1], &k[2]],
        JustificationChain::default(),
    );
    let b = confirmation(
        "B",
        Round::SingleLeader(1),
        false,
        &[&k[0], &k[2], &k[3]],
        chain(b_hash, &[(Round::SingleLeader(1), &[&k[0], &k[2], &k[3]])]),
    );
    for (x, y) in [(&a, &b), (&b, &a)] {
        let proofs = extract_equivocations(x, y);
        assert!(proofs
            .iter()
            .all(|proof| matches!(proof, EquivocationProof::LockViolation { .. })));
        assert_proves(&committee, &proofs, &[&k[0], &k[2]]);
    }
}

#[test]
fn extract_finds_first_round_violation() {
    let (committee, k) = setup(4);
    // A was confirmed in the fast round. A fork B was then confirmed in `MultiLeader(0)` with
    // the first-round attestation and no justification chain — so neither block's chain can
    // straddle the other's confirmation, and the rounds differ. The only attributable fault is
    // the false attestation itself: `k[0]` and `k[2]` confirmed A below the round they attested
    // to be the chain's first.
    let a = confirmation(
        "A",
        Round::Fast,
        true,
        &[&k[0], &k[1], &k[2]],
        JustificationChain::default(),
    );
    let b = confirmation(
        "B",
        Round::MultiLeader(0),
        true,
        &[&k[0], &k[2], &k[3]],
        JustificationChain::default(),
    );
    for (x, y) in [(&a, &b), (&b, &a)] {
        let proofs = extract_equivocations(x, y);
        for proof in &proofs {
            match proof {
                EquivocationProof::FirstRoundViolation {
                    attested_round,
                    earlier_round,
                    ..
                } => {
                    assert_eq!(*attested_round, Round::MultiLeader(0));
                    assert_eq!(*earlier_round, Round::Fast);
                }
                other => panic!("expected a first-round violation, got {other:?}"),
            }
        }
        assert_proves(&committee, &proofs, &[&k[0], &k[2]]);
    }
}

#[test]
fn proof_check_rejects_non_violating_first_round() {
    let (committee, k) = setup(4);
    // The "earlier" confirmation is *above* the attested first round, so the two votes are
    // compatible: confirming in a later round never contradicts the attestation.
    let (_, attested_signature) = sign(
        block("A"),
        Round::Fast,
        CertificateKind::Confirmed,
        None,
        true,
        None,
        &k[0],
    );
    let (_, earlier_signature) = sign(
        block("B"),
        Round::MultiLeader(0),
        CertificateKind::Confirmed,
        None,
        false,
        None,
        &k[0],
    );
    let proof = EquivocationProof::FirstRoundViolation {
        validator: k[0].public_key,
        attested_header: header("A"),
        attested_round: Round::Fast,
        attested_commitment: None,
        attested_signature,
        earlier_header: header("B"),
        earlier_round: Round::MultiLeader(0),
        earlier_attested: false,
        earlier_commitment: None,
        earlier_signature,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofNoFirstRoundViolation)
    ));
}

#[test]
fn proof_check_rejects_different_chain() {
    let (committee, k) = setup(4);
    // `k[0]` legitimately confirmed a block on chain 1 and validated a different block on chain 2,
    // with round numbers that would otherwise satisfy the lock-violation window. Both signatures
    // are genuine, but the votes concern different chains, so this is not a violation.
    let confirmed_header = header_at(1, 0, "A");
    let validated_header = header_at(2, 0, "B");
    let (_, confirmed_signature) = sign(
        CryptoHash::new(&confirmed_header),
        Round::SingleLeader(0),
        CertificateKind::Confirmed,
        None,
        false,
        None,
        &k[0],
    );
    let (_, validated_signature) = sign(
        CryptoHash::new(&validated_header),
        Round::SingleLeader(1),
        CertificateKind::Validated,
        None,
        false,
        None,
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header,
        confirmed_round: Round::SingleLeader(0),
        confirmed_attested: false,
        confirmed_commitment: None,
        confirmed_signature,
        validated_header,
        validated_round: Round::SingleLeader(1),
        validated_unlocking_round: None,
        validated_commitment: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofDifferentChainOrHeight)
    ));
}

// --- Chain auditing -----------------------------------------------------------------------

#[test]
fn audit_blames_attesters_of_invalid_link() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // The grounding link is a sub-quorum (one signer out of four), which certificate
    // verification never notices: the voters above signed its commitment, attesting they had
    // verified it. Auditing re-checks the links for real and convicts exactly those attesters.
    let justification = chain(
        b_hash,
        &[
            (Round::SingleLeader(1), &[&k[0]]),
            (Round::SingleLeader(3), &[&k[0], &k[1], &k[2]]),
        ],
    );
    let confirmation = confirmation(
        "B",
        Round::SingleLeader(3),
        false,
        &[&k[1], &k[2], &k[3]],
        justification,
    );
    let proofs = audit_confirmation(&confirmation, &committee);
    for proof in &proofs {
        match proof {
            EquivocationProof::InvalidJustification { round, kind, .. } => {
                assert_eq!(*round, Round::SingleLeader(3));
                assert_eq!(*kind, CertificateKind::Validated);
            }
            other => panic!("expected an invalid justification, got {other:?}"),
        }
    }
    assert_proves(&committee, &proofs, &[&k[0], &k[1], &k[2]]);

    // A sound chain audits clean.
    let sound = super::JustifiedConfirmation {
        justification: chain(
            b_hash,
            &[
                (Round::SingleLeader(1), &[&k[0], &k[1], &k[2]]),
                (Round::SingleLeader(3), &[&k[0], &k[1], &k[2]]),
            ],
        ),
        ..confirmation
    };
    assert!(audit_confirmation(&sound, &committee).is_empty());
}

#[test]
fn audit_blames_confirmation_quorum_for_invalid_top_link() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // The chain's only link is a sub-quorum; the level above it is the confirmation quorum
    // itself, so its signers are the ones convicted.
    let justification = chain(b_hash, &[(Round::SingleLeader(1), &[&k[0]])]);
    let confirmation = confirmation(
        "B",
        Round::SingleLeader(1),
        false,
        &[&k[1], &k[2], &k[3]],
        justification,
    );
    let proofs = audit_confirmation(&confirmation, &committee);
    for proof in &proofs {
        match proof {
            EquivocationProof::InvalidJustification { round, kind, .. } => {
                assert_eq!(*round, Round::SingleLeader(1));
                assert_eq!(*kind, CertificateKind::Confirmed);
            }
            other => panic!("expected an invalid justification, got {other:?}"),
        }
    }
    assert_proves(&committee, &proofs, &[&k[1], &k[2], &k[3]]);
}

#[test]
fn audit_catches_broken_linkage() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // The top link's voters signed the wrong unlocking round (`2` instead of the grounding
    // link's round `1`), so their signatures do not verify over the payload the chain derives
    // for them: the link is not a genuine quorum, and the confirmation quorum that committed to
    // it is convicted.
    let ground = chain(b_hash, &[(Round::SingleLeader(1), &[&k[0], &k[1], &k[2]])]);
    let broken_signatures = [&k[0], &k[1], &k[2]]
        .iter()
        .map(|kp| {
            sign(
                b_hash,
                Round::SingleLeader(3),
                CertificateKind::Validated,
                Some(Round::SingleLeader(2)),
                false,
                ground.commitment(b_hash),
                kp,
            )
        })
        .collect();
    let justification = ground.append(Round::SingleLeader(3), broken_signatures);
    let confirmation = confirmation(
        "B",
        Round::SingleLeader(3),
        false,
        &[&k[0], &k[2], &k[3]],
        justification,
    );
    let proofs = audit_confirmation(&confirmation, &committee);
    assert!(proofs.iter().all(|proof| matches!(
        proof,
        EquivocationProof::InvalidJustification {
            kind: CertificateKind::Confirmed,
            ..
        }
    )));
    assert_proves(&committee, &proofs, &[&k[0], &k[2], &k[3]]);
}

#[test]
fn proof_check_rejects_valid_justification() {
    let (committee, k) = setup(4);
    let b_hash = block("B");
    // A genuine vote citing a genuine quorum: the signature verifies, but the opening is a valid
    // quorum, so there is no fault to attribute.
    let cited = chain(b_hash, &[(Round::SingleLeader(1), &[&k[0], &k[1], &k[2]])]);
    let opening = CommittedQuorum {
        value_hash: b_hash,
        round: Round::SingleLeader(1),
        unlocking_round: None,
        previous: None,
        signatures: cited.links()[0].signatures.clone(),
    };
    let (_, signature) = sign(
        b_hash,
        Round::SingleLeader(3),
        CertificateKind::Validated,
        Some(Round::SingleLeader(1)),
        false,
        Some(opening.commitment()),
        &k[0],
    );
    let proof = EquivocationProof::InvalidJustification {
        validator: k[0].public_key,
        header: header("B"),
        round: Round::SingleLeader(3),
        kind: CertificateKind::Validated,
        unlocking_round: Some(Round::SingleLeader(1)),
        first_round: false,
        signature,
        opening,
    };
    assert!(matches!(
        proof.check(&committee),
        Err(ChainError::EquivocationProofValidJustification)
    ));
}
