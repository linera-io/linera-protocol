// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{
        AccountSecretKey, CryptoHash, ValidatorKeypair, ValidatorPublicKey, ValidatorSignature,
    },
    data_types::{BlockHeight, Epoch, Round, Timestamp},
    identifiers::ChainId,
};
use linera_execution::committee::Committee;

use super::{
    extract_double_validation, extract_equivocation, EquivocationProof, JustificationChain,
    JustificationLink, JustifiedConfirmation, ValidatedQuorum,
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
    key: &ValidatorKeypair,
) -> (ValidatorPublicKey, ValidatorSignature) {
    let value = VoteValue(block, round, kind, unlocking_round, false);
    (
        key.public_key,
        ValidatorSignature::new(&value, &key.secret_key),
    )
}

/// A `ValidatedBlock` quorum for `block` in `round`, cast with the given `unlocking_round`.
fn validated_link(
    block: CryptoHash,
    round: Round,
    unlocking_round: Option<Round>,
    signers: &[&ValidatorKeypair],
) -> JustificationLink {
    JustificationLink {
        round,
        signatures: signers
            .iter()
            .map(|kp| {
                sign(
                    block,
                    round,
                    CertificateKind::Validated,
                    unlocking_round,
                    kp,
                )
            })
            .collect(),
    }
}

fn confirmed_signatures(
    block: CryptoHash,
    round: Round,
    signers: &[&ValidatorKeypair],
) -> Vec<(ValidatorPublicKey, ValidatorSignature)> {
    signers
        .iter()
        .map(|kp| sign(block, round, CertificateKind::Confirmed, None, kp))
        .collect()
}

// --- Chain verification -----------------------------------------------------------------

#[test]
fn verify_accepts_valid_chain() {
    let (committee, k) = setup(4);
    let b = block("B");
    // Link 0 at round 1 is the grounding link; link 1 at round 3 is justified by it.
    let chain = JustificationChain::new(vec![
        validated_link(b, Round::SingleLeader(1), None, &[&k[0], &k[1], &k[2]]),
        validated_link(
            b,
            Round::SingleLeader(3),
            Some(Round::SingleLeader(1)),
            &[&k[0], &k[1], &k[2]],
        ),
    ]);
    assert!(chain.verify(b, &committee).is_ok());
    assert_eq!(chain.top_unlocking_round(), Some(Round::SingleLeader(3)));
}

#[test]
fn verify_rejects_non_increasing_rounds() {
    let (committee, k) = setup(4);
    let b = block("B");
    let chain = JustificationChain::new(vec![
        validated_link(b, Round::SingleLeader(1), None, &[&k[0], &k[1], &k[2]]),
        validated_link(
            b,
            Round::SingleLeader(1),
            Some(Round::SingleLeader(1)),
            &[&k[0], &k[1], &k[2]],
        ),
    ]);
    assert!(matches!(
        chain.verify(b, &committee),
        Err(ChainError::JustificationRoundsNotIncreasing)
    ));
}

#[test]
fn verify_rejects_broken_linkage() {
    let (committee, k) = setup(4);
    let b = block("B");
    // Link 1's votes signed unlocking round `SingleLeader(2)`, but the previous link is at
    // `SingleLeader(1)`, so the unlocking round the verifier derives doesn't match what was signed.
    let chain = JustificationChain::new(vec![
        validated_link(b, Round::SingleLeader(1), None, &[&k[0], &k[1], &k[2]]),
        validated_link(
            b,
            Round::SingleLeader(3),
            Some(Round::SingleLeader(2)),
            &[&k[0], &k[1], &k[2]],
        ),
    ]);
    assert!(chain.verify(b, &committee).is_err());
}

#[test]
fn verify_rejects_sub_quorum() {
    let (committee, k) = setup(4);
    let b = block("B");
    let chain = JustificationChain::new(vec![validated_link(
        b,
        Round::SingleLeader(0),
        None,
        &[&k[0]],
    )]);
    assert!(matches!(
        chain.verify(b, &committee),
        Err(ChainError::CertificateRequiresQuorum)
    ));
}

// --- Equivocation extraction ------------------------------------------------------------

#[test]
fn extract_returns_none_for_same_block() {
    let (_committee, k) = setup(4);
    let a = JustifiedConfirmation {
        header: header("A"),
        round: Round::SingleLeader(0),
        confirmed_signatures: confirmed_signatures(block("A"), Round::SingleLeader(0), &[&k[0]]),
        justification: JustificationChain::default(),
    };
    let b = a.clone();
    assert!(extract_equivocation(&a, &b).is_none());
}

#[test]
fn extract_finds_lock_violation() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // A is confirmed in round 0; B is freshly validated and confirmed in round 1. `k[0]` and
    // `k[2]` confirmed A in round 0 and then validated B in round 1 with unlocking round `None` —
    // whose claim covers round 0 — so confirming A in round 0 is a lock violation (round 0 ∈ [0, 1)).
    let a = JustifiedConfirmation {
        header: header("A"),
        round: Round::SingleLeader(0),
        confirmed_signatures: confirmed_signatures(
            a_hash,
            Round::SingleLeader(0),
            &[&k[0], &k[1], &k[2]],
        ),
        justification: JustificationChain::new(vec![validated_link(
            a_hash,
            Round::SingleLeader(0),
            None,
            &[&k[0], &k[1], &k[2]],
        )]),
    };
    let b = JustifiedConfirmation {
        header: header("B"),
        round: Round::SingleLeader(1),
        confirmed_signatures: confirmed_signatures(
            b_hash,
            Round::SingleLeader(1),
            &[&k[0], &k[2], &k[3]],
        ),
        justification: JustificationChain::new(vec![validated_link(
            b_hash,
            Round::SingleLeader(1),
            None,
            &[&k[0], &k[2], &k[3]],
        )]),
    };
    let proof = extract_equivocation(&a, &b).expect("a proof must exist");
    assert!(matches!(proof, EquivocationProof::LockViolation { .. }));
    assert!(proof.check().is_ok());
    assert!([k[0].public_key, k[2].public_key].contains(&proof.validator()));
}

#[test]
fn proof_check_rejects_confirmation_after_validation() {
    let (_committee, k) = setup(4);
    let (a, b) = (block("A"), block("B"));
    // `k[0]` validated B in round 2 (fresh, unlocking round `None`) and confirmed a *different* block A in a
    // *later* round 5. The round-2 validation's claim covers only rounds before round 2, so
    // confirming A in round 5 is a legitimate later switch, not a lock violation.
    let (_, validated_signature) = sign(
        b,
        Round::SingleLeader(2),
        CertificateKind::Validated,
        None,
        &k[0],
    );
    let (_, confirmed_signature) = sign(
        a,
        Round::SingleLeader(5),
        CertificateKind::Confirmed,
        None,
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: Round::SingleLeader(5),
        confirmed_signature,
        validated_header: header("B"),
        validated_round: Round::SingleLeader(2),
        validated_unlocking_round: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(),
        Err(ChainError::EquivocationProofNoLockViolation)
    ));
}

#[test]
fn extract_is_independent_of_argument_order() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // A is freshly validated and confirmed in round 2. B is grounded in round 1 and
    // re-validated in round 5 (unlocking round 1), then confirmed there. `k[1]`/`k[2]` confirmed A
    // in round 2 and validated B in round 5 under unlocking round 1, whose window [1, 5) contains 2
    // — a genuine lock
    // violation. Extraction must find that real fault for either argument order, never a
    // spurious one read off A's own chain at a round below the B-confirmation.
    let a = JustifiedConfirmation {
        header: header("A"),
        round: Round::SingleLeader(2),
        confirmed_signatures: confirmed_signatures(
            a_hash,
            Round::SingleLeader(2),
            &[&k[0], &k[1], &k[2]],
        ),
        justification: JustificationChain::new(vec![validated_link(
            a_hash,
            Round::SingleLeader(2),
            None,
            &[&k[0], &k[1], &k[2]],
        )]),
    };
    let b = JustifiedConfirmation {
        header: header("B"),
        round: Round::SingleLeader(5),
        confirmed_signatures: confirmed_signatures(
            b_hash,
            Round::SingleLeader(5),
            &[&k[1], &k[2], &k[3]],
        ),
        justification: JustificationChain::new(vec![
            validated_link(b_hash, Round::SingleLeader(1), None, &[&k[1], &k[2], &k[3]]),
            validated_link(
                b_hash,
                Round::SingleLeader(5),
                Some(Round::SingleLeader(1)),
                &[&k[1], &k[2], &k[3]],
            ),
        ]),
    };
    for (x, y) in [(&a, &b), (&b, &a)] {
        let proof = extract_equivocation(x, y).expect("a proof must exist");
        assert!(proof.check().is_ok());
        match &proof {
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
}

#[test]
fn extract_descends_to_grounding_link() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // A was confirmed in round 0; B's chain has its top link in round 3, justified by the
    // grounding link in round 1. Because A's confirmation round (0) is below the top link's
    // unlocking round (1), the walk must skip the top link and catch the violation in the
    // grounding link.
    let a = JustifiedConfirmation {
        header: header("A"),
        round: Round::SingleLeader(0),
        confirmed_signatures: confirmed_signatures(
            a_hash,
            Round::SingleLeader(0),
            &[&k[0], &k[1], &k[2]],
        ),
        justification: JustificationChain::default(),
    };
    let b = JustifiedConfirmation {
        header: header("B"),
        round: Round::SingleLeader(3),
        confirmed_signatures: confirmed_signatures(
            b_hash,
            Round::SingleLeader(3),
            &[&k[1], &k[2], &k[3]],
        ),
        justification: JustificationChain::new(vec![
            validated_link(b_hash, Round::SingleLeader(1), None, &[&k[0], &k[2], &k[3]]),
            validated_link(
                b_hash,
                Round::SingleLeader(3),
                Some(Round::SingleLeader(1)),
                &[&k[1], &k[2], &k[3]],
            ),
        ]),
    };
    let proof = extract_equivocation(&a, &b).expect("a proof must exist");
    match &proof {
        EquivocationProof::LockViolation {
            validator,
            validated_round,
            validated_unlocking_round,
            ..
        } => {
            assert_eq!(*validator, k[0].public_key);
            assert_eq!(*validated_round, Round::SingleLeader(1));
            assert_eq!(*validated_unlocking_round, None);
        }
        other => panic!("expected a lock violation, got {other:?}"),
    }
    assert!(proof.check().is_ok());
}

#[test]
fn extract_finds_double_confirm_in_fast_round() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // Both blocks confirmed in the fast round, so neither carries a justification chain.
    let a = JustifiedConfirmation {
        header: header("A"),
        round: Round::Fast,
        confirmed_signatures: confirmed_signatures(a_hash, Round::Fast, &[&k[0], &k[1], &k[2]]),
        justification: JustificationChain::default(),
    };
    let b = JustifiedConfirmation {
        header: header("B"),
        round: Round::Fast,
        confirmed_signatures: confirmed_signatures(b_hash, Round::Fast, &[&k[0], &k[2], &k[3]]),
        justification: JustificationChain::default(),
    };
    let proof = extract_equivocation(&a, &b).expect("a proof must exist");
    assert!(matches!(proof, EquivocationProof::DoubleVote { .. }));
    assert!(proof.check().is_ok());
    assert!([k[0].public_key, k[2].public_key].contains(&proof.validator()));
}

#[test]
fn extract_finds_double_validation() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    let r = Round::SingleLeader(3);
    // `k[0]` and `k[2]` validated two different blocks in the same round, each under its own
    // (different) unlocking round. That overlap is the double-validation fault.
    let a = ValidatedQuorum {
        header: header("A"),
        round: r,
        unlocking_round: Some(Round::SingleLeader(1)),
        signatures: validated_link(
            a_hash,
            r,
            Some(Round::SingleLeader(1)),
            &[&k[0], &k[1], &k[2]],
        )
        .signatures,
    };
    let b = ValidatedQuorum {
        header: header("B"),
        round: r,
        unlocking_round: Some(Round::SingleLeader(2)),
        signatures: validated_link(
            b_hash,
            r,
            Some(Round::SingleLeader(2)),
            &[&k[0], &k[2], &k[3]],
        )
        .signatures,
    };
    let proof = extract_double_validation(&a, &b).expect("a proof must exist");
    match &proof {
        EquivocationProof::DoubleVote { kind, .. } => {
            assert_eq!(*kind, CertificateKind::Validated);
        }
        other => panic!("expected a double vote, got {other:?}"),
    }
    assert!(proof.check().is_ok());
    assert!([k[0].public_key, k[2].public_key].contains(&proof.validator()));
}

#[test]
fn extract_double_validation_ignores_different_rounds() {
    let (_committee, k) = setup(4);
    let (a_hash, b_hash) = (block("A"), block("B"));
    // Validating conflicting blocks in different rounds is not a fault on its own.
    let a = ValidatedQuorum {
        header: header("A"),
        round: Round::SingleLeader(1),
        unlocking_round: None,
        signatures: validated_link(a_hash, Round::SingleLeader(1), None, &[&k[0], &k[1], &k[2]])
            .signatures,
    };
    let b = ValidatedQuorum {
        header: header("B"),
        round: Round::SingleLeader(3),
        unlocking_round: Some(Round::SingleLeader(1)),
        signatures: validated_link(
            b_hash,
            Round::SingleLeader(3),
            Some(Round::SingleLeader(1)),
            &[&k[0], &k[2], &k[3]],
        )
        .signatures,
    };
    assert!(extract_double_validation(&a, &b).is_none());
}

// --- Proof self-verification ------------------------------------------------------------

#[test]
fn proof_check_rejects_same_block() {
    let (_committee, k) = setup(4);
    let a = block("A");
    let r = Round::SingleLeader(0);
    let (_, confirmed_signature) = sign(a, r, CertificateKind::Confirmed, None, &k[0]);
    let (_, validated_signature) = sign(a, r, CertificateKind::Validated, None, &k[0]);
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: r,
        confirmed_signature,
        validated_header: header("A"),
        validated_round: r,
        validated_unlocking_round: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(),
        Err(ChainError::EquivocationProofSameBlock)
    ));
}

#[test]
fn proof_check_rejects_non_violating_lock() {
    let (_committee, k) = setup(4);
    let (a, b) = (block("A"), block("B"));
    // The confirmation is in round 0, but the unlocking-round claim only covers rounds at or
    // above 2, so
    // confirming a different block in round 0 is not actually a violation.
    let (_, confirmed_signature) = sign(
        a,
        Round::SingleLeader(0),
        CertificateKind::Confirmed,
        None,
        &k[0],
    );
    let (_, validated_signature) = sign(
        b,
        Round::SingleLeader(3),
        CertificateKind::Validated,
        Some(Round::SingleLeader(2)),
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: Round::SingleLeader(0),
        confirmed_signature,
        validated_header: header("B"),
        validated_round: Round::SingleLeader(3),
        validated_unlocking_round: Some(Round::SingleLeader(2)),
        validated_signature,
    };
    assert!(matches!(
        proof.check(),
        Err(ChainError::EquivocationProofNoLockViolation)
    ));
}

#[test]
fn proof_check_rejects_forged_signature() {
    let (_committee, k) = setup(4);
    let (a, b) = (block("A"), block("B"));
    let r = Round::SingleLeader(0);
    let (_, confirmed_signature) = sign(a, r, CertificateKind::Confirmed, None, &k[0]);
    // Signed by k[1], but the proof names k[0].
    let (_, validated_signature) = sign(b, r, CertificateKind::Validated, None, &k[1]);
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header: header("A"),
        confirmed_round: r,
        confirmed_signature,
        validated_header: header("B"),
        validated_round: r,
        validated_unlocking_round: None,
        validated_signature,
    };
    assert!(proof.check().is_err());
}

#[test]
fn extract_returns_none_across_heights() {
    let (_committee, k) = setup(4);
    // Same shape as `extract_finds_lock_violation`, but the two blocks are at *different heights*.
    // The locking rule is per height, so a validator confirming A at height 0 and validating B at
    // height 1 is not equivocating — no proof must be produced, in either argument order.
    let a_hash = block_at(1, 0, "A");
    let b_hash = block_at(1, 1, "B");
    let a = JustifiedConfirmation {
        header: header_at(1, 0, "A"),
        round: Round::SingleLeader(0),
        confirmed_signatures: confirmed_signatures(
            a_hash,
            Round::SingleLeader(0),
            &[&k[0], &k[1], &k[2]],
        ),
        justification: JustificationChain::new(vec![validated_link(
            a_hash,
            Round::SingleLeader(0),
            None,
            &[&k[0], &k[1], &k[2]],
        )]),
    };
    let b = JustifiedConfirmation {
        header: header_at(1, 1, "B"),
        round: Round::SingleLeader(1),
        confirmed_signatures: confirmed_signatures(
            b_hash,
            Round::SingleLeader(1),
            &[&k[0], &k[2], &k[3]],
        ),
        justification: JustificationChain::new(vec![validated_link(
            b_hash,
            Round::SingleLeader(1),
            None,
            &[&k[0], &k[2], &k[3]],
        )]),
    };
    assert!(extract_equivocation(&a, &b).is_none());
    assert!(extract_equivocation(&b, &a).is_none());
}

#[test]
fn proof_check_rejects_different_chain() {
    let (_committee, k) = setup(4);
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
        &k[0],
    );
    let (_, validated_signature) = sign(
        CryptoHash::new(&validated_header),
        Round::SingleLeader(1),
        CertificateKind::Validated,
        None,
        &k[0],
    );
    let proof = EquivocationProof::LockViolation {
        validator: k[0].public_key,
        confirmed_header,
        confirmed_round: Round::SingleLeader(0),
        confirmed_signature,
        validated_header,
        validated_round: Round::SingleLeader(1),
        validated_unlocking_round: None,
        validated_signature,
    };
    assert!(matches!(
        proof.check(),
        Err(ChainError::EquivocationProofDifferentChainOrHeight)
    ));
}
