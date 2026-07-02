// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{AccountSecretKey, Ed25519SecretKey, Secp256k1SecretKey, ValidatorKeypair},
    data_types::Amount,
};

use super::*;
use crate::{
    block::{Block, BlockBodyField, ConfirmedBlock, ValidatedBlock},
    test::{make_first_block, BlockTestExt},
};

fn dummy_chain_id(index: u32) -> ChainId {
    ChainId(CryptoHash::test_hash(format!("chain{index}")))
}

fn sample_block() -> Block {
    BlockExecutionOutcome {
        messages: vec![Vec::new()],
        previous_message_blocks: BTreeMap::new(),
        previous_event_blocks: BTreeMap::new(),
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
        blobs: vec![Vec::new()],
        operation_results: vec![OperationResult::default()],
    }
    .with(make_first_block(dummy_chain_id(1)).with_simple_transfer(dummy_chain_id(2), Amount::ONE))
}

#[test]
fn block_hash_is_header_hash() {
    let block = sample_block();
    assert_eq!(block.hash(), CryptoHash::new(&block.header));
}

#[test]
fn confirmed_block_hash_is_header_hash() {
    let block = sample_block();
    let header_hash = CryptoHash::new(&block.header);
    assert_eq!(ConfirmedBlock::new(block.clone()).hash(), header_hash);
    assert_eq!(ValidatedBlock::new(block).hash(), header_hash);
}

#[test]
fn block_serde_round_trip_preserves_hash() {
    let block = sample_block();
    let hash = block.hash();
    let bytes = bcs::to_bytes(&block).unwrap();
    let restored: Block = bcs::from_bytes(&bytes).unwrap();
    assert_eq!(restored, block);
    assert_eq!(restored.hash(), hash);
}

#[test]
fn header_verifies_each_body_field() {
    let block = sample_block();
    let h = &block.header;
    let b = &block.body;
    assert!(h.verifies(b.transactions.clone()));
    assert!(h.verifies(b.messages.clone()));
    assert!(h.verifies(b.previous_message_blocks.clone()));
    assert!(h.verifies(b.previous_event_blocks.clone()));
    assert!(h.verifies(b.oracle_responses.clone()));
    assert!(h.verifies(b.events.clone()));
    assert!(h.verifies(b.blobs.clone()));
    assert!(h.verifies(b.operation_results.clone()));
}

#[test]
fn header_rejects_wrong_field() {
    let block = sample_block();
    // sample_block contains one transfer, so an empty transactions list must not verify.
    assert!(!block
        .header
        .verifies(BlockBodyField::Transactions(Vec::new())));
}

#[test]
fn light_client_verifies_header_and_one_field() {
    let validator_key_pair = ValidatorKeypair::generate();
    let account_secret = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
    let committee = Committee::make_simple(vec![(
        validator_key_pair.public_key,
        account_secret.public(),
    )]);

    let value = ConfirmedBlock::new(sample_block());
    let vote = LiteVote::new(
        LiteValue::new(&value),
        Round::Fast,
        &validator_key_pair.secret_key,
    );
    let mut builder = SignatureAggregator::new(value, Round::Fast, None, false, &committee);
    let certificate = builder
        .append(validator_key_pair.public_key, vote.signature)
        .unwrap()
        .unwrap();

    // The signed commitment is reproducible from the header alone.
    let header = certificate.block().header.clone();
    assert_eq!(CryptoHash::new(&header), certificate.hash());

    // One body field can be proven against that header without the rest of the body.
    let events = certificate.block().body.events.clone();
    assert!(header.verifies(events));
}

#[test]
fn confirmed_certificate_check_and_absent_chain() {
    use crate::{
        justification::JustificationChain, test::VoteTestExt, types::ConfirmedBlockCertificate,
    };

    let validator = ValidatorKeypair::generate();
    let account = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
    let committee = Committee::make_simple(vec![(validator.public_key, account.public())]);

    // A confirmed certificate in a non-fast round carrying no justification chain is accepted
    // only when its quorum carries the first-round attestation: such a block is always the lower
    // one in any fork, so it never needs a chain of its own.
    let round = Round::SingleLeader(0);
    let value = ConfirmedBlock::new(sample_block());
    let quorum = Vote::new_with_first_round(value.clone(), round, true, &validator.secret_key)
        .into_certificate(validator.public_key);
    let certificate = ConfirmedBlockCertificate::from_parts(quorum, JustificationChain::default());
    assert!(certificate.check(&committee).is_ok());

    // Without the attestation, a non-fast-round confirmation with no chain is rejected.
    let quorum =
        Vote::new(value, round, &validator.secret_key).into_certificate(validator.public_key);
    let certificate = ConfirmedBlockCertificate::from_parts(quorum, JustificationChain::default());
    assert!(certificate.check(&committee).is_err());
}

#[test]
fn lite_certificate_check_binds_justification_chain() {
    use crate::{
        justification::{JustificationChain, JustificationLink},
        test::VoteTestExt,
        types::{CertificateKind, ValidatedBlockCertificate},
    };

    let validator = ValidatorKeypair::generate();
    let account = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
    let committee = Committee::make_simple(vec![(validator.public_key, account.public())]);

    let value = ValidatedBlock::new(sample_block());
    let hash = value.hash();
    let ground_round = Round::SingleLeader(1);
    let round = Round::SingleLeader(3);

    // A grounding validated quorum in the lower round (unlocking round `None`) ...
    let ground_value = VoteValue(hash, ground_round, CertificateKind::Validated, None, false);
    let ground_signature = ValidatorSignature::new(&ground_value, &validator.secret_key);
    let below = JustificationChain::new(vec![JustificationLink {
        round: ground_round,
        signatures: vec![(validator.public_key, ground_signature)],
    }]);
    // ... justifies a validated quorum in `round` that signs the unlocking round `ground_round`.
    let quorum =
        Vote::new_with_unlocking_round(value, round, Some(ground_round), &validator.secret_key)
            .into_certificate(validator.public_key);
    let certificate = ValidatedBlockCertificate::from_parts(quorum, below);

    // The lite certificate carries the chain, so its check — the only gate the worker applies to
    // the certificate a retry proposal carries — passes.
    assert!(certificate.lite_certificate().check(&committee).is_ok());

    // Stripping the chain while keeping the signed unlocking round leaves the two unbound. The
    // lite check must reject it, or a proposer could wedge the height with a chainless certificate
    // that every honest validator then stores and later fails to re-verify.
    let mut stripped = certificate.lite_certificate();
    stripped.justification = Default::default();
    assert!(matches!(
        stripped.check(&committee),
        Err(ChainError::JustificationUnlockingRoundMismatch)
    ));
}

#[test]
fn test_signed_values() {
    let validator1_key_pair = ValidatorKeypair::generate();
    let validator2_key_pair = ValidatorKeypair::generate();

    let block = BlockExecutionOutcome {
        messages: vec![Vec::new()],
        previous_message_blocks: BTreeMap::new(),
        previous_event_blocks: BTreeMap::new(),
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
        blobs: vec![Vec::new()],
        operation_results: vec![OperationResult::default()],
    }
    .with(make_first_block(dummy_chain_id(1)).with_simple_transfer(dummy_chain_id(2), Amount::ONE));
    let confirmed_value = ConfirmedBlock::new(block.clone());

    let confirmed_vote = LiteVote::new(
        LiteValue::new(&confirmed_value),
        Round::Fast,
        &validator1_key_pair.secret_key,
    );
    assert!(confirmed_vote.check(validator1_key_pair.public_key).is_ok());

    let validated_value = ValidatedBlock::new(block);
    let validated_vote = LiteVote::new(
        LiteValue::new(&validated_value),
        Round::Fast,
        &validator1_key_pair.secret_key,
    );
    assert_ne!(
        confirmed_vote.value, validated_vote.value,
        "Confirmed and validated votes should be different, even if for the same block"
    );

    let v = LiteVote::new(
        LiteValue::new(&confirmed_value),
        Round::Fast,
        &validator2_key_pair.secret_key,
    );
    // The vote was created with validator2's key but we'll check it with validator1's key
    assert!(v.check(validator1_key_pair.public_key).is_err());

    assert!(validated_vote.check(validator1_key_pair.public_key).is_ok());
    assert!(confirmed_vote.check(validator1_key_pair.public_key).is_ok());

    let mut v = validated_vote.clone();
    // Use signature from ConfirmedBlock to sign a ValidatedBlock.
    v.signature = confirmed_vote.signature;
    assert!(
        v.check(validator1_key_pair.public_key).is_err(),
        "Confirmed and validated votes must not be interchangeable"
    );

    let mut v = confirmed_vote.clone();
    v.signature = validated_vote.signature;
    assert!(
        v.check(validator1_key_pair.public_key).is_err(),
        "Confirmed and validated votes must not be interchangeable"
    );
}

#[test]
fn test_certificates() {
    let validator1_key_pair = ValidatorKeypair::generate();
    let account1_secret = AccountSecretKey::Ed25519(Ed25519SecretKey::generate());
    let validator2_key_pair = ValidatorKeypair::generate();
    let account2_secret = AccountSecretKey::Secp256k1(Secp256k1SecretKey::generate());
    let validator3_key_pair = ValidatorKeypair::generate();

    let committee = Committee::make_simple(vec![
        (validator1_key_pair.public_key, account1_secret.public()),
        (validator2_key_pair.public_key, account2_secret.public()),
    ]);

    let block = BlockExecutionOutcome {
        messages: vec![Vec::new()],
        previous_message_blocks: BTreeMap::new(),
        previous_event_blocks: BTreeMap::new(),
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
        blobs: vec![Vec::new()],
        operation_results: vec![OperationResult::default()],
    }
    .with(make_first_block(dummy_chain_id(1)).with_simple_transfer(dummy_chain_id(1), Amount::ONE));
    let value = ConfirmedBlock::new(block);

    let v1 = LiteVote::new(
        LiteValue::new(&value),
        Round::Fast,
        &validator1_key_pair.secret_key,
    );
    let v2 = LiteVote::new(
        LiteValue::new(&value),
        Round::Fast,
        &validator2_key_pair.secret_key,
    );
    let v3 = LiteVote::new(
        LiteValue::new(&value),
        Round::Fast,
        &validator3_key_pair.secret_key,
    );

    let mut builder = SignatureAggregator::new(value.clone(), Round::Fast, None, false, &committee);
    assert!(builder
        .append(validator1_key_pair.public_key, v1.signature)
        .unwrap()
        .is_none());
    let mut c = builder
        .append(validator2_key_pair.public_key, v2.signature)
        .unwrap()
        .unwrap();
    assert!(c.check(&committee).is_ok());
    c.signatures_mut().pop();
    assert!(c.check(&committee).is_err());

    let mut builder = SignatureAggregator::new(value, Round::Fast, None, false, &committee);
    assert!(builder
        .append(validator1_key_pair.public_key, v1.signature)
        .unwrap()
        .is_none());
    assert!(builder
        .append(validator3_key_pair.public_key, v3.signature)
        .is_err());
}

#[test]
fn round_ordering() {
    assert!(Round::Fast < Round::MultiLeader(0));
    assert!(Round::MultiLeader(1) < Round::MultiLeader(2));
    assert!(Round::MultiLeader(2) < Round::SingleLeader(0));
    assert!(Round::SingleLeader(1) < Round::SingleLeader(2));
    assert!(Round::SingleLeader(2) < Round::Validator(0));
    assert!(Round::Validator(1) < Round::Validator(2))
}
