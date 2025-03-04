// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{Ed25519SecretKey, Secp256k1SecretKey, ValidatorKeypair},
    data_types::Amount,
};

use super::*;
use crate::{
    block::{ConfirmedBlock, ValidatedBlock},
    test::{make_first_block, BlockTestExt},
};

#[test]
fn test_signed_values() {
    let validator1_key_pair = ValidatorKeypair::generate();
    let validator2_key_pair = ValidatorKeypair::generate();

    let block =
        make_first_block(ChainId::root(1)).with_simple_transfer(ChainId::root(2), Amount::ONE);
    let executed_block = BlockExecutionOutcome {
        messages: vec![Vec::new()],
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
    }
    .with(block);
    let confirmed_value = Hashed::new(ConfirmedBlock::new(executed_block.clone()));

    let confirmed_vote = LiteVote::new(
        LiteValue::new(&confirmed_value),
        Round::Fast,
        &validator1_key_pair.secret_key,
    );
    assert!(confirmed_vote.check().is_ok());

    let validated_value = Hashed::new(ValidatedBlock::new(executed_block));
    let validated_vote = LiteVote::new(
        LiteValue::new(&validated_value),
        Round::Fast,
        &validator1_key_pair.secret_key,
    );
    assert_ne!(
        confirmed_vote.value, validated_vote.value,
        "Confirmed and validated votes should be different, even if for the same executed block"
    );

    let mut v = LiteVote::new(
        LiteValue::new(&confirmed_value),
        Round::Fast,
        &validator2_key_pair.secret_key,
    );
    v.public_key = validator1_key_pair.public_key;
    assert!(v.check().is_err());

    assert!(validated_vote.check().is_ok());
    assert!(confirmed_vote.check().is_ok());

    let mut v = validated_vote.clone();
    // Use signature from ConfirmedBlock to sign a ValidatedBlock.
    v.signature = confirmed_vote.signature;
    assert!(
        v.check().is_err(),
        "Confirmed and validated votes must not be interchangeable"
    );

    let mut v = confirmed_vote.clone();
    v.signature = validated_vote.signature;
    assert!(
        v.check().is_err(),
        "Confirmed and validated votes must not be interchangeable"
    );
}

#[test]
fn test_hashes() {
    // Test that hash of confirmed and validated blocks are different,
    // even if the blocks are the same.
    let block =
        make_first_block(ChainId::root(1)).with_simple_transfer(ChainId::root(2), Amount::ONE);
    let executed_block = BlockExecutionOutcome {
        messages: vec![Vec::new()],
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
    }
    .with(block);
    let confirmed_hashed = Hashed::new(ConfirmedBlock::new(executed_block.clone()));
    let validated_hashed = Hashed::new(ValidatedBlock::new(executed_block));

    assert_eq!(confirmed_hashed.hash(), validated_hashed.hash());
}

#[test]
fn test_certificates() {
    let validator1_key_pair = ValidatorKeypair::generate();
    let account1_secret: AccountSecretKey = Ed25519SecretKey::generate().into();
    let validator2_key_pair = ValidatorKeypair::generate();
    let account2_secret: AccountSecretKey = Secp256k1SecretKey::generate().into();
    let validator3_key_pair = ValidatorKeypair::generate();

    let committee = Committee::make_simple(vec![
        (validator1_key_pair.public_key, account1_secret.public()),
        (validator2_key_pair.public_key, account2_secret.public()),
    ]);

    let block =
        make_first_block(ChainId::root(1)).with_simple_transfer(ChainId::root(1), Amount::ONE);
    let executed_block = BlockExecutionOutcome {
        messages: vec![Vec::new()],
        state_hash: CryptoHash::test_hash("state"),
        oracle_responses: vec![Vec::new()],
        events: vec![Vec::new()],
    }
    .with(block);
    let value = Hashed::new(ConfirmedBlock::new(executed_block));

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

    let mut builder = SignatureAggregator::new(value.clone(), Round::Fast, &committee);
    assert!(builder
        .append(v1.public_key, v1.signature)
        .unwrap()
        .is_none());
    let mut c = builder
        .append(v2.public_key, v2.signature)
        .unwrap()
        .unwrap();
    assert!(c.check(&committee).is_ok());
    c.signatures_mut().pop();
    assert!(c.check(&committee).is_err());

    let mut builder = SignatureAggregator::new(value, Round::Fast, &committee);
    assert!(builder
        .append(v1.public_key, v1.signature)
        .unwrap()
        .is_none());
    assert!(builder.append(v3.public_key, v3.signature).is_err());
}
