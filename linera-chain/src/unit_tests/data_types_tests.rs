// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_base::data_types::Amount;
use linera_execution::system::{Account, Recipient, SystemOperation, UserData};

#[test]
fn test_signed_values() {
    let key1 = KeyPair::generate();
    let key2 = KeyPair::generate();
    let name1 = ValidatorName(key1.public());

    let block = Block {
        epoch: Epoch::from(0),
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::System(SystemOperation::Transfer {
            owner: None,
            recipient: Recipient::Account(Account::chain(ChainId::root(2))),
            amount: Amount::ONE,
            user_data: UserData::default(),
        })],
        height: BlockHeight::from(0),
        timestamp: Default::default(),
        authenticated_signer: None,
        previous_block_hash: None,
    };
    let executed_block = ExecutedBlock {
        block,
        messages: Vec::new(),
        state_hash: CryptoHash::new(&Dummy),
    };
    let value = HashedValue::new_confirmed(executed_block);

    let v = LiteVote::new(value.lite(), RoundNumber(0), &key1);
    assert!(v.check().is_ok());

    let mut v = LiteVote::new(value.lite(), RoundNumber(0), &key2);
    v.validator = name1;
    assert!(v.check().is_err());
}

#[test]
fn test_certificates() {
    let key1 = KeyPair::generate();
    let key2 = KeyPair::generate();
    let key3 = KeyPair::generate();
    let name1 = ValidatorName(key1.public());
    let name2 = ValidatorName(key2.public());

    let committee = Committee::make_simple(vec![name1, name2]);

    let block = Block {
        epoch: Epoch::from(0),
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::System(SystemOperation::Transfer {
            owner: None,
            recipient: Recipient::Account(Account::chain(ChainId::root(1))),
            amount: Amount::ONE,
            user_data: UserData::default(),
        })],
        previous_block_hash: None,
        height: BlockHeight::from(0),
        authenticated_signer: None,
        timestamp: Default::default(),
    };
    let executed_block = ExecutedBlock {
        block,
        messages: Vec::new(),
        state_hash: CryptoHash::new(&Dummy),
    };
    let value = HashedValue::new_confirmed(executed_block);

    let v1 = LiteVote::new(value.lite(), RoundNumber(0), &key1);
    let v2 = LiteVote::new(value.lite(), RoundNumber(0), &key2);
    let v3 = LiteVote::new(value.lite(), RoundNumber(0), &key3);

    let mut builder = SignatureAggregator::new(value.clone(), RoundNumber(0), &committee);
    assert!(builder
        .append(v1.validator, v1.signature)
        .unwrap()
        .is_none());
    let mut c = builder.append(v2.validator, v2.signature).unwrap().unwrap();
    assert!(c.check(&committee).is_ok());
    c.signatures.pop();
    assert!(c.check(&committee).is_err());

    let mut builder = SignatureAggregator::new(value, RoundNumber(0), &committee);
    assert!(builder
        .append(v1.validator, v1.signature)
        .unwrap()
        .is_none());
    assert!(builder.append(v3.validator, v3.signature).is_err());
}

#[derive(Serialize, Deserialize)]
struct Dummy;

impl BcsSignable for Dummy {}
