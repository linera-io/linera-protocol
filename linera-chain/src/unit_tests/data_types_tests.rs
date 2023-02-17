// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use linera_execution::{
    system::{Account, Amount, Recipient, SystemOperation, UserData},
    ApplicationId,
};

#[test]
fn test_signed_values() {
    let key1 = KeyPair::generate();
    let key2 = KeyPair::generate();
    let name1 = ValidatorName(key1.public());

    let block = Block {
        epoch: Epoch::from(0),
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![(
            ApplicationId::System,
            Operation::System(SystemOperation::Transfer {
                recipient: Recipient::Account(Account::chain(ChainId::root(2))),
                amount: Amount::from(1),
                user_data: UserData::default(),
            }),
        )],
        height: BlockHeight::from(0),
        timestamp: Default::default(),
        previous_block_hash: None,
    };
    let value = Value::ConfirmedBlock {
        block,
        effects: Vec::new(),
        state_hash: CryptoHash::new(&Dummy),
    };

    let v = LiteVote::new(value.lite(), &key1);
    assert!(v.check().is_ok());

    let mut v = LiteVote::new(value.lite(), &key2);
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
        operations: vec![(
            ApplicationId::System,
            Operation::System(SystemOperation::Transfer {
                recipient: Recipient::Account(Account::chain(ChainId::root(1))),
                amount: Amount::from(1),
                user_data: UserData::default(),
            }),
        )],
        previous_block_hash: None,
        height: BlockHeight::from(0),
        timestamp: Default::default(),
    };
    let value = Value::ConfirmedBlock {
        block,
        effects: Vec::new(),
        state_hash: CryptoHash::new(&Dummy),
    };

    let v1 = LiteVote::new(value.lite(), &key1);
    let v2 = LiteVote::new(value.lite(), &key2);
    let v3 = LiteVote::new(value.lite(), &key3);

    let mut builder = SignatureAggregator::new(value.clone(), &committee);
    assert!(builder
        .append(v1.validator, v1.signature)
        .unwrap()
        .is_none());
    let mut c = builder.append(v2.validator, v2.signature).unwrap().unwrap();
    assert!(c.check(&committee).is_ok());
    c.signatures.pop();
    assert!(c.check(&committee).is_err());

    let mut builder = SignatureAggregator::new(value, &committee);
    assert!(builder
        .append(v1.validator, v1.signature)
        .unwrap()
        .is_none());
    assert!(builder.append(v3.validator, v3.signature).is_err());
}

#[derive(Serialize, Deserialize)]
struct Dummy;

impl BcsSignable for Dummy {}
