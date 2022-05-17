// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::BTreeMap;

#[test]
fn test_signed_values() {
    let key1 = KeyPair::generate();
    let key2 = KeyPair::generate();
    let name1 = key1.public();

    let block = Block {
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::Transfer {
            recipient: Address::Account(ChainId::root(2)),
            amount: Amount::from(1),
            user_data: UserData::default(),
        }],
        height: BlockHeight::from(0),
        previous_block_hash: None,
    };
    let value = Value::Confirmed { block };

    let v = Vote::new(value.clone(), &key1);
    assert!(v.check(name1).is_ok());

    let v = Vote::new(value, &key2);
    assert!(v.check(name1).is_err());
}

#[test]
fn test_certificates() {
    let key1 = KeyPair::generate();
    let key2 = KeyPair::generate();
    let key3 = KeyPair::generate();
    let name1 = key1.public();
    let name2 = key2.public();

    let mut validators = BTreeMap::new();
    validators.insert(name1, /* voting right */ 1);
    validators.insert(name2, /* voting right */ 1);
    let committee = Committee::new(validators);

    let block = Block {
        chain_id: ChainId::root(1),
        incoming_messages: Vec::new(),
        operations: vec![Operation::Transfer {
            recipient: Address::Account(ChainId::root(1)),
            amount: Amount::from(1),
            user_data: UserData::default(),
        }],
        previous_block_hash: None,
        height: BlockHeight::from(0),
    };
    let value = Value::Confirmed { block };

    let v1 = Vote::new(value.clone(), &key1);
    let v2 = Vote::new(value.clone(), &key2);
    let v3 = Vote::new(value.clone(), &key3);

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
