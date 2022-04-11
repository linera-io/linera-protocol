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

    let request = Request {
        account_id: dbg_account(1),
        operation: Operation::Transfer {
            recipient: Address::Account(dbg_account(2)),
            amount: Amount::from(1),
            user_data: UserData::default(),
        },
        sequence_number: SequenceNumber::new(),
        round: RoundNumber::default(),
    };
    let value = Value::Confirmed { request };

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

    let mut authorities = BTreeMap::new();
    authorities.insert(name1, /* voting right */ 1);
    authorities.insert(name2, /* voting right */ 1);
    let committee = Committee::new(authorities);

    let request = Request {
        account_id: dbg_account(1),
        operation: Operation::Transfer {
            recipient: Address::Account(dbg_account(1)),
            amount: Amount::from(1),
            user_data: UserData::default(),
        },
        sequence_number: SequenceNumber::new(),
        round: RoundNumber::default(),
    };
    let value = Value::Confirmed { request };

    let v1 = Vote::new(value.clone(), &key1);
    let v2 = Vote::new(value.clone(), &key2);
    let v3 = Vote::new(value.clone(), &key3);

    let mut builder = SignatureAggregator::new(value.clone(), &committee);
    assert!(builder
        .append(v1.authority, v1.signature)
        .unwrap()
        .is_none());
    let mut c = builder.append(v2.authority, v2.signature).unwrap().unwrap();
    assert!(c.check(&committee).is_ok());
    c.signatures.pop();
    assert!(c.check(&committee).is_err());

    let mut builder = SignatureAggregator::new(value, &committee);
    assert!(builder
        .append(v1.authority, v1.signature)
        .unwrap()
        .is_none());
    assert!(builder.append(v3.authority, v3.signature).is_err());
}
