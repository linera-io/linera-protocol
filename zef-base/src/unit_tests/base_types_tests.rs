// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::blacklisted_name)]

use super::*;

#[derive(Debug, Serialize, Deserialize)]
struct Foo(String);

impl BcsSignable for Foo {}

#[derive(Debug, Serialize, Deserialize)]
struct Bar(String);

impl BcsSignable for Bar {}

#[test]
fn test_signatures() {
    let key1 = KeyPair::generate();
    let addr1 = key1.public();
    let key2 = KeyPair::generate();
    let addr2 = key2.public();

    let foo = Foo("hello".into());
    let foox = Foo("hellox".into());
    let bar = Bar("hello".into());

    let s = Signature::new(&foo, &key1);
    assert!(s.check(&foo, addr1).is_ok());
    assert!(s.check(&foo, addr2).is_err());
    assert!(s.check(&foox, addr1).is_err());
    assert!(s.check(&bar, addr1).is_err());
}

#[test]
fn test_max_sequence_number() {
    let max = SequenceNumber::max();
    assert_eq!(max.0 * 2 + 1, std::u64::MAX);
}

fn chain(numbers: Vec<u64>) -> ChainId {
    ChainId::new(numbers.into_iter().map(SequenceNumber::from).collect())
}

#[test]
fn test_chain_ids() {
    let id = chain(vec![1]);
    assert_eq!(id.make_child(SequenceNumber::from(2)), chain(vec![1, 2]));

    let id = chain(vec![1]);
    assert_eq!(id.split(), None);

    let id = chain(vec![1, 2]);
    assert_eq!(id.split(), Some((chain(vec![1]), SequenceNumber::from(2))));

    let id = chain(vec![1, 2, 3]);
    assert_eq!(
        id.split(),
        Some((chain(vec![1, 2]), SequenceNumber::from(3)))
    );

    let id = chain(vec![1, 2, 3]);
    assert_eq!(
        id.ancestors(),
        vec![chain(vec![1]), chain(vec![1, 2]), chain(vec![1, 2, 3])]
    );
}
