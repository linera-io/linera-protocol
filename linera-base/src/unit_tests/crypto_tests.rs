// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::disallowed_names)]

use super::*;
use crate::messages::BlockHeight;

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
fn test_max_block_height() {
    let max = BlockHeight::max();
    assert_eq!(max.0 * 2 + 1, std::u64::MAX);
}
