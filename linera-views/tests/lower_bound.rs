// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::common::LowerBound;
use std::collections::BTreeSet;

#[test]
fn test_lower_bound() {
    let mut set = BTreeSet::<u8>::new();
    set.insert(4);
    set.insert(7);
    set.insert(8);
    set.insert(10);
    set.insert(24);
    set.insert(40);

    let mut lower_bound = LowerBound::new(&set);
    assert_eq!(lower_bound.get_lower_bound(3), None);
    assert_eq!(lower_bound.get_lower_bound(15), Some(10));
    assert_eq!(lower_bound.get_lower_bound(17), Some(10));
    assert_eq!(lower_bound.get_lower_bound(25), Some(24));
    assert_eq!(lower_bound.get_lower_bound(27), Some(24));
    assert_eq!(lower_bound.get_lower_bound(42), Some(40));
}
