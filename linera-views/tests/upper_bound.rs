// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::common::get_upper_bound;

#[test]
fn test_upper_bound() {
    assert_eq!(get_upper_bound(&[255]), None);
    assert_eq!(get_upper_bound(&[255, 255, 255, 255]), None);
    assert_eq!(get_upper_bound(&[0, 2]), Some(vec![0, 3]));
    assert_eq!(get_upper_bound(&[0, 255]), Some(vec![1]));
    assert_eq!(get_upper_bound(&[255, 0]), Some(vec![255, 1]));
}
