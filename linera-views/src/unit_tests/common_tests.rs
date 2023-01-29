// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::get_upper_bound;
use std::ops::Bound::{Excluded, Unbounded};

#[test]
fn test_upper_bound() {
    assert_eq!(get_upper_bound(&[255]), Unbounded);
    assert_eq!(get_upper_bound(&[255, 255, 255, 255]), Unbounded);
    assert_eq!(get_upper_bound(&[0, 2]), Excluded(vec![0, 3]));
    assert_eq!(get_upper_bound(&[0, 255]), Excluded(vec![1]));
    assert_eq!(get_upper_bound(&[255, 0]), Excluded(vec![255, 1]));
}
