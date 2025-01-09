// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for guest Wasm module memory manipulation.

use super::GuestPointer;

/// Test aligning memory addresses.
///
/// Checks that the resulting address is aligned and that it never advances more than the alignment
/// amount.
#[test]
fn align_guest_pointer() {
    for alignment_bits in 0..3 {
        let alignment = 1 << alignment_bits;
        let alignment_mask = alignment - 1;

        for start_offset in 0..32 {
            let address = GuestPointer(start_offset).aligned_at(alignment);

            assert_eq!(address.0 & alignment_mask, 0);
            assert!(address.0 - start_offset < alignment);
        }
    }
}
