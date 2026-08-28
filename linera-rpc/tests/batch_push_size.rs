// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The batch push carries only as many certificates as fit the wire.
//!
//! This is the sole guard left between block export and `GRPC_MAX_MESSAGE_SIZE`: the sender picks a
//! run length from how fast the destination has been answering and knows nothing about how large
//! these particular blocks are. A run that cannot be encoded fails identically on every retry, so
//! getting this wrong stalls a chain rather than slowing it.

#![cfg(not(target_arch = "wasm32"))]

use linera_base::{
    crypto::CryptoHash,
    data_types::Round,
    identifiers::{ApplicationId, ChainId},
};
use linera_cache::ValueCache;
use linera_chain::{
    data_types::BlockExecutionOutcome,
    test::{make_child_block, make_first_block, BlockTestExt as _},
    types::{ConfirmedBlock, ConfirmedBlockCertificate},
};
use linera_execution::Operation;
use linera_rpc::grpc::{batch_push_request, BATCH_PUSH_FILL_LIMIT, GRPC_MAX_MESSAGE_SIZE};
use prost::Message as _;

/// A run of certificates on one chain, each padded to roughly `payload` bytes.
///
/// `Arc` has no public constructor by design — the cache is the only way to mint one — so the
/// certificates are handed out the way storage would hand them out.
fn run_of(count: usize, payload: usize) -> Vec<linera_cache::Arc<ConfirmedBlockCertificate>> {
    let cache = ValueCache::new("test", 1024, 60);
    let chain_id = ChainId(CryptoHash::test_hash("batch push size"));
    let mut certificates = Vec::with_capacity(count);
    let mut previous: Option<ConfirmedBlock> = None;
    for index in 0..count {
        let proposed = match &previous {
            None => make_first_block(chain_id),
            Some(block) => make_child_block(block),
        }
        .with_operation(
            Operation::user_without_abi(
                ApplicationId::new(CryptoHash::test_hash("padding app")),
                &vec![0u8; payload],
            )
            .unwrap(),
        );
        let block = ConfirmedBlock::new(BlockExecutionOutcome::default().with(proposed));
        previous = Some(block.clone());
        let certificate = ConfirmedBlockCertificate::new(block, Round::Fast, vec![]);
        certificates
            .push(cache.insert(&CryptoHash::test_hash(format!("cert {index}")), certificate));
    }
    certificates
}

#[test]
fn a_run_is_truncated_to_the_fill_limit() {
    // Each certificate is a sizeable fraction of the budget, so a run of 20 cannot fit and the
    // request has to carry fewer than it was offered.
    let payload = BATCH_PUSH_FILL_LIMIT / 4;
    let certificates = run_of(20, payload);
    let request = batch_push_request(&certificates, false).expect("the run is not empty");

    assert!(
        request.certificates.len() < certificates.len(),
        "a run that cannot fit must be truncated, not sent whole",
    );
    assert!(
        !request.certificates.is_empty(),
        "truncation must never empty the run, or the chain never advances",
    );
    assert!(
        request.encoded_len() < GRPC_MAX_MESSAGE_SIZE,
        "the encoded request must fit the gRPC message limit: {} bytes",
        request.encoded_len(),
    );
}

#[test]
fn one_certificate_larger_than_the_whole_budget_still_goes() {
    // A block larger than a whole push is exactly what every push carried before batching, so
    // refusing it would strand the chain rather than merely slow it.
    let certificates = run_of(3, BATCH_PUSH_FILL_LIMIT * 2);
    let request = batch_push_request(&certificates, false).expect("the run is not empty");

    assert_eq!(
        request.certificates.len(),
        1,
        "an oversized certificate travels on its own",
    );
}

#[test]
fn a_run_that_fits_is_sent_whole() {
    let certificates = run_of(20, 128);
    let request = batch_push_request(&certificates, false).expect("the run is not empty");

    assert_eq!(
        request.certificates.len(),
        certificates.len(),
        "small certificates must not be truncated",
    );
}
