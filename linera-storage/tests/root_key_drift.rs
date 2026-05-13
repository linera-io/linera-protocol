// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Drift-detection between the canonical `RootKey` in `linera-storage` and the mirrored copy in
//! `linera-storage-udf`. Both must BCS-encode identically; otherwise the deployed Wasm UDFs will
//! mis-decode partition keys written by validators.

use linera_base::{
    crypto::CryptoHash,
    identifiers::{BlobId, BlobType, ChainId},
};
use linera_storage::RootKey;
use linera_storage_udf::RootKey as UdfRootKey;

fn check<F>(canonical: RootKey, mirror: F)
where
    F: FnOnce() -> UdfRootKey,
{
    assert_eq!(
        canonical.bytes(),
        mirror().bytes(),
        "BCS encoding diverged for {canonical:?}"
    );
}

#[test]
fn every_variant_encodes_identically() {
    let chain_id = ChainId(CryptoHash::test_hash("drift"));
    let other_hash = CryptoHash::test_hash("drift-2");
    let blob_id = BlobId::new(other_hash, BlobType::Data);

    check(RootKey::NetworkDescription, || {
        UdfRootKey::NetworkDescription
    });
    check(RootKey::BlockExporterState(42), || {
        UdfRootKey::BlockExporterState(42)
    });
    check(RootKey::ChainState(chain_id), || {
        UdfRootKey::ChainState(chain_id)
    });
    check(RootKey::BlockHash(other_hash), || {
        UdfRootKey::BlockHash(other_hash)
    });
    check(RootKey::BlobId(blob_id), || UdfRootKey::BlobId(blob_id));
    check(RootKey::Event(chain_id), || UdfRootKey::Event(chain_id));
    check(RootKey::BlockByHeight(chain_id), || {
        UdfRootKey::BlockByHeight(chain_id)
    });
    check(RootKey::EventBlockHeight(chain_id), || {
        UdfRootKey::EventBlockHeight(chain_id)
    });
}
