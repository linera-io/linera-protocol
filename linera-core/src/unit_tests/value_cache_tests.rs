// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeSet};

use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::{CertificateValue, HashedCertificateValue};
use linera_execution::committee::Epoch;

use super::{CertificateValueCache, DEFAULT_VALUE_CACHE_SIZE};

/// Tests attempt to retrieve non-existent value.
#[tokio::test]
async fn test_retrieve_missing_value() {
    let cache = CertificateValueCache::default();
    let hash = CryptoHash::test_hash("Missing value");

    assert!(cache.get(&hash).await.is_none());
    assert!(cache.keys::<Vec<_>>().await.is_empty());
}

/// Tests inserting a value in the cache.
#[tokio::test]
async fn test_insert_single_value() {
    let cache = CertificateValueCache::default();
    let value = create_dummy_value(0);
    let hash = value.hash();

    assert!(cache.insert(Cow::Borrowed(&value)).await);
    assert!(cache.contains(&hash).await);
    assert_eq!(cache.get(&hash).await, Some(value));
    assert_eq!(cache.keys::<BTreeSet<_>>().await, BTreeSet::from([hash]));
}

/// Tests inserting many values in the cache, one-by-one.
#[tokio::test]
async fn test_insert_many_values_individually() {
    let cache = CertificateValueCache::default();
    let values = create_dummy_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    for value in &values {
        assert!(cache.insert(Cow::Borrowed(value)).await);
    }

    for value in &values {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(values.iter().map(HashedCertificateValue::hash))
    );
}

/// Creates multiple dummy [`HashedCertificateValue`]s to use in the tests.
fn create_dummy_values<Heights>(heights: Heights) -> impl Iterator<Item = HashedCertificateValue>
where
    Heights: IntoIterator,
    Heights::Item: Into<BlockHeight>,
{
    heights.into_iter().map(create_dummy_value)
}

/// Creates a new dummy [`HashedCertificateValue`] to use in the tests.
fn create_dummy_value(height: impl Into<BlockHeight>) -> HashedCertificateValue {
    CertificateValue::Timeout {
        chain_id: ChainId(CryptoHash::test_hash("Fake chain ID")),
        height: height.into(),
        epoch: Epoch(0),
    }
    .into()
}
