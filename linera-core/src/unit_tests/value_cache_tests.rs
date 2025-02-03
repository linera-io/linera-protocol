// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeSet};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    hashed::Hashed,
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::Timeout;
use linera_execution::committee::Epoch;

use super::{ValueCache, DEFAULT_VALUE_CACHE_SIZE};

/// Tests attempt to retrieve non-existent value.
#[test]
fn test_retrieve_missing_value() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let hash = CryptoHash::test_hash("Missing value");

    assert!(cache.get(&hash).is_none());
    assert!(cache.keys::<Vec<_>>().is_empty());
}

/// Tests inserting a certificate value in the cache.
#[test]
fn test_insert_single_certificate_value() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let value = create_dummy_certificate_value(0);
    let hash = value.hash();

    assert!(cache.insert(Cow::Borrowed(&value)));
    assert!(cache.contains(&hash));
    assert_eq!(cache.get(&hash), Some(value));
    assert_eq!(cache.keys::<BTreeSet<_>>(), BTreeSet::from([hash]));
}

/// Tests inserting a blob in the cache.
#[test]
fn test_insert_single_blob() {
    let cache = ValueCache::<BlobId, Blob>::default();
    let value = create_dummy_blob(0);
    let blob_id = value.id();

    assert!(cache.insert(Cow::Borrowed(&value)));
    assert!(cache.contains(&blob_id));
    assert_eq!(cache.get(&blob_id), Some(value));
    assert_eq!(cache.keys::<BTreeSet<_>>(), BTreeSet::from([blob_id]));
}

/// Tests inserting many certificate values in the cache, one-by-one.
#[test]
fn test_insert_many_certificate_values_individually() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    for value in &values {
        assert!(cache.insert(Cow::Borrowed(value)));
    }

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(values.iter().map(Hashed::hash))
    );
}

/// Tests inserting many blobs in the cache, one-by-one.
#[test]
fn test_insert_many_blobs_individually() {
    let cache = ValueCache::<BlobId, Blob>::default();
    let blobs = create_dummy_blobs();

    for blob in &blobs {
        assert!(cache.insert(Cow::Borrowed(blob)));
    }

    for blob in &blobs {
        assert!(cache.contains(&blob.id()));
        assert_eq!(cache.get(&blob.id()).as_ref(), Some(blob));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(blobs.iter().map(Blob::id))
    );
}

/// Tests inserting many values in the cache, all-at-once.
#[test]
fn test_insert_many_values_together() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(values.iter().map(|el| el.hash()))
    );
}

/// Tests re-inserting many values in the cache, all-at-once.
#[test]
fn test_reinsertion_of_values() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    for value in &values {
        assert!(!cache.insert(Cow::Borrowed(value)));
    }

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(values.iter().map(Hashed::hash))
    );
}

/// Tests eviction of one entry.
#[test]
fn test_one_eviction() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    assert!(!cache.contains(&values[0].hash()));
    assert!(cache.get(&values[0].hash()).is_none());

    for value in values.iter().skip(1) {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(values.iter().skip(1).map(Hashed::hash))
    );
}

/// Tests eviction of the second entry.
#[test]
fn test_eviction_of_second_entry() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(
        values
            .iter()
            .take(DEFAULT_VALUE_CACHE_SIZE)
            .map(Cow::Borrowed),
    );
    cache.get(&values[0].hash());
    assert!(cache.insert(Cow::Borrowed(&values[DEFAULT_VALUE_CACHE_SIZE])));

    assert!(cache.contains(&values[0].hash()));
    assert_eq!(cache.get(&values[0].hash()).as_ref(), Some(&values[0]));

    assert!(!cache.contains(&values[1].hash()));
    assert!(cache.get(&values[1].hash()).is_none());

    for value in values.iter().skip(2) {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(
            values
                .iter()
                .skip(2)
                .map(Hashed::hash)
                .chain(Some(values[0].hash()))
        )
    );
}

/// Tests if reinsertion of the first entry promotes it so that it's not evicted so soon.
#[test]
fn test_promotion_of_reinsertion() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(
        values
            .iter()
            .take(DEFAULT_VALUE_CACHE_SIZE)
            .map(Cow::Borrowed),
    );
    assert!(!cache.insert(Cow::Borrowed(&values[0])));
    assert!(cache.insert(Cow::Borrowed(&values[DEFAULT_VALUE_CACHE_SIZE])));

    assert!(cache.contains(&values[0].hash()));
    assert_eq!(cache.get(&values[0].hash()).as_ref(), Some(&values[0]));

    assert!(!cache.contains(&values[1].hash()));
    assert!(cache.get(&values[1].hash()).is_none());

    for value in values.iter().skip(2) {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(
            values
                .iter()
                .skip(2)
                .map(Hashed::hash)
                .chain(Some(values[0].hash()))
        )
    );
}

/// Test that the cache correctly filters out cached items from an iterator.
#[test]
fn test_filtering_out_cached_items() {
    #[derive(Debug, Eq, PartialEq)]
    struct DummyWrapper(CryptoHash);

    let cached_values = create_dummy_certificate_values(3..7).collect::<Vec<_>>();
    let items = create_dummy_certificate_values(0..10).map(|value| DummyWrapper(value.hash()));

    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::default();
    cache.insert_all(cached_values.iter().map(Cow::Borrowed));

    let output = cache.subtract_cached_items_from::<_, Vec<_>>(items, |item| &item.0);

    let expected = create_dummy_certificate_values(0..3)
        .chain(create_dummy_certificate_values(7..10))
        .map(|value| DummyWrapper(value.hash()))
        .collect::<Vec<_>>();

    assert_eq!(output, expected);

    assert_eq!(
        cache.keys::<BTreeSet<_>>(),
        BTreeSet::from_iter(cached_values.iter().map(|el| el.hash()))
    );
}

/// Creates multiple dummy [`Hashed<Timeout>`]s to use in the tests.
fn create_dummy_certificate_values<Heights>(
    heights: Heights,
) -> impl Iterator<Item = Hashed<Timeout>>
where
    Heights: IntoIterator,
    Heights::Item: Into<BlockHeight>,
{
    heights.into_iter().map(create_dummy_certificate_value)
}

/// Creates multiple dummy [`Blob`]s to use in the tests.
fn create_dummy_blobs() -> Vec<Blob> {
    let mut blobs = Vec::new();
    for i in 0..DEFAULT_VALUE_CACHE_SIZE {
        blobs.push(create_dummy_blob(i));
    }
    blobs
}

/// Creates a new dummy [`Hashed<Timeout>`] to use in the tests.
fn create_dummy_certificate_value(height: impl Into<BlockHeight>) -> Hashed<Timeout> {
    Hashed::new(Timeout::new(
        ChainId(CryptoHash::test_hash("Fake chain ID")),
        height.into(),
        Epoch(0),
    ))
}

/// Creates a new dummy data [`Blob`] to use in the tests.
fn create_dummy_blob(id: usize) -> Blob {
    Blob::new_data(format!("test{}", id).as_bytes().to_vec())
}
