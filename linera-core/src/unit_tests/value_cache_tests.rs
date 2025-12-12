// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Epoch},
    hashed::Hashed,
    identifiers::ChainId,
    value_cache::ValueCache,
};
use linera_chain::types::Timeout;

/// Test cache size for unit tests.
const TEST_CACHE_SIZE: usize = 10;

/// Tests attempt to retrieve non-existent value.
#[test]
fn test_retrieve_missing_value() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let hash = CryptoHash::test_hash("Missing value");

    assert!(cache.get(&hash).is_none());
    assert_eq!(cache.len(), 0);
}

/// Tests inserting a certificate value in the cache.
#[test]
fn test_insert_single_certificate_value() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let value = create_dummy_certificate_value(0);
    let hash = value.hash();

    assert!(cache.insert_hashed(Cow::Borrowed(&value)));
    assert!(cache.contains(&hash));
    assert_eq!(cache.get(&hash), Some(value));
    assert_eq!(cache.len(), 1);
}

/// Tests inserting many certificate values in the cache, one-by-one.
#[test]
fn test_insert_many_certificate_values_individually() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    for value in &values {
        assert!(cache.insert_hashed(Cow::Borrowed(value)));
    }

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(cache.len(), TEST_CACHE_SIZE);
}

/// Tests inserting many values in the cache, all-at-once.
#[test]
fn test_insert_many_values_together() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    assert_eq!(cache.len(), TEST_CACHE_SIZE);
}

/// Tests re-inserting many values in the cache, all-at-once.
#[test]
fn test_reinsertion_of_values() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    for value in &values {
        assert!(!cache.insert_hashed(Cow::Borrowed(value)));
    }

    for value in &values {
        assert!(cache.contains(&value.hash()));
        assert_eq!(cache.get(&value.hash()).as_ref(), Some(value));
    }

    // Re-insertion should not increase the count
    assert_eq!(cache.len(), TEST_CACHE_SIZE);
}

/// Tests eviction when cache is full.
/// Note: quick_cache uses S3-FIFO eviction, so we verify that the cache
/// doesn't grow unboundedly and eviction occurs.
#[test]
fn test_eviction_occurs() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    // Insert more than capacity
    let values =
        create_dummy_certificate_values(0..((TEST_CACHE_SIZE as u64) * 2)).collect::<Vec<_>>();

    for value in &values {
        cache.insert_hashed(Cow::Borrowed(value));
    }

    // Cache size should be bounded by capacity
    assert!(
        cache.len() <= TEST_CACHE_SIZE,
        "Cache size {} exceeds capacity {}",
        cache.len(),
        TEST_CACHE_SIZE
    );

    // Count how many values are still in cache
    let present_count = values.iter().filter(|v| cache.contains(&v.hash())).count();

    // At least some values should have been evicted
    assert!(
        present_count < values.len(),
        "Cache should have evicted at least some entries"
    );
}

/// Tests eviction when inserting one more than capacity.
/// With S3-FIFO, the first inserted item may or may not be evicted (depends on access patterns).
#[test]
fn test_one_over_capacity() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..=(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed));

    // Cache should not exceed capacity
    assert!(
        cache.len() <= TEST_CACHE_SIZE,
        "Cache size {} exceeds capacity {}",
        cache.len(),
        TEST_CACHE_SIZE
    );

    // Exactly one value should have been evicted
    let present_count = values.iter().filter(|v| cache.contains(&v.hash())).count();
    assert_eq!(
        present_count, TEST_CACHE_SIZE,
        "Expected {} items in cache after inserting {} items with capacity {}",
        TEST_CACHE_SIZE,
        values.len(),
        TEST_CACHE_SIZE
    );
}

/// Tests that accessing a value affects eviction (values accessed recently are more likely to stay).
/// S3-FIFO uses frequency-based eviction, so frequently accessed items should survive.
#[test]
fn test_access_affects_eviction() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    // Fill the cache
    cache.insert_all(values.iter().map(Cow::Borrowed));

    // Access the first value multiple times to make it "hot"
    for _ in 0..5 {
        cache.get(&values[0].hash());
    }

    // Insert additional values to trigger eviction
    let extra_values =
        create_dummy_certificate_values((TEST_CACHE_SIZE as u64)..((TEST_CACHE_SIZE as u64) + 5))
            .collect::<Vec<_>>();

    for value in &extra_values {
        cache.insert_hashed(Cow::Borrowed(value));
    }

    // The frequently accessed first value should still be present
    assert!(
        cache.contains(&values[0].hash()),
        "Frequently accessed value should survive eviction"
    );
}

/// Tests that re-inserting a value promotes it in the eviction order.
#[test]
fn test_promotion_of_reinsertion() {
    let cache = ValueCache::<CryptoHash, Hashed<Timeout>>::new(TEST_CACHE_SIZE);
    let values = create_dummy_certificate_values(0..(TEST_CACHE_SIZE as u64)).collect::<Vec<_>>();

    // Fill the cache
    cache.insert_all(values.iter().map(Cow::Borrowed));

    // Re-insert the first value (this should "promote" it)
    assert!(!cache.insert_hashed(Cow::Borrowed(&values[0])));

    // Insert additional values to trigger eviction
    let extra_values =
        create_dummy_certificate_values((TEST_CACHE_SIZE as u64)..((TEST_CACHE_SIZE as u64) + 3))
            .collect::<Vec<_>>();

    for value in &extra_values {
        cache.insert_hashed(Cow::Borrowed(value));
    }

    // The re-inserted first value should still be present
    assert!(
        cache.contains(&values[0].hash()),
        "Re-inserted value should survive eviction"
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

/// Creates a new dummy [`Hashed<Timeout>`] to use in the tests.
fn create_dummy_certificate_value(height: impl Into<BlockHeight>) -> Hashed<Timeout> {
    Hashed::new(Timeout::new(
        ChainId(CryptoHash::test_hash("Fake chain ID")),
        height.into(),
        Epoch(0),
    ))
}
