// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeSet};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{
    Block, BlockExecutionOutcome, CertificateValue, ExecutedBlock, HashedCertificateValue,
};
use linera_execution::committee::Epoch;

use super::{ValueCache, DEFAULT_VALUE_CACHE_SIZE};

/// Tests attempt to retrieve non-existent value.
#[tokio::test]
async fn test_retrieve_missing_value() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let hash = CryptoHash::test_hash("Missing value");

    assert!(cache.get(&hash).await.is_none());
    assert!(cache.keys::<Vec<_>>().await.is_empty());
}

/// Tests inserting a certificate value in the cache.
#[tokio::test]
async fn test_insert_single_certificate_value() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let value = create_dummy_certificate_value(0);
    let hash = value.hash();

    assert!(cache.insert(Cow::Borrowed(&value)).await);
    assert!(cache.contains(&hash).await);
    assert_eq!(cache.get(&hash).await, Some(value));
    assert_eq!(cache.keys::<BTreeSet<_>>().await, BTreeSet::from([hash]));
}

/// Tests inserting a blob in the cache.
#[tokio::test]
async fn test_insert_single_blob() {
    let cache = ValueCache::<BlobId, Blob>::default();
    let value = create_dummy_blob(0);
    let blob_id = value.id();

    assert!(cache.insert(Cow::Borrowed(&value)).await);
    assert!(cache.contains(&blob_id).await);
    assert_eq!(cache.get(&blob_id).await, Some(value));
    assert_eq!(cache.keys::<BTreeSet<_>>().await, BTreeSet::from([blob_id]));
}

/// Tests inserting many certificate values in the cache, one-by-one.
#[tokio::test]
async fn test_insert_many_certificate_values_individually() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

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

/// Tests inserting many blobs in the cache, one-by-one.
#[tokio::test]
async fn test_insert_many_blobs_individually() {
    let cache = ValueCache::<BlobId, Blob>::default();
    let blobs = create_dummy_blobs();

    for blob in &blobs {
        assert!(cache.insert(Cow::Borrowed(blob)).await);
    }

    for blob in &blobs {
        assert!(cache.contains(&blob.id()).await);
        assert_eq!(cache.get(&blob.id()).await.as_ref(), Some(blob));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(blobs.iter().map(Blob::id))
    );
}

/// Tests inserting many values in the cache, all-at-once.
#[tokio::test]
async fn test_insert_many_values_together() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed)).await;

    for value in &values {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(values.iter().map(HashedCertificateValue::hash))
    );
}

/// Tests re-inserting many values in the cache, all-at-once.
#[tokio::test]
async fn test_reinsertion_of_values() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed)).await;

    for value in &values {
        assert!(!cache.insert(Cow::Borrowed(value)).await);
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

/// Tests eviction of one entry.
#[tokio::test]
async fn test_one_eviction() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache.insert_all(values.iter().map(Cow::Borrowed)).await;

    assert!(!cache.contains(&values[0].hash()).await);
    assert!(cache.get(&values[0].hash()).await.is_none());

    for value in values.iter().skip(1) {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(values.iter().skip(1).map(HashedCertificateValue::hash))
    );
}

/// Tests eviction of the second entry.
#[tokio::test]
async fn test_eviction_of_second_entry() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache
        .insert_all(
            values
                .iter()
                .take(DEFAULT_VALUE_CACHE_SIZE)
                .map(Cow::Borrowed),
        )
        .await;
    cache.get(&values[0].hash()).await;
    assert!(
        cache
            .insert(Cow::Borrowed(&values[DEFAULT_VALUE_CACHE_SIZE]))
            .await
    );

    assert!(cache.contains(&values[0].hash()).await);
    assert_eq!(
        cache.get(&values[0].hash()).await.as_ref(),
        Some(&values[0])
    );

    assert!(!cache.contains(&values[1].hash()).await);
    assert!(cache.get(&values[1].hash()).await.is_none());

    for value in values.iter().skip(2) {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(
            values
                .iter()
                .skip(2)
                .map(HashedCertificateValue::hash)
                .chain(Some(values[0].hash()))
        )
    );
}

/// Test that insertion of a validated block certificate also inserts its respective confirmed
/// block certificate.
#[tokio::test]
async fn test_insertion_of_validated_also_inserts_confirmed() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();

    let validated_value = create_dummy_validated_block_value();
    let validated_hash = validated_value.hash();

    let confirmed_value = validated_value
        .validated_to_confirmed()
        .expect("a validated value should be convertible to a confirmed value");
    let confirmed_hash = confirmed_value.hash();

    assert!(cache.insert(Cow::Borrowed(&validated_value)).await);

    assert!(cache.contains(&validated_hash).await);
    assert!(cache.contains(&confirmed_hash).await);
    assert_eq!(cache.get(&validated_hash).await, Some(validated_value));
    assert_eq!(cache.get(&confirmed_hash).await, Some(confirmed_value));
    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from([validated_hash, confirmed_hash])
    );
}

/// Test that an inserted validated block certificate value gets evicted before its respective
/// confirmed block certificate value that was inserted with it.
#[tokio::test]
async fn test_eviction_of_validated_before_respective_confirmed() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values = create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64 - 1))
        .collect::<Vec<_>>();

    let validated_value = create_dummy_validated_block_value();
    let validated_hash = validated_value.hash();

    let confirmed_value = validated_value
        .validated_to_confirmed()
        .expect("a validated value should be convertible to a confirmed value");
    let confirmed_hash = confirmed_value.hash();

    assert!(cache.insert(Cow::Borrowed(&validated_value)).await);
    cache.insert_all(values.iter().map(Cow::Borrowed)).await;

    assert!(!cache.contains(&validated_hash).await);
    assert!(cache.get(&validated_hash).await.is_none());

    assert!(cache.contains(&confirmed_hash).await);
    assert_eq!(cache.get(&confirmed_hash).await, Some(confirmed_value));
    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(
            values
                .iter()
                .map(HashedCertificateValue::hash)
                .chain([confirmed_hash])
        )
    );
}

/// Tests if reinsertion of the first entry promotes it so that it's not evicted so soon.
#[tokio::test]
async fn test_promotion_of_reinsertion() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let values =
        create_dummy_certificate_values(0..=(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();

    cache
        .insert_all(
            values
                .iter()
                .take(DEFAULT_VALUE_CACHE_SIZE)
                .map(Cow::Borrowed),
        )
        .await;
    assert!(!cache.insert(Cow::Borrowed(&values[0])).await);
    assert!(
        cache
            .insert(Cow::Borrowed(&values[DEFAULT_VALUE_CACHE_SIZE]))
            .await
    );

    assert!(cache.contains(&values[0].hash()).await);
    assert_eq!(
        cache.get(&values[0].hash()).await.as_ref(),
        Some(&values[0])
    );

    assert!(!cache.contains(&values[1].hash()).await);
    assert!(cache.get(&values[1].hash()).await.is_none());

    for value in values.iter().skip(2) {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(
            values
                .iter()
                .skip(2)
                .map(HashedCertificateValue::hash)
                .chain(Some(values[0].hash()))
        )
    );
}

/// Tests if reinsertion of a validated block certificate value promotes it and its respective
/// confirmed block certificate value so that it's not evicted so soon.
#[tokio::test]
async fn test_promotion_of_reinsertion_of_validated_block() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let dummy_values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();
    let validated_value = create_dummy_validated_block_value();
    let confirmed_value = validated_value
        .validated_to_confirmed()
        .expect("Dummy validated value should be able to create a confirmed value");

    assert!(cache.insert(Cow::Borrowed(&validated_value)).await);
    cache
        .insert_all(
            dummy_values
                .iter()
                .take(DEFAULT_VALUE_CACHE_SIZE - 2)
                .map(Cow::Borrowed),
        )
        .await;
    assert!(!cache.insert(Cow::Borrowed(&validated_value)).await);
    cache
        .insert_all(
            dummy_values
                .iter()
                .skip(DEFAULT_VALUE_CACHE_SIZE - 2)
                .map(Cow::Borrowed),
        )
        .await;

    for value in dummy_values.iter().take(2) {
        assert!(!cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), None);
    }

    let expected_values_in_cache = dummy_values
        .iter()
        .skip(2)
        .chain([&validated_value, &confirmed_value]);

    for value in expected_values_in_cache.clone() {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(expected_values_in_cache.map(HashedCertificateValue::hash))
    );
}

/// Tests if reinsertion of a confirmed block certificate value promotes it but not its respective
/// validated block certificate value.
#[tokio::test]
async fn test_promotion_of_reinsertion_of_confirmed_block() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    let dummy_values =
        create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64)).collect::<Vec<_>>();
    let validated_value = create_dummy_validated_block_value();
    let confirmed_value = validated_value
        .validated_to_confirmed()
        .expect("Dummy validated value should be able to create a confirmed value");

    assert!(cache.insert(Cow::Borrowed(&validated_value)).await);
    cache
        .insert_all(
            dummy_values
                .iter()
                .take(DEFAULT_VALUE_CACHE_SIZE - 2)
                .map(Cow::Borrowed),
        )
        .await;
    assert!(!cache.insert(Cow::Borrowed(&confirmed_value)).await);
    cache
        .insert_all(
            dummy_values
                .iter()
                .skip(DEFAULT_VALUE_CACHE_SIZE - 2)
                .map(Cow::Borrowed),
        )
        .await;

    for value in dummy_values.iter().take(1).chain([&validated_value]) {
        assert!(!cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), None);
    }

    let expected_values_in_cache = dummy_values.iter().skip(1).chain([&confirmed_value]);

    for value in expected_values_in_cache.clone() {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(expected_values_in_cache.map(HashedCertificateValue::hash))
    );
}

/// Test that a re-inserted validated block certificate value gets evicted before its respective
/// confirmed block certificate value that was inserted with it.
#[tokio::test]
async fn test_eviction_of_reinserted_validated_before_respective_confirmed() {
    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();

    let initial_values = create_dummy_certificate_values(0..(DEFAULT_VALUE_CACHE_SIZE as u64 - 2))
        .collect::<Vec<_>>();

    let final_values = create_dummy_certificate_values(
        (0..(DEFAULT_VALUE_CACHE_SIZE - 1)).map(|index| (index + DEFAULT_VALUE_CACHE_SIZE) as u64),
    )
    .collect::<Vec<_>>();

    let validated_value = create_dummy_validated_block_value();
    let confirmed_value = validated_value
        .validated_to_confirmed()
        .expect("a validated value should be convertible to a confirmed value");

    assert!(cache.insert(Cow::Borrowed(&validated_value)).await);
    cache
        .insert_all(initial_values.iter().map(Cow::Borrowed))
        .await;
    assert!(!cache.insert(Cow::Borrowed(&validated_value)).await);
    cache
        .insert_all(final_values.iter().map(Cow::Borrowed))
        .await;

    for value in initial_values.iter().chain([&validated_value]) {
        assert!(!cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), None);
    }

    let expected_values_in_cache = final_values.iter().chain([&confirmed_value]);

    for value in expected_values_in_cache.clone() {
        assert!(cache.contains(&value.hash()).await);
        assert_eq!(cache.get(&value.hash()).await.as_ref(), Some(value));
    }

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(expected_values_in_cache.map(HashedCertificateValue::hash))
    );
}

/// Test that the cache correctly filters out cached items from an iterator.
#[tokio::test]
async fn test_filtering_out_cached_items() {
    #[derive(Debug, Eq, PartialEq)]
    struct DummyWrapper(CryptoHash);

    let cached_values = create_dummy_certificate_values(3..7).collect::<Vec<_>>();
    let items = create_dummy_certificate_values(0..10).map(|value| DummyWrapper(value.hash()));

    let cache = ValueCache::<CryptoHash, HashedCertificateValue>::default();
    cache
        .insert_all(cached_values.iter().map(Cow::Borrowed))
        .await;

    let output = cache
        .subtract_cached_items_from::<_, Vec<_>>(items, |item| &item.0)
        .await;

    let expected = create_dummy_certificate_values(0..3)
        .chain(create_dummy_certificate_values(7..10))
        .map(|value| DummyWrapper(value.hash()))
        .collect::<Vec<_>>();

    assert_eq!(output, expected);

    assert_eq!(
        cache.keys::<BTreeSet<_>>().await,
        BTreeSet::from_iter(cached_values.iter().map(HashedCertificateValue::hash))
    );
}

/// Creates multiple dummy [`HashedCertificateValue`]s to use in the tests.
fn create_dummy_certificate_values<Heights>(
    heights: Heights,
) -> impl Iterator<Item = HashedCertificateValue>
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

/// Creates a new dummy [`HashedCertificateValue`] to use in the tests.
fn create_dummy_certificate_value(height: impl Into<BlockHeight>) -> HashedCertificateValue {
    CertificateValue::Timeout {
        chain_id: ChainId(CryptoHash::test_hash("Fake chain ID")),
        height: height.into(),
        epoch: Epoch(0),
    }
    .into()
}

/// Creates a new dummy data [`Blob`] to use in the tests.
fn create_dummy_blob(id: usize) -> Blob {
    Blob::new_data(format!("test{}", id).as_bytes().to_vec())
}

/// Creates a dummy [`HashedCertificateValue::ValidatedBlock`] to use in the tests.
fn create_dummy_validated_block_value() -> HashedCertificateValue {
    CertificateValue::ValidatedBlock {
        executed_block: ExecutedBlock {
            block: Block {
                chain_id: ChainId(CryptoHash::test_hash("Fake chain ID")),
                epoch: Epoch::ZERO,
                incoming_bundles: vec![],
                operations: vec![],
                height: BlockHeight::ZERO,
                timestamp: Timestamp::from(0),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            outcome: BlockExecutionOutcome::default(),
        },
    }
    .into()
}
