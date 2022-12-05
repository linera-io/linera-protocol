// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::{Batch, KeyValueOperations},
    dynamo_db::DynamoDbContainer,
    rocksdb::DB,
    test_utils::{get_random_key_value_vec_prefix, LocalStackTestContext},
};
use rand::SeedableRng;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

#[cfg(test)]
async fn test_ordering_keys_key_value_vec<OP: KeyValueOperations + Sync>(
    key_value_operation: OP,
    key_value_vec: Vec<(Vec<u8>, Vec<u8>)>,
) {
    // We need a nontrivial key_prefix because dynamo requires a non-trivial prefix
    let key_prefix = vec![0];
    let mut batch = Batch::default();
    for key_value in key_value_vec {
        batch.put_key_value_bytes(key_value.0, key_value.1);
    }
    key_value_operation.write_batch(batch).await.unwrap();
    let l_keys: Vec<Vec<u8>> = key_value_operation
        .find_keys_with_prefix(&key_prefix)
        .await
        .unwrap()
        .map(|x| x.expect("Failed to get vector").to_vec())
        .collect();
    for i in 1..l_keys.len() {
        let key1 = l_keys[i - 1].clone();
        let key2 = l_keys[i].clone();
        assert!(key1 < key2);
    }
    let mut map = HashMap::new();
    for key in &l_keys {
        for u in 1..key.len() + 1 {
            let key_prefix = key[0..u].to_vec();
            match map.get_mut(&key_prefix) {
                Some(v) => {
                    *v += 1;
                }
                None => {
                    map.insert(key_prefix, 1);
                }
            }
        }
    }
    for (key_prefix, value) in map {
        let n_ent = key_value_operation
            .find_keys_with_prefix(&key_prefix)
            .await
            .unwrap()
            .count();
        assert!(n_ent == value);
    }
}

#[cfg(test)]
async fn test_ordering_keys<OP: KeyValueOperations + Sync>(key_value_operation: OP) {
    let key_prefix = vec![0];
    let n = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let key_value_vec = get_random_key_value_vec_prefix(&mut rng, key_prefix.clone(), n);
    test_ordering_keys_key_value_vec(key_value_operation, key_value_vec).await;
}

#[tokio::test]
async fn test_ordering_memory() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_owned().await;
    let key_value_operation = Arc::new(RwLock::new(guard));
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_rocksdb() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    let key_value_operation = Arc::new(DB::open(&options, &dir).unwrap());
    //
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
#[ignore]
async fn test_ordering_dynamodb() {
    let localstack = LocalStackTestContext::new().await.unwrap();
    let (key_value_operation, _) = DynamoDbContainer::from_config(
        localstack.dynamo_db_config(),
        "test_table".parse().expect("Invalid table name"),
    )
    .await
    .unwrap();
    //
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_memory_specific() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_owned().await;
    let key_value_operation = Arc::new(RwLock::new(guard));
    let key_value_vec = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    test_ordering_keys_key_value_vec(key_value_operation, key_value_vec).await;
}
