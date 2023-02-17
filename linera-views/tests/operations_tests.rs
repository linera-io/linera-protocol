// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_lock::{Mutex, RwLock};
use linera_views::{
    common::{Batch, KeyIterable, KeyValueOperations},
    key_value_store_view::ViewContainer,
    memory::MemoryContext,
    rocksdb::DB,
    test_utils::get_random_key_value_vec_prefix,
};
use rand::SeedableRng;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[cfg(all(not(target_arch = "wasm32"), feature = "aws"))]
use linera_views::{dynamo_db::DynamoDbClient, test_utils::LocalStackTestContext};

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
    let keys: Vec<Vec<u8>> = key_value_operation
        .find_keys_by_prefix(&key_prefix)
        .await
        .unwrap()
        .iterator()
        .map(|x| {
            let mut v = key_prefix.clone();
            v.extend(x.unwrap());
            v
        })
        .collect();
    for i in 1..keys.len() {
        let key1 = keys[i - 1].clone();
        let key2 = keys[i].clone();
        assert!(key1 < key2);
    }
    let mut map = HashMap::new();
    for key in &keys {
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
            .find_keys_by_prefix(&key_prefix)
            .await
            .unwrap()
            .iterator()
            .count();
        assert_eq!(n_ent, value);
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
    let guard = map.clone().lock_arc().await;
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

#[cfg(all(not(target_arch = "wasm32"), feature = "aws"))]
#[tokio::test]
async fn test_ordering_dynamodb() {
    let localstack = LocalStackTestContext::new().await.unwrap();
    let (key_value_operation, _) = DynamoDbClient::from_config(
        localstack.dynamo_db_config(),
        "test_table".parse().expect("Invalid table name"),
    )
    .await
    .unwrap();
    //
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_key_value_store_view_memory() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_arc().await;
    let context = MemoryContext::new(guard, ());
    let key_value_operation = ViewContainer::new(context).await.unwrap();
    test_ordering_keys(key_value_operation).await;
}

#[tokio::test]
async fn test_ordering_memory_specific() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_arc().await;
    let key_value_operation = Arc::new(RwLock::new(guard));
    let key_value_vec = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    test_ordering_keys_key_value_vec(key_value_operation, key_value_vec).await;
}
