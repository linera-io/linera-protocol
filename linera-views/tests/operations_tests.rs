// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_lock::{Mutex, RwLock};
use linera_views::{
    batch::Batch,
    common::{get_interval, KeyIterable, KeyValueIterable, KeyValueStoreClient},
    key_value_store_view::ViewContainer,
    memory::{create_memory_test_client, create_test_context},
    test_utils::{get_random_byte_vector, get_random_key_value_vec_prefix, get_small_key_space},
};
use rand::{Rng, SeedableRng};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

#[cfg(feature = "rocksdb")]
use linera_views::rocksdb::create_rocksdb_test_client;

#[cfg(feature = "aws")]
use linera_views::test_utils::create_dynamodb_test_client;

#[cfg(test)]
async fn test_readings_vec<OP: KeyValueStoreClient + Sync>(
    key_value_operation: OP,
    key_value_vec: Vec<(Vec<u8>, Vec<u8>)>,
) {
    // We need a nontrivial key_prefix because dynamo requires a non-trivial prefix
    let mut batch = Batch::new();
    let mut keys = Vec::new();
    let mut set_keys = HashSet::new();
    for key_value in &key_value_vec {
        keys.push(key_value.0.clone());
        set_keys.insert(key_value.0.clone());
        batch.put_key_value_bytes(key_value.0.clone(), key_value.1.clone());
    }
    key_value_operation.write_batch(batch, &[]).await.unwrap();
    let mut set = HashSet::new();
    for key in &keys {
        for u in 1..key.len() + 1 {
            let key_prefix = key[0..u].to_vec();
            set.insert(key_prefix);
        }
    }
    for key_prefix in set {
        // Getting the find_keys_by_prefix / find_key_values_by_prefix
        let len_prefix = key_prefix.len();
        let mut keys_request = Vec::new();
        for key in key_value_operation
            .find_keys_by_prefix(&key_prefix)
            .await
            .unwrap()
            .iterator()
        {
            let key = key.unwrap().to_vec();
            keys_request.push(key);
        }
        let mut key_values_request = Vec::new();
        let mut keys_request_deriv = Vec::new();
        for key_value in key_value_operation
            .find_key_values_by_prefix(&key_prefix)
            .await
            .unwrap()
            .iterator()
        {
            let key_value = key_value.unwrap();
            key_values_request.push((key_value.0.to_vec(), key_value.1.to_vec()));
            keys_request_deriv.push(key_value.0.to_vec());
        }
        let len = keys_request.len();
        // Check find_keys / find_key_values
        assert_eq!(keys_request, keys_request_deriv);
        // Check key ordering
        for i in 1..len {
            let key1 = keys_request[i - 1].clone();
            let key2 = keys_request[i].clone();
            assert!(key1 < key2);
        }
        // Check the obtained values
        let mut set_key_value1 = HashSet::<(Vec<u8>, Vec<u8>)>::new();
        for key_value in key_values_request {
            set_key_value1.insert((key_value.0.to_vec(), key_value.1.to_vec()));
        }
        let mut set_key_value2 = HashSet::new();
        for key_value in &key_value_vec {
            if key_value.0.starts_with(&key_prefix) {
                let key = key_value.0[len_prefix..].to_vec();
                let value = key_value.1.clone();
                set_key_value2.insert((key, value));
            }
        }
        assert_eq!(set_key_value1, set_key_value2);
    }
    // Now checking the read_multi_key_bytes
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for _ in 0..10 {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for key_value in &key_value_vec {
            let thr = rng.gen_range(0..2);
            if thr == 0 {
                // Put a key that is already present
                keys.push(key_value.0.clone());
                values.push(Some(key_value.1.clone()));
            } else {
                // Put a missing key
                let len = key_value.0.len();
                let pos = rng.gen_range(0..len);
                let byte = *key_value.0.get(pos).unwrap();
                let new_byte: u8 = if byte < 255 { byte + 1 } else { byte - 1 };
                let mut new_key = key_value.0.clone();
                *new_key.get_mut(pos).unwrap() = new_byte;
                if !set_keys.contains(&new_key) {
                    keys.push(new_key);
                    values.push(None);
                }
            }
        }
        let values_read = key_value_operation
            .read_multi_key_bytes(keys)
            .await
            .unwrap();
        assert_eq!(values, values_read);
    }
}

fn get_random_key_value_vec1(len_value: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let key_prefix = vec![0];
    let n = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    get_random_key_value_vec_prefix(&mut rng, key_prefix, 8, len_value, n)
}

fn get_random_key_value_vec2(len_value: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let key_prefix = vec![0];
    let n = 100;
    let mut key_values = Vec::new();
    let mut key_set = HashSet::new();
    for _ in 0..n {
        let key = get_small_key_space(&mut rng, &key_prefix, 4);
        let value = get_random_byte_vector(&mut rng, &[], len_value);
        if !key_set.contains(&key) {
            key_set.insert(key.clone());
            key_values.push((key, value));
        }
    }
    key_values
}

fn get_random_test_scenarios() -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut scenarios = Vec::new();
    for len_value in [10, 1000] {
        scenarios.push(get_random_key_value_vec1(len_value));
        scenarios.push(get_random_key_value_vec2(len_value));
    }
    scenarios
}

#[tokio::test]
async fn test_readings_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_operation = create_memory_test_client();
        test_readings_vec(key_value_operation, scenario).await;
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_readings_rocksdb() {
    for scenario in get_random_test_scenarios() {
        let key_value_operation = create_rocksdb_test_client();
        test_readings_vec(key_value_operation, scenario).await;
    }
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_readings_dynamodb() {
    for scenario in get_random_test_scenarios() {
        let key_value_operation = create_dynamodb_test_client().await;
        test_readings_vec(key_value_operation, scenario).await;
    }
}

#[tokio::test]
async fn test_readings_key_value_store_view_memory() {
    for scenario in get_random_test_scenarios() {
        let context = create_test_context();
        let key_value_operation = ViewContainer::new(context).await.unwrap();
        test_readings_vec(key_value_operation, scenario).await;
    }
}

#[tokio::test]
async fn test_readings_memory_specific() {
    let map = Arc::new(Mutex::new(BTreeMap::new()));
    let guard = map.clone().lock_arc().await;
    let key_value_operation = Arc::new(RwLock::new(guard));
    let key_value_vec = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    test_readings_vec(key_value_operation, key_value_vec).await;
}

#[cfg(test)]
async fn test_writings_random<OP: KeyValueStoreClient + Sync>(key_value_operation: OP) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut kv_state = BTreeMap::new();
    let n_oper = 100;
    let siz_batch = 8;
    // key space has size 4^4 = 256 so we necessarily encounter collisions
    let key_prefix = vec![0];
    for _ in 0..n_oper {
        let mut batch = Batch::new();
        for _ in 0..siz_batch {
            let thr = rng.gen_range(0..8);
            // Inserting a key
            if thr < 5 {
                // Insert
                let key = get_small_key_space(&mut rng, &key_prefix, 4);
                let len_value = rng.gen_range(0..10); // Could need to be split
                let value = get_random_byte_vector(&mut rng, &[], len_value);
                batch.put_key_value_bytes(key.clone(), value.clone());
                kv_state.insert(key, value);
            }
            if thr == 6 {
                // key might be missing, no matter, it has to work
                let key = get_small_key_space(&mut rng, &key_prefix, 4);
                kv_state.remove(&key);
                batch.delete_key(key);
            }
            if thr == 7 {
                let len = rng.gen_range(1..4); // We want a non-trivial range
                let delete_key_prefix = get_small_key_space(&mut rng, &key_prefix, len);
                batch.delete_key_prefix(delete_key_prefix.clone());
                let key_list = kv_state
                    .range(get_interval(delete_key_prefix.clone()))
                    .map(|x| x.0.to_vec())
                    .collect::<Vec<_>>();
                for key in key_list {
                    kv_state.remove(&key);
                }
            }
        }
        key_value_operation.write_batch(batch, &[]).await.unwrap();
        // Checking the consistency
        let mut key_values = BTreeMap::new();
        for key_value in key_value_operation
            .find_key_values_by_prefix(&key_prefix)
            .await
            .unwrap()
            .iterator()
        {
            let key_value = key_value.unwrap();
            let mut key = key_prefix.clone();
            key.extend(key_value.0);
            key_values.insert(key, key_value.1.to_vec());
        }
        assert_eq!(key_values, kv_state);
    }
}

#[tokio::test]
async fn test_writings_memory() {
    let key_value_operation = create_memory_test_client();
    test_writings_random(key_value_operation).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_writings_rocksdb() {
    let key_value_operation = create_rocksdb_test_client();
    test_writings_random(key_value_operation).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_writings_dynamodb() {
    let key_value_operation = create_dynamodb_test_client().await;
    test_writings_random(key_value_operation).await;
}

#[tokio::test]
async fn test_big_value_read_write() {
    use rand::{distributions::Alphanumeric, Rng};
    let context = create_test_context();
    for count in [50, 1024] {
        let rng = rand::rngs::StdRng::seed_from_u64(2);
        let test_string = rng
            .sample_iter(&Alphanumeric)
            .take(count)
            .map(char::from)
            .collect::<String>();
        let mut batch = Batch::new();
        let key = vec![43, 23, 56];
        batch.put_key_value(key.clone(), &test_string).unwrap();
        context.db.write_batch(batch, &[]).await.unwrap();
        let read_string = context.db.read_key::<String>(&key).await.unwrap().unwrap();
        assert_eq!(read_string, test_string);
    }
}
