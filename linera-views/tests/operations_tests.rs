// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    batch::Batch,
    common::{get_interval, KeyIterable, KeyValueIterable, KeyValueStoreClient},
    key_value_store_view::ViewContainer,
    memory::{create_memory_client, create_memory_context},
    test_utils::{get_random_byte_vector, get_random_key_value_vec_prefix, get_small_key_space},
    value_splitting::create_test_memory_client,
};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashSet};

#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::create_rocks_db_test_client;

#[cfg(feature = "aws")]
use linera_views::dynamo_db::create_dynamo_db_test_client;

#[cfg(feature = "scylladb")]
use linera_views::scylla_db::create_scylla_db_test_client;

/// This test starts with a collection of key/values being inserted into the code
/// which is then followed by a number of reading tests. The functionalities being
/// tested are all the reading functionalities:
/// * `read_key_bytes`
/// * `read_multi_key_bytes`
/// * `find_keys_by_prefix` / `find_key_values_by_prefix`
/// * The ordering of keys returned by `find_keys_by_prefix` and `find_key_values_by_prefix`
#[cfg(test)]
async fn run_readings_vec<OP: KeyValueStoreClient + Sync>(
    key_value_store: OP,
    key_value_vec: Vec<(Vec<u8>, Vec<u8>)>,
) {
    // We need a nontrivial key_prefix because dynamo requires a non-trivial prefix
    let mut batch = Batch::new();
    let mut keys = Vec::new();
    let mut set_keys = HashSet::new();
    for (key, value) in &key_value_vec {
        keys.push(&key[..]);
        set_keys.insert(&key[..]);
        batch.put_key_value_bytes(key.clone(), value.clone());
    }
    key_value_store.write_batch(batch, &[]).await.unwrap();
    for key_prefix in keys
        .iter()
        .flat_map(|key| (0..key.len()).map(|u| &key[..=u]))
    {
        // Getting the find_keys_by_prefix / find_key_values_by_prefix
        let len_prefix = key_prefix.len();
        let keys_by_prefix = key_value_store
            .find_keys_by_prefix(key_prefix)
            .await
            .unwrap();
        let keys_request: Vec<_> = keys_by_prefix.iterator().map(Result::unwrap).collect();
        let mut set_key_value1 = HashSet::new();
        let mut keys_request_deriv = Vec::new();
        let key_values_by_prefix = key_value_store
            .find_key_values_by_prefix(key_prefix)
            .await
            .unwrap();
        for (key, value) in key_values_by_prefix.iterator().map(Result::unwrap) {
            set_key_value1.insert((key, value));
            keys_request_deriv.push(key);
        }
        // Check find_keys / find_key_values
        assert_eq!(keys_request, keys_request_deriv);
        // Check key ordering
        for i in 1..keys_request.len() {
            assert!(keys_request[i - 1] < keys_request[i]);
        }
        // Check the obtained values
        let mut set_key_value2 = HashSet::new();
        for (key, value) in &key_value_vec {
            if key.starts_with(key_prefix) {
                set_key_value2.insert((&key[len_prefix..], &value[..]));
            }
        }
        assert_eq!(set_key_value1, set_key_value2);
    }
    // Now checking the read_multi_key_bytes
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for _ in 0..10 {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for (key, value) in &key_value_vec {
            if rng.gen() {
                // Put a key that is already present
                keys.push(key.clone());
                values.push(Some(value.clone()));
            } else {
                // Put a missing key
                let len = key.len();
                let pos = rng.gen_range(0..len);
                let byte = *key.get(pos).unwrap();
                let new_byte: u8 = if byte < 255 { byte + 1 } else { byte - 1 };
                let mut new_key = key.clone();
                *new_key.get_mut(pos).unwrap() = new_byte;
                if !set_keys.contains(&*new_key) {
                    keys.push(new_key);
                    values.push(None);
                }
            }
        }
        let values_read = key_value_store.read_multi_key_bytes(keys).await.unwrap();
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
        if !key_set.contains(&key) {
            key_set.insert(key.clone());
            let value = get_random_byte_vector(&mut rng, &[], len_value);
            key_values.push((key, value));
        }
    }
    key_values
}

fn get_random_test_scenarios() -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut scenarios = Vec::new();
    for len_value in [10, 100] {
        scenarios.push(get_random_key_value_vec1(len_value));
        scenarios.push(get_random_key_value_vec2(len_value));
    }
    scenarios
}

#[tokio::test]
async fn test_readings_test_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_test_memory_client();
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_readings_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_memory_client();
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_readings_rocks_db() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_rocks_db_test_client().await;
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_readings_dynamodb() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_dynamo_db_test_client().await;
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_readings_scylla_db() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_scylla_db_test_client().await;
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_readings_key_value_store_view_memory() {
    for scenario in get_random_test_scenarios() {
        let context = create_memory_context();
        let key_value_store = ViewContainer::new(context).await.unwrap();
        run_readings_vec(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_readings_memory_specific() {
    let key_value_store = create_memory_client();
    let key_value_vec = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    run_readings_vec(key_value_store, key_value_vec).await;
}

#[cfg(test)]
async fn run_writings_random<OP: KeyValueStoreClient + Sync>(key_value_store: OP) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut kv_state = BTreeMap::new();
    let n_oper = 100;
    let size_batch = 8;
    // key space has size 4^4 = 256 so we necessarily encounter collisions
    // because the number of generated keys is about size_batch * n_oper = 800 > 256.
    let key_prefix = vec![0];
    for _ in 0..n_oper {
        let mut batch = Batch::new();
        for _ in 0..size_batch {
            let choice = rng.gen_range(0..8);
            // Inserting a key
            if choice < 5 {
                // Insert
                let key = get_small_key_space(&mut rng, &key_prefix, 4);
                let len_value = rng.gen_range(0..10); // Could need to be split
                let value = get_random_byte_vector(&mut rng, &[], len_value);
                batch.put_key_value_bytes(key.clone(), value.clone());
                kv_state.insert(key, value);
            }
            if choice == 6 {
                // key might be missing, no matter, it has to work
                let key = get_small_key_space(&mut rng, &key_prefix, 4);
                kv_state.remove(&key);
                batch.delete_key(key);
            }
            if choice == 7 {
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
        key_value_store.write_batch(batch, &[]).await.unwrap();
        // Checking the consistency
        let mut key_values = BTreeMap::new();
        for key_value in key_value_store
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
async fn test_writings_test_memory() {
    let key_value_store = create_test_memory_client();
    run_writings_random(key_value_store).await;
}

#[tokio::test]
async fn test_writings_memory() {
    let key_value_store = create_memory_client();
    run_writings_random(key_value_store).await;
}

#[tokio::test]
async fn test_writings_key_value_store_view_memory() {
    let context = create_memory_context();
    let key_value_store = ViewContainer::new(context).await.unwrap();
    run_writings_random(key_value_store).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_writings_rocks_db() {
    let key_value_store = create_rocks_db_test_client().await;
    run_writings_random(key_value_store).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_writings_dynamodb() {
    let key_value_store = create_dynamo_db_test_client().await;
    run_writings_random(key_value_store).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_writings_scylla_db() {
    let key_value_store = create_scylla_db_test_client().await;
    run_writings_random(key_value_store).await;
}

#[tokio::test]
async fn test_big_value_read_write() {
    use rand::{distributions::Alphanumeric, Rng};
    let context = create_memory_context();
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

// DynamoDb has limits at 1M (for pagination), 4M (for write)
// Let us go right past them at 20M of data with writing and then
// reading it. And 20M is not huge by any mean. All KeyValueStoreClient
// must handle that.
//
// The size of the value vary as each size has its own issues.
#[cfg(test)]
async fn run_big_write_read<OP: KeyValueStoreClient + Sync>(key_value_store: OP) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let target_size = 20000000;
    for (pos, value_size) in [100, 1000, 200000, 5000000].into_iter().enumerate() {
        let n_entry = target_size / value_size;
        let mut batch = Batch::new();
        let key_prefix = vec![0, pos as u8];
        let mut key_values1 = BTreeMap::new();
        for i in 0..n_entry {
            let mut key = key_prefix.clone();
            bcs::serialize_into(&mut key, &i).unwrap();
            let value = get_random_byte_vector(&mut rng, &[], value_size);
            key_values1.insert(key.clone(), value.clone());
            batch.put_key_value_bytes(key, value);
        }
        key_value_store.write_batch(batch, &[]).await.unwrap();
        // Checking the consistency
        let mut key_values2 = BTreeMap::new();
        for key_value in key_value_store
            .find_key_values_by_prefix(&key_prefix)
            .await
            .unwrap()
            .iterator()
        {
            let (key_suffix, value) = key_value.unwrap();
            let mut key = key_prefix.clone();
            key.extend(key_suffix);
            key_values2.insert(key, value.to_vec());
        }
        assert_eq!(key_values1, key_values2);
    }
}

#[tokio::test]
async fn test_big_write_read_memory() {
    let key_value_store = create_memory_client();
    run_big_write_read(key_value_store).await;
}

// TODO(#1092): That test fails for value of 5M, probably needs value splitting.
#[ignore]
#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_big_write_read_rocks_db() {
    let key_value_store = create_rocks_db_test_client().await;
    run_big_write_read(key_value_store).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_big_write_read_dynamo_db() {
    let key_value_store = create_dynamo_db_test_client().await;
    run_big_write_read(key_value_store).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_big_write_read_scylla_db() {
    let key_value_store = create_scylla_db_test_client().await;
    run_big_write_read(key_value_store).await;
}
