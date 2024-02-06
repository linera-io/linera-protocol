// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    batch::{Batch, WriteOperation},
    common::{
        KeyIterable, KeyValueIterable, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore,
    },
    key_value_store_view::ViewContainer,
    memory::{create_memory_context, create_memory_store},
    test_utils::{
        get_random_byte_vector, get_random_key_prefix, get_random_key_values_prefix,
        get_small_key_space,
    },
    value_splitting::create_test_memory_store,
};
use rand::{Rng, RngCore, SeedableRng};
use std::collections::{BTreeMap, HashSet};

#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::create_rocks_db_test_store;

#[cfg(feature = "aws")]
use linera_views::dynamo_db::create_dynamo_db_test_store;

#[cfg(feature = "scylladb")]
use linera_views::scylla_db::create_scylla_db_test_store;

/// This test starts with a collection of key/values being inserted into the code
/// which is then followed by a number of reading tests. The functionalities being
/// tested are all the reading functionalities:
/// * `read_value_bytes`
/// * `read_multi_values_bytes`
/// * `find_keys_by_prefix` / `find_key_values_by_prefix`
/// * The ordering of keys returned by `find_keys_by_prefix` and `find_key_values_by_prefix`
#[cfg(test)]
async fn run_reads<S: KeyValueStore + Sync>(store: S, key_values: Vec<(Vec<u8>, Vec<u8>)>) {
    // We need a nontrivial key_prefix because dynamo requires a non-trivial prefix
    let mut batch = Batch::new();
    let mut keys = Vec::new();
    let mut set_keys = HashSet::new();
    for (key, value) in &key_values {
        keys.push(&key[..]);
        set_keys.insert(&key[..]);
        batch.put_key_value_bytes(key.clone(), value.clone());
    }
    store.write_batch(batch, &[]).await.unwrap();
    for key_prefix in keys
        .iter()
        .flat_map(|key| (0..key.len()).map(|u| &key[..=u]))
    {
        // Getting the find_keys_by_prefix / find_key_values_by_prefix
        let len_prefix = key_prefix.len();
        let keys_by_prefix = store.find_keys_by_prefix(key_prefix).await.unwrap();
        let keys_request = keys_by_prefix
            .iterator()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let mut set_key_value1 = HashSet::new();
        let mut keys_request_deriv = Vec::new();
        let key_values_by_prefix = store.find_key_values_by_prefix(key_prefix).await.unwrap();
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
        for (key, value) in &key_values {
            if key.starts_with(key_prefix) {
                set_key_value2.insert((&key[len_prefix..], &value[..]));
            }
        }
        assert_eq!(set_key_value1, set_key_value2);
    }
    // Now checking the read_multi_values_bytes
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for _ in 0..10 {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for (key, value) in &key_values {
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
        let mut test_exists = Vec::new();
        for key in &keys {
            test_exists.push(store.contains_key(key).await.unwrap());
        }
        let values_read = store.read_multi_values_bytes(keys).await.unwrap();
        assert_eq!(values, values_read);
        let values_read_stat = values_read.iter().map(|x| x.is_some()).collect::<Vec<_>>();
        assert_eq!(values_read_stat, test_exists);
    }
}

#[cfg(test)]
fn get_random_key_values1(len_value: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let key_prefix = vec![0];
    let n = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    get_random_key_values_prefix(&mut rng, key_prefix, 8, len_value, n)
}

#[cfg(test)]
fn get_random_key_values2(len_value: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
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

#[cfg(test)]
fn get_random_test_scenarios() -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut scenarios = Vec::new();
    for len_value in [10, 100] {
        scenarios.push(get_random_key_values1(len_value));
        scenarios.push(get_random_key_values2(len_value));
    }
    scenarios
}

#[tokio::test]
async fn test_reads_test_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_test_memory_store();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_reads_memory() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_memory_store();
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_reads_rocks_db() {
    for scenario in get_random_test_scenarios() {
        let (key_value_store, _dir) = create_rocks_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_reads_dynamodb() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_dynamo_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_reads_scylla_db() {
    for scenario in get_random_test_scenarios() {
        let key_value_store = create_scylla_db_test_store().await;
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_reads_key_value_store_view_memory() {
    for scenario in get_random_test_scenarios() {
        let context = create_memory_context();
        let key_value_store = ViewContainer::new(context).await.unwrap();
        run_reads(key_value_store, scenario).await;
    }
}

#[tokio::test]
async fn test_reads_memory_specific() {
    let key_value_store = create_memory_store();
    let key_values = vec![
        (vec![0, 1, 255], Vec::new()),
        (vec![0, 1, 255, 37], Vec::new()),
        (vec![0, 2], Vec::new()),
        (vec![0, 2, 0], Vec::new()),
    ];
    run_reads(key_value_store, key_values).await;
}

#[cfg(test)]
fn generate_random_batch<R: RngCore>(rng: &mut R, key_prefix: &[u8], batch_size: usize) -> Batch {
    let mut batch = Batch::new();
    // Fully random batch
    for _ in 0..batch_size {
        let choice = rng.gen_range(0..8);
        // Inserting a key
        if choice < 6 {
            // Insert
            let key = get_small_key_space(rng, key_prefix, 4);
            let len_value = rng.gen_range(0..10); // Could need to be split
            let value = get_random_byte_vector(rng, &[], len_value);
            batch.put_key_value_bytes(key.clone(), value.clone());
        }
        if choice == 6 {
            // key might be missing, no matter, it has to work
            let key = get_small_key_space(rng, key_prefix, 4);
            batch.delete_key(key);
        }
        if choice == 7 {
            let len = rng.gen_range(1..4); // We want a non-trivial range
            let delete_key_prefix = get_small_key_space(rng, key_prefix, len);
            batch.delete_key_prefix(delete_key_prefix.clone());
        }
    }
    batch
}

#[cfg(test)]
fn get_key(key_prefix: &[u8], key_suffix: Vec<u8>) -> Vec<u8> {
    let mut key = key_prefix.to_vec();
    key.extend(key_suffix);
    key
}

#[cfg(test)]
fn generate_specific_batch(key_prefix: &[u8], option: usize) -> Batch {
    let mut batch = Batch::new();
    if option == 0 {
        let key = get_key(key_prefix, vec![34]);
        batch.put_key_value_bytes(key.clone(), vec![]);
        batch.delete_key(key);
    }
    if option == 1 {
        let key1 = get_key(key_prefix, vec![12, 34]);
        let key2 = get_key(key_prefix, vec![12, 33]);
        let key3 = get_key(key_prefix, vec![13]);
        batch.put_key_value_bytes(key1.clone(), vec![]);
        batch.put_key_value_bytes(key2, vec![]);
        batch.put_key_value_bytes(key3, vec![]);
        batch.delete_key(key1);
        let key_prefix = get_key(key_prefix, vec![12]);
        batch.delete_key_prefix(key_prefix);
    }
    batch
}

#[cfg(test)]
fn update_state_from_batch(kv_state: &mut BTreeMap<Vec<u8>, Vec<u8>>, batch: &Batch) {
    for operation in &batch.operations {
        match operation {
            WriteOperation::Put { key, value } => {
                kv_state.insert(key.to_vec(), value.to_vec());
            }
            WriteOperation::Delete { key } => {
                kv_state.remove(key);
            }
            WriteOperation::DeletePrefix { key_prefix } => {
                kv_state.retain(|key, _| !key.starts_with(key_prefix));
            }
        }
    }
}

#[cfg(test)]
fn realize_batch(batch: &Batch) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut kv_state = BTreeMap::new();
    update_state_from_batch(&mut kv_state, batch);
    kv_state
}

#[cfg(test)]
async fn read_key_values_prefix<C: KeyValueStore + Sync>(
    key_value_store: &C,
    key_prefix: &[u8],
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut key_values = BTreeMap::new();
    for key_value in key_value_store
        .find_key_values_by_prefix(key_prefix)
        .await
        .unwrap()
        .iterator()
    {
        let (key_suffix, value) = key_value.unwrap();
        let mut key = key_prefix.to_vec();
        key.extend(key_suffix);
        key_values.insert(key, value.to_vec());
    }
    key_values
}

#[cfg(test)]
async fn run_test_batch_from_blank<C: KeyValueStore + Sync>(
    key_value_store: &C,
    key_prefix: Vec<u8>,
    batch: Batch,
) {
    let kv_state = realize_batch(&batch);
    key_value_store.write_batch(batch, &[]).await.unwrap();
    // Checking the consistency
    let key_values = read_key_values_prefix(key_value_store, &key_prefix).await;
    assert_eq!(key_values, kv_state);
}

#[cfg(test)]
async fn run_writes_from_blank<C: KeyValueStore + Sync>(key_value_store: &C) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let n_oper = 1000;
    let batch_size = 500;
    // key space has size 4^4 = 256 so we necessarily encounter collisions
    // because the number of generated keys is about batch_size * n_oper = 800 > 256.
    for _ in 0..n_oper {
        let key_prefix = get_random_key_prefix();
        let batch = generate_random_batch(&mut rng, &key_prefix, batch_size);
        run_test_batch_from_blank(key_value_store, key_prefix, batch).await;
    }
    for option in 0..2 {
        let key_prefix = get_random_key_prefix();
        let batch = generate_specific_batch(&key_prefix, option);
        run_test_batch_from_blank(key_value_store, key_prefix, batch).await;
    }
}

#[tokio::test]
async fn test_test_memory_writes_from_blank() {
    let key_value_store = create_test_memory_store();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_memory_writes_from_blank() {
    let key_value_store = create_memory_store();
    run_writes_from_blank(&key_value_store).await;
}

#[tokio::test]
async fn test_key_value_store_view_memory_writes_from_blank() {
    let context = create_memory_context();
    let key_value_store = ViewContainer::new(context).await.unwrap();
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_rocks_db_writes_from_blank() {
    let (key_value_store, _dir) = create_rocks_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_dynamo_db_writes_from_blank() {
    let key_value_store = create_dynamo_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylla_db_writes_from_blank() {
    let key_value_store = create_scylla_db_test_store().await;
    run_writes_from_blank(&key_value_store).await;
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
        context.store.write_batch(batch, &[]).await.unwrap();
        let read_string = context
            .store
            .read_value::<String>(&key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_string, test_string);
    }
}

// That test is especially challenging for ScyllaDB.
// In its default settings, Scylla has a limitation to 10000 tombstones.
// A tombstone is an indication that the data has been deleted. That
// is thus a trie data structure for checking whether a requested key
// is deleted or not.
//
// In this test we insert 200000 keys into the database.
// Then we select half of them at random and delete them. By the random
// selection, Scylla is forced to introduce around 100000 tombstones
// which triggers the crash with the default settings.
#[cfg(feature = "scylladb")]
#[cfg(test)]
async fn tombstone_triggering_test<C: KeyValueStore + Sync>(key_value_store: C) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let value_size = 100;
    let n_entry = 200000;
    // Putting the keys
    let mut batch_insert = Batch::new();
    let key_prefix = vec![0];
    let mut batch_delete = Batch::new();
    let mut remaining_key_values = BTreeMap::new();
    for i in 0..n_entry {
        let mut key = key_prefix.clone();
        bcs::serialize_into(&mut key, &i).unwrap();
        let value = get_random_byte_vector(&mut rng, &[], value_size);
        batch_insert.put_key_value_bytes(key.clone(), value.clone());
        let to_delete = rng.gen::<bool>();
        if to_delete {
            batch_delete.delete_key(key);
        } else {
            remaining_key_values.insert(key, value);
        }
    }
    run_test_batch_from_blank(&key_value_store, key_prefix.clone(), batch_insert).await;
    // Deleting them all
    key_value_store
        .write_batch(batch_delete, &[])
        .await
        .unwrap();
    // Reading everything and seeing that it is now cleaned.
    let key_values = read_key_values_prefix(&key_value_store, &key_prefix).await;
    assert_eq!(key_values, remaining_key_values);
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn scylla_db_tombstone_triggering_test() {
    let key_value_store = create_scylla_db_test_store().await;
    tombstone_triggering_test(key_value_store).await;
}

// DynamoDb has limits at 1M (for pagination), 4M (for write)
// Let us go right past them at 20M of data with writing and then
// reading it. And 20M is not huge by any mean. All KeyValueStore
// must handle that.
//
// The size of the value vary as each size has its own issues.
#[cfg(test)]
async fn run_big_write_read<C: KeyValueStore + Sync>(
    key_value_store: C,
    target_size: usize,
    value_sizes: Vec<usize>,
) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for (pos, value_size) in value_sizes.into_iter().enumerate() {
        let n_entry: usize = target_size / value_size;
        let mut batch = Batch::new();
        let key_prefix = vec![0, pos as u8];
        for i in 0..n_entry {
            let mut key = key_prefix.clone();
            bcs::serialize_into(&mut key, &i).unwrap();
            let value = get_random_byte_vector(&mut rng, &[], value_size);
            batch.put_key_value_bytes(key, value);
        }
        run_test_batch_from_blank(&key_value_store, key_prefix, batch).await;
    }
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylla_db_big_write_read() {
    let key_value_store = create_scylla_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[tokio::test]
async fn test_memory_big_write_read() {
    let key_value_store = create_memory_store();
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_rocks_db_big_write_read() {
    let (key_value_store, _dir) = create_rocks_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_dynamo_db_big_write_read() {
    let key_value_store = create_dynamo_db_test_store().await;
    let value_sizes = vec![100, 1000, 200000, 5000000];
    let target_size = 20000000;
    run_big_write_read(key_value_store, target_size, value_sizes).await;
}

#[cfg(test)]
type StateBatch = (Vec<(Vec<u8>, Vec<u8>)>, Batch);

#[cfg(test)]
async fn run_test_batch_from_state<C: KeyValueStore + Sync>(
    key_value_store: &C,
    key_prefix: Vec<u8>,
    state_and_batch: StateBatch,
) {
    let (key_values, batch) = state_and_batch;
    let mut batch_insert = Batch::new();
    let mut kv_state = BTreeMap::new();
    for (key, value) in key_values {
        kv_state.insert(key.clone(), value.clone());
        batch_insert.put_key_value_bytes(key, value);
    }
    key_value_store
        .write_batch(batch_insert, &[])
        .await
        .unwrap();
    update_state_from_batch(&mut kv_state, &batch);
    key_value_store.write_batch(batch, &[]).await.unwrap();
    let key_values = read_key_values_prefix(key_value_store, &key_prefix).await;
    assert_eq!(key_values, kv_state);
}

#[cfg(test)]
fn generate_specific_state_batch(key_prefix: &[u8], option: usize) -> StateBatch {
    let mut key_values = Vec::new();
    let mut batch = Batch::new();
    if option == 0 {
        // A DeletePrefix followed by an insertion that matches the DeletePrefix
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        let key3 = get_key(key_prefix, vec![1, 4, 5]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.delete_key_prefix(key2);
        batch.put_key_value_bytes(key3, vec![23]);
    }
    if option == 1 {
        // Just a DeletePrefix
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.delete_key_prefix(key2);
    }
    if option == 2 {
        // A Put followed by a DeletePrefix that matches the Put
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        let key3 = get_key(key_prefix, vec![1, 4, 5]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.put_key_value_bytes(key3, vec![23]);
        batch.delete_key_prefix(key2);
    }
    if option == 3 {
        // A Put followed by a Delete on the same value
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        let key3 = get_key(key_prefix, vec![1, 4, 5]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.put_key_value_bytes(key3.clone(), vec![23]);
        batch.delete_key(key3);
    }
    if option == 4 {
        // A Delete Key followed by a Put on the same key
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        let key3 = get_key(key_prefix, vec![1, 4, 5]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.delete_key(key3.clone());
        batch.put_key_value_bytes(key3, vec![23]);
    }
    if option == 5 {
        // A Delete Key followed by a Put on the same key
        let key1 = get_key(key_prefix, vec![1, 3]);
        let key2 = get_key(key_prefix, vec![1, 4]);
        let key3 = get_key(key_prefix, vec![1, 4, 5]);
        let key4 = get_key(key_prefix, vec![1, 5]);
        key_values.push((key1.clone(), vec![34]));
        key_values.push((key2.clone(), vec![45]));
        batch.delete_key(key3.clone());
        batch.put_key_value_bytes(key4, vec![23]);
    }
    if option == 6 {
        let key1 = get_key(key_prefix, vec![0]);
        let key2 = get_key(key_prefix, vec![]);
        key_values.push((key1, vec![33]));
        batch.delete_key_prefix(key2);
    }
    (key_values, batch)
}

#[cfg(test)]
async fn run_writes_from_state<C: KeyValueStore + Sync>(key_value_store: &C) {
    for option in 0..7 {
        let key_prefix = if option == 6 {
            vec![255, 255, 255]
        } else {
            get_random_key_prefix()
        };
        let state_batch = generate_specific_state_batch(&key_prefix, option);
        run_test_batch_from_state(key_value_store, key_prefix, state_batch).await;
    }
}

#[tokio::test]
async fn test_memory_writes_from_state() {
    let key_value_store = create_memory_store();
    run_writes_from_state(&key_value_store).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_rocks_db_writes_from_state() {
    let (key_value_store, _dir) = create_rocks_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_dynamo_db_writes_from_state() {
    let key_value_store = create_dynamo_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_scylla_db_writes_from_state() {
    let key_value_store = create_scylla_db_test_store().await;
    run_writes_from_state(&key_value_store).await;
}
