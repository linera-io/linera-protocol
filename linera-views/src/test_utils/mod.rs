// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod test_views;

/// Functions for computing the performance of stores.
#[cfg(not(target_arch = "wasm32"))]
pub mod performance;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Debug,
};

use rand::{seq::SliceRandom, Rng};

use crate::{
    batch::{
        Batch, WriteOperation,
        WriteOperation::{Delete, Put},
    },
    random::{generate_test_namespace, make_deterministic_rng, make_nondeterministic_rng},
    store::{
        KeyIterable, KeyValueIterable, LocalKeyValueStore, LocalRestrictedKeyValueStore,
        TestKeyValueStore,
    },
};

/// Returns a random key_prefix used for tests
pub fn get_random_key_prefix() -> Vec<u8> {
    let mut key_prefix = vec![0];
    let value: usize = make_nondeterministic_rng().rng_mut().gen();
    bcs::serialize_into(&mut key_prefix, &value).unwrap();
    key_prefix
}

/// Takes a random number generator, a key_prefix and extends it by n random bytes.
pub fn get_random_byte_vector<R: Rng>(rng: &mut R, key_prefix: &[u8], n: usize) -> Vec<u8> {
    let mut v = key_prefix.to_vec();
    for _ in 0..n {
        let val = rng.gen_range(0..256) as u8;
        v.push(val);
    }
    v
}

/// Appends a small value to a key making collisions likely.
pub fn get_small_key_space<R: Rng>(rng: &mut R, key_prefix: &[u8], n: usize) -> Vec<u8> {
    let mut key = key_prefix.to_vec();
    for _ in 0..n {
        let byte = rng.gen_range(0..4) as u8;
        key.push(byte);
    }
    key
}

/// Builds a random k element subset of n
pub fn get_random_kset<R: Rng>(rng: &mut R, n: usize, k: usize) -> Vec<usize> {
    let mut values = Vec::new();
    for u in 0..n {
        values.push(u);
    }
    values.shuffle(rng);
    values[..k].to_vec()
}

/// Takes a random number generator, a key_prefix and generates
/// pairs `(key, value)` with key obtained by appending 8 bytes at random to key_prefix
/// and value obtained by appending 8 bytes to the trivial vector.
/// We return n such `(key, value)` pairs which are all distinct
pub fn get_random_key_values_prefix<R: Rng>(
    rng: &mut R,
    key_prefix: Vec<u8>,
    len_key: usize,
    len_value: usize,
    num_entries: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    loop {
        let mut v_ret = Vec::new();
        let mut vector_set = HashSet::new();
        for _ in 0..num_entries {
            let v1 = get_random_byte_vector(rng, &key_prefix, len_key);
            let v2 = get_random_byte_vector(rng, &Vec::new(), len_value);
            let v12 = (v1.clone(), v2);
            vector_set.insert(v1);
            v_ret.push(v12);
        }
        if vector_set.len() == num_entries {
            return v_ret;
        }
    }
}

/// Takes a random number generator rng, a number n and returns n random `(key, value)`
/// which are all distinct with key and value being of length 8.
pub fn get_random_key_values<R: Rng>(rng: &mut R, num_entries: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    get_random_key_values_prefix(rng, Vec::new(), 8, 8, num_entries)
}

type VectorPutDelete = (Vec<(Vec<u8>, Vec<u8>)>, usize);

/// A bunch of puts and some deletes.
pub fn get_random_key_value_operations<R: Rng>(
    rng: &mut R,
    num_entries: usize,
    k: usize,
) -> VectorPutDelete {
    let key_value_vector = get_random_key_values_prefix(rng, Vec::new(), 8, 8, num_entries);
    (key_value_vector, k)
}

/// A random reordering of the puts and deletes.
/// For something like MapView it should get us the same result whatever way we are calling.
pub fn span_random_reordering_put_delete<R: Rng>(
    rng: &mut R,
    info_op: VectorPutDelete,
) -> Vec<WriteOperation> {
    let n = info_op.0.len();
    let k = info_op.1;
    let mut indices = Vec::new();
    for i in 0..n {
        indices.push(i);
    }
    indices.shuffle(rng);
    let mut indices_rev = vec![0; n];
    for i in 0..n {
        indices_rev[indices[i]] = i;
    }
    let mut pos_remove_vector = vec![Vec::new(); n];
    for (i, pos) in indices_rev.iter().enumerate().take(k) {
        let idx = rng.gen_range(*pos..n);
        pos_remove_vector[idx].push(i);
    }
    let mut operations = Vec::new();
    for i in 0..n {
        let pos = indices[i];
        let pair = info_op.0[pos].clone();
        operations.push(Put {
            key: pair.0,
            value: pair.1,
        });
        for pos_remove in pos_remove_vector[i].clone() {
            let key = info_op.0[pos_remove].0.clone();
            operations.push(Delete { key });
        }
    }
    operations
}

/// This test starts with a collection of key/values being inserted into the code
/// which is then followed by a number of reading tests. The functionalities being
/// tested are all the reading functionalities:
/// * `read_value_bytes`
/// * `read_multi_values_bytes`
/// * `find_keys_by_prefix` / `find_key_values_by_prefix`
/// * The ordering of keys returned by `find_keys_by_prefix` and `find_key_values_by_prefix`
pub async fn run_reads<S: LocalRestrictedKeyValueStore>(
    store: S,
    key_values: Vec<(Vec<u8>, Vec<u8>)>,
) {
    // We need a nontrivial key_prefix because dynamo requires a non-trivial prefix
    let mut batch = Batch::new();
    let mut keys = Vec::new();
    let mut set_keys = HashSet::new();
    for (key, value) in &key_values {
        keys.push(&key[..]);
        set_keys.insert(&key[..]);
        batch.put_key_value_bytes(key.clone(), value.clone());
    }
    store.write_batch(batch).await.unwrap();
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
    let mut rng = make_deterministic_rng();
    for _ in 0..3 {
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
        let mut values_single_read = Vec::new();
        for key in &keys {
            test_exists.push(store.contains_key(key).await.unwrap());
            values_single_read.push(store.read_value_bytes(key).await.unwrap());
        }
        let test_exists_direct = store.contains_keys(keys.clone()).await.unwrap();
        let values_read = store.read_multi_values_bytes(keys).await.unwrap();
        assert_eq!(values, values_read);
        assert_eq!(values, values_single_read);
        let values_read_stat = values_read.iter().map(|x| x.is_some()).collect::<Vec<_>>();
        assert_eq!(values_read_stat, test_exists);
        assert_eq!(values_read_stat, test_exists_direct);
    }
}

fn get_random_key_values1(num_entries: usize, len_value: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let key_prefix = vec![0];
    let mut rng = make_deterministic_rng();
    get_random_key_values_prefix(&mut rng, key_prefix, 8, len_value, num_entries)
}

/// Generates a list of random key-values with no duplicates
pub fn get_random_key_values2(
    num_entries: usize,
    len_key: usize,
    len_value: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = make_deterministic_rng();
    let key_prefix = vec![0];
    let mut key_values = Vec::new();
    let mut key_set = HashSet::new();
    for _ in 0..num_entries {
        let key = get_small_key_space(&mut rng, &key_prefix, len_key);
        if !key_set.contains(&key) {
            key_set.insert(key.clone());
            let value = get_random_byte_vector(&mut rng, &[], len_value);
            key_values.push((key, value));
        }
    }
    key_values
}

/// Adds a prefix to a list of key-values
pub fn add_prefix(prefix: &[u8], key_values: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    key_values
        .into_iter()
        .map(|(key, value)| {
            let mut big_key = prefix.to_vec();
            big_key.extend(key);
            (big_key, value)
        })
        .collect()
}

/// We build a number of scenarios for testing the reads.
pub fn get_random_test_scenarios() -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    vec![
        get_random_key_values1(7, 3),
        get_random_key_values1(150, 3),
        get_random_key_values1(30, 10),
        get_random_key_values2(30, 4, 10),
        get_random_key_values2(30, 4, 100),
    ]
}

fn generate_random_batch<R: Rng>(rng: &mut R, key_prefix: &[u8], batch_size: usize) -> Batch {
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

fn get_key(key_prefix: &[u8], key_suffix: Vec<u8>) -> Vec<u8> {
    let mut key = key_prefix.to_vec();
    key.extend(key_suffix);
    key
}

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

fn realize_batch(batch: &Batch) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut kv_state = BTreeMap::new();
    update_state_from_batch(&mut kv_state, batch);
    kv_state
}

async fn read_key_values_prefix<C: LocalRestrictedKeyValueStore>(
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

/// Writes and then reads data under a prefix, and verifies the result.
pub async fn run_test_batch_from_blank<C: LocalRestrictedKeyValueStore>(
    key_value_store: &C,
    key_prefix: Vec<u8>,
    batch: Batch,
) {
    let kv_state = realize_batch(&batch);
    key_value_store.write_batch(batch).await.unwrap();
    // Checking the consistency
    let key_values = read_key_values_prefix(key_value_store, &key_prefix).await;
    assert_eq!(key_values, kv_state);
}

/// Run many operations on batches always starting from a blank state.
pub async fn run_writes_from_blank<C: LocalRestrictedKeyValueStore>(key_value_store: &C) {
    let mut rng = make_deterministic_rng();
    let n_oper = 10;
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

/// Reading many keys at a time could trigger an error. This needs to be tested.
pub async fn big_read_multi_values<C: LocalKeyValueStore>(
    config: C::Config,
    value_size: usize,
    n_entries: usize,
) {
    let mut rng = make_deterministic_rng();
    let namespace = generate_test_namespace();
    let root_key = &[];
    //
    let store = C::recreate_and_connect(&config, &namespace, root_key)
        .await
        .unwrap();
    let key_prefix = vec![42, 54];
    let mut batch = Batch::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for i in 0..n_entries {
        let mut key = key_prefix.clone();
        bcs::serialize_into(&mut key, &i).unwrap();
        let value = get_random_byte_vector(&mut rng, &[], value_size);
        batch.put_key_value_bytes(key.clone(), value.clone());
        keys.push(key);
        values.push(Some(value));
    }
    store.write_batch(batch).await.unwrap();
    // We reconnect so that the read is not using the cache.
    let store = C::connect(&config, &namespace, root_key).await.unwrap();
    let values_read = store.read_multi_values_bytes(keys).await.unwrap();
    assert_eq!(values, values_read);
}

/// That test is especially challenging for ScyllaDB.
/// In its default settings, Scylla has a limitation to 10000 tombstones.
/// A tombstone is an indication that the data has been deleted. That
/// is thus a trie data structure for checking whether a requested key
/// is deleted or not.
///
/// In this test we insert 200000 keys into the database.
/// Then we select half of them at random and delete them. By the random
/// selection, Scylla is forced to introduce around 100000 tombstones
/// which triggers the crash with the default settings.
pub async fn tombstone_triggering_test<C: LocalRestrictedKeyValueStore>(key_value_store: C) {
    let mut rng = make_deterministic_rng();
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
    key_value_store.write_batch(batch_delete).await.unwrap();
    // Reading everything and seeing that it is now cleaned.
    let key_values = read_key_values_prefix(&key_value_store, &key_prefix).await;
    assert_eq!(key_values, remaining_key_values);
}

/// DynamoDb has limits at 1M (for pagination), 4M (for write)
/// Let us go right past them at 20M of data with writing and then
/// reading it. And 20M is not huge by any mean. All KeyValueStore
/// must handle that.
///
/// The size of the value vary as each size has its own issues.
pub async fn run_big_write_read<C: LocalRestrictedKeyValueStore>(
    key_value_store: C,
    target_size: usize,
    value_sizes: Vec<usize>,
) {
    let mut rng = make_deterministic_rng();
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

type StateBatch = (Vec<(Vec<u8>, Vec<u8>)>, Batch);

async fn run_test_batch_from_state<C: LocalRestrictedKeyValueStore>(
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
    key_value_store.write_batch(batch_insert).await.unwrap();
    update_state_from_batch(&mut kv_state, &batch);
    key_value_store.write_batch(batch).await.unwrap();
    let key_values = read_key_values_prefix(key_value_store, &key_prefix).await;
    assert_eq!(key_values, kv_state);
}

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

/// Run some deterministic and random batches operation and check their
/// correctness
pub async fn run_writes_from_state<C: LocalRestrictedKeyValueStore>(key_value_store: &C) {
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

async fn namespaces_with_prefix<S: LocalKeyValueStore>(
    config: &S::Config,
    prefix: &str,
) -> BTreeSet<String>
where
    S::Error: Debug,
{
    let namespaces = S::list_all(config).await.expect("namespaces");
    namespaces
        .into_iter()
        .filter(|x| x.starts_with(prefix))
        .collect::<BTreeSet<_>>()
}

/// Exercises the functionalities of the `AdminKeyValueStore`.
/// This tests everything except the `delete_all` which would
/// interact with other namespaces.
pub async fn admin_test<S: TestKeyValueStore>()
where
    S::Error: Debug,
{
    let config = S::new_test_config().await.expect("config");
    let prefix = generate_test_namespace();
    let namespaces = namespaces_with_prefix::<S>(&config, &prefix).await;
    assert_eq!(namespaces.len(), 0);
    let mut rng = make_deterministic_rng();
    let size = 9;
    // Creating the initial list of namespaces
    let mut working_namespaces = BTreeSet::new();
    for i in 0..size {
        let namespace = format!("{}_{}", prefix, i);
        assert!(!S::exists(&config, &namespace).await.expect("test"));
        working_namespaces.insert(namespace);
    }
    // Creating the namespaces
    for namespace in &working_namespaces {
        S::create(&config, namespace)
            .await
            .expect("creation of a namespace");
        assert!(S::exists(&config, namespace).await.expect("test"));
    }
    // Connecting to all of them at once
    {
        let mut connections = Vec::new();
        let root_key = &[];
        for namespace in &working_namespaces {
            let connection = S::connect(&config, namespace, root_key)
                .await
                .expect("a connection to the namespace");
            connections.push(connection);
        }
    }
    // Listing all of them
    let namespaces = namespaces_with_prefix::<S>(&config, &prefix).await;
    assert_eq!(namespaces, working_namespaces);
    // Selecting at random some for deletion
    let mut kept_namespaces = BTreeSet::new();
    for namespace in working_namespaces {
        let delete = rng.gen::<bool>();
        if delete {
            S::delete(&config, &namespace)
                .await
                .expect("A successful deletion");
            assert!(!S::exists(&config, &namespace).await.expect("test"));
        } else {
            kept_namespaces.insert(namespace);
        }
    }
    for namespace in &kept_namespaces {
        assert!(S::exists(&config, namespace).await.expect("test"));
    }
    let namespaces = namespaces_with_prefix::<S>(&config, &prefix).await;
    assert_eq!(namespaces, kept_namespaces);
    for namespace in kept_namespaces {
        S::delete(&config, &namespace)
            .await
            .expect("A successful deletion");
    }
}
