// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::batch::{
    WriteOperation,
    WriteOperation::{Delete, Put},
};

use rand::{Rng, RngCore};
use std::collections::HashSet;
use tracing::warn;

fn generate_random_alphanumeric_string(length: usize) -> String {
    // Define the characters that are allowed in the alphanumeric string
    let charset: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    let mut rng = rand::thread_rng();
    let alphanumeric_string: String = (0..length)
        .map(|_| {
            let random_index = rng.gen_range(0..charset.len());
            charset[random_index] as char
        })
        .collect();

    alphanumeric_string
}

/// Returns a unique table name for testing.
pub fn get_table_name() -> String {
    let entry = generate_random_alphanumeric_string(20);
    let table_name = format!("table_{}", entry);
    warn!("Generating table_name={}", table_name);
    table_name
}

/// Returns a random key_prefix used for tests
pub fn get_random_key_prefix() -> Vec<u8> {
    let mut key_prefix = vec![0];
    let mut rng = rand::thread_rng();
    let value: usize = rng.gen();
    bcs::serialize_into(&mut key_prefix, &value).unwrap();
    key_prefix
}

/// Shuffles the values entries randomly
pub fn random_shuffle<R: RngCore, T: Clone>(rng: &mut R, values: &mut Vec<T>) {
    let n = values.len();
    for _ in 0..4 * n {
        let index1: usize = rng.gen_range(0..n);
        let index2: usize = rng.gen_range(0..n);
        if index1 != index2 {
            let val1 = values.get(index1).unwrap().clone();
            let val2 = values.get(index2).unwrap().clone();
            values[index1] = val2;
            values[index2] = val1;
        }
    }
}

/// Takes a random number generator, a key_prefix and extends it by n random bytes.
pub fn get_random_byte_vector<R: RngCore>(
    rng: &mut R,
    key_prefix: &[u8],
    n: usize,
) -> Vec<u8> {
    let mut v = key_prefix.to_vec();
    for _ in 0..n {
        let val = rng.gen_range(0..256) as u8;
        v.push(val);
    }
    v
}

/// Appends a small value to a key making collisions likely.
pub fn get_small_key_space<R: RngCore>(
    rng: &mut R,
    key_prefix: &[u8],
    n: usize,
) -> Vec<u8> {
    let mut key = key_prefix.to_vec();
    for _ in 0..n {
        let byte = rng.gen_range(0..4) as u8;
        key.push(byte);
    }
    key
}

/// Builds a random k element subset of n
pub fn get_random_kset<R: RngCore>(rng: &mut R, n: usize, k: usize) -> Vec<usize> {
    let mut values = Vec::new();
    for u in 0..n {
        values.push(u);
    }
    random_shuffle(rng, &mut values);
    values[..k].to_vec()
}

/// Takes a random number generator, a key_prefix and generates
/// pairs `(key, value)` with key obtained by appending 8 bytes at random to key_prefix
/// and value obtained by appending 8 bytes to the trivial vector.
/// We return n such `(key, value)` pairs which are all distinct
pub fn get_random_key_values_prefix<R: RngCore>(
    rng: &mut R,
    key_prefix: Vec<u8>,
    len_key: usize,
    len_value: usize,
    n: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    loop {
        let mut v_ret = Vec::new();
        let mut vector_set = HashSet::new();
        for _ in 0..n {
            let v1 = get_random_byte_vector(rng, &key_prefix, len_key);
            let v2 = get_random_byte_vector(rng, &Vec::new(), len_value);
            let v12 = (v1.clone(), v2);
            vector_set.insert(v1);
            v_ret.push(v12);
        }
        if vector_set.len() == n {
            return v_ret;
        }
    }
}

/// Takes a random number generator rng, a number n and returns n random `(key, value)`
/// which are all distinct with key and value being of length 8.
pub fn get_random_key_values<R: RngCore>(
    rng: &mut R,
    n: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    get_random_key_values_prefix(rng, Vec::new(), 8, 8, n)
}

type VectorPutDelete = (Vec<(Vec<u8>, Vec<u8>)>, usize);

/// A bunch of puts and some deletes.
pub fn get_random_key_value_operations<R: RngCore>(
    rng: &mut R,
    n: usize,
    k: usize,
) -> VectorPutDelete {
    let key_value_vector = get_random_key_values_prefix(rng, Vec::new(), 8, 8, n);
    (key_value_vector, k)
}

/// A random reordering of the puts and deletes.
/// For something like MapView it should get us the same result whatever way we are calling.
pub fn span_random_reordering_put_delete<R: RngCore>(
    rng: &mut R,
    info_op: VectorPutDelete,
) -> Vec<WriteOperation> {
    let n = info_op.0.len();
    let k = info_op.1;
    let mut indices = Vec::new();
    for i in 0..n {
        indices.push(i);
    }
    random_shuffle(rng, &mut indices);
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
