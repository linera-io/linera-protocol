// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::Rng;
use std::collections::HashSet;

pub fn random_shuffle<T: Clone>(l_val: &mut Vec<T>) {
    let mut rng = rand::thread_rng();
    let n = l_val.len();
    for _ in 0..4 * n {
        let idx1: usize = rng.gen_range(0..n);
        let idx2: usize = rng.gen_range(0..n);
        if idx1 != idx2 {
            let val1 = l_val.get(idx1).unwrap().clone();
            let val2 = l_val.get(idx2).unwrap().clone();
            l_val[idx1] = val2;
            l_val[idx2] = val1;
        }
    }
}

pub fn get_random_vec_bytes(n: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut v = Vec::new();
    for _ in 0..n {
        let val = rng.gen_range(0..256) as u8;
        v.push(val);
    }
    v
}

pub fn get_random_vec_keyvalues(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    loop {
        let mut v_ret = Vec::new();
        let mut set_vect = HashSet::new();
        for _ in 0..n {
            let v1 = get_random_vec_bytes(8);
            let v2 = get_random_vec_bytes(8);
            let v12 = (v1.clone(), v2);
            set_vect.insert(v1);
            v_ret.push(v12);
        }
        if set_vect.len() == n {
            return v_ret;
        }
    }
}
