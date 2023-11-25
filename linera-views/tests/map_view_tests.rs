// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    map_view::ByteMapView,
    memory::create_memory_context,
    views::{CryptoHashRootView, RootView, View},
};
use rand::{distributions::Uniform, Rng, RngCore, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub map: ByteMapView<C, u8>,
}

fn remove_by_prefix<V>(map: &mut BTreeMap<Vec<u8>, V>, key_prefix: Vec<u8>) {
    map.retain(|key, _| !key.starts_with(&key_prefix));
}

async fn run_map_view_mutability<R: RngCore + Clone>(rng: &mut R) {
    let context = create_memory_context();
    let mut state_map = BTreeMap::new();
    let mut all_keys = BTreeSet::new();
    let n = 200;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let save = rng.gen::<bool>();
        let read_state = view.map.key_values().await.unwrap();
        let state_vec = state_map.clone().into_iter().collect::<Vec<_>>();
        assert_eq!(state_vec, read_state);
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_state_map = state_map.clone();
        let mut new_state_vec = state_vec.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..5);
            let count = view.map.count().await.unwrap();
            if choice == 0 {
                // inserting random stuff
                let n_ins = rng.gen_range(0..10);
                for _ in 0..n_ins {
                    let len = rng.gen_range(1..6);
                    let key = rng
                        .clone()
                        .sample_iter(Uniform::from(0..4))
                        .take(len)
                        .collect::<Vec<_>>();
                    all_keys.insert(key.clone());
                    let value = rng.gen::<u8>();
                    view.map.insert(key.clone(), value);
                    new_state_map.insert(key, value);
                }
            }
            if choice == 1 && count > 0 {
                // deleting some entries
                let n_remove = rng.gen_range(0..count);
                for _ in 0..n_remove {
                    let pos = rng.gen_range(0..count);
                    let vec = new_state_vec[pos].clone();
                    view.map.remove(vec.0.clone());
                    new_state_map.remove(&vec.0);
                }
            }
            if choice == 2 && count > 0 {
                // deleting a prefix
                let val = rng.gen_range(0..5) as u8;
                let key_prefix = vec![val];
                view.map.remove_by_prefix(key_prefix.clone());
                remove_by_prefix(&mut new_state_map, key_prefix);
            }
            if choice == 3 {
                // Doing the clearing
                view.clear();
                new_state_map.clear();
            }
            if choice == 4 {
                // Doing the rollback
                view.rollback();
                new_state_map = state_map.clone();
            }
            new_state_vec = new_state_map.clone().into_iter().collect();
            let new_key_values = view.map.key_values().await.unwrap();
            assert_eq!(new_state_vec, new_key_values);
            for u in 0..4 {
                let part_state_vec = new_state_vec
                    .iter()
                    .filter(|&x| x.0[0] == u)
                    .cloned()
                    .collect::<Vec<_>>();
                let part_key_values = view.map.key_values_by_prefix(vec![u]).await.unwrap();
                assert_eq!(part_state_vec, part_key_values);
            }
            for key in &all_keys {
                let test_map = new_state_map.contains_key(key);
                let test_view = view.map.get(key).await.unwrap().is_some();
                assert_eq!(test_map, test_view);
            }
        }
        if save {
            state_map = new_state_map.clone();
            view.save().await.unwrap();
        }
    }
}

#[tokio::test]
async fn map_view_mutability() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for _ in 0..10 {
        run_map_view_mutability(&mut rng).await;
    }
}
