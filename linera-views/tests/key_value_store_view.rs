// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use anyhow::Result;
use linera_views::{
    key_value_store_view::{KeyValueStoreView, SizeData},
    memory::create_memory_context,
    test_utils,
    views::{CryptoHashRootView, RootView, View},
};
use rand::{distributions::Uniform, Rng};

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub store: KeyValueStoreView<C>,
}

fn remove_by_prefix<V: Debug>(map: &mut BTreeMap<Vec<u8>, V>, key_prefix: Vec<u8>) {
    map.retain(|key, _| !key.starts_with(&key_prefix));
}

fn total_size(vec: &Vec<(Vec<u8>, Vec<u8>)>) -> SizeData {
    let mut total_key_size = 0;
    let mut total_value_size = 0;
    for (key, value) in vec {
        total_key_size += key.len();
        total_value_size += value.len();
    }
    SizeData {
        key: total_key_size as u32,
        value: total_value_size as u32,
    }
}

#[tokio::test]
async fn key_value_store_view_mutability() -> Result<()> {
    let context = create_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut state_map = BTreeMap::new();
    let n = 200;
    let mut all_keys = BTreeSet::new();
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await?;
        let save = rng.gen::<bool>();
        let read_state = view.store.index_values().await?;
        let state_vec = state_map.clone().into_iter().collect::<Vec<_>>();
        assert!(read_state.iter().map(|kv| (&kv.0, &kv.1)).eq(&state_map));
        assert_eq!(total_size(&state_vec), view.store.total_size());

        let count_oper = rng.gen_range(0..25);
        let mut new_state_map = state_map.clone();
        let mut new_state_vec = state_vec.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..5);
            let entry_count = view.store.count().await?;
            if choice == 0 {
                // inserting random stuff
                let n_ins = rng.gen_range(0..10);
                for _ in 0..n_ins {
                    let len = rng.gen_range(1..6);
                    let key = (&mut rng)
                        .sample_iter(Uniform::from(0..4))
                        .take(len)
                        .collect::<Vec<_>>();
                    all_keys.insert(key.clone());
                    let value = Vec::new();
                    view.store.insert(key.clone(), value.clone()).await?;
                    new_state_map.insert(key, value);

                    new_state_vec = new_state_map.clone().into_iter().collect();
                    let new_key_values = view.store.index_values().await?;
                    assert_eq!(new_state_vec, new_key_values);
                    assert_eq!(total_size(&new_state_vec), view.store.total_size());
                }
            }
            if choice == 1 && entry_count > 0 {
                // deleting some entries
                let n_remove = rng.gen_range(0..entry_count);
                for _ in 0..n_remove {
                    let pos = rng.gen_range(0..entry_count);
                    let (key, _) = new_state_vec[pos].clone();
                    new_state_map.remove(&key);
                    view.store.remove(key).await?;
                }
            }
            if choice == 2 && entry_count > 0 {
                // deleting a prefix
                let val = rng.gen_range(0..5) as u8;
                let key_prefix = vec![val];
                view.store.remove_by_prefix(key_prefix.clone()).await?;
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
                assert!(!view.has_pending_changes().await);
                new_state_map = state_map.clone();
            }
            new_state_vec = new_state_map.clone().into_iter().collect();
            let new_key_values = view.store.index_values().await?;
            assert_eq!(new_state_vec, new_key_values);
            assert_eq!(total_size(&new_state_vec), view.store.total_size());
            let all_keys_vec = all_keys.clone().into_iter().collect::<Vec<_>>();
            let tests_multi_get = view.store.multi_get(all_keys_vec).await?;
            for (i, key) in all_keys.clone().into_iter().enumerate() {
                let test_map = new_state_map.contains_key(&key);
                let test_view = view.store.get(&key).await?.is_some();
                let test_multi_get = tests_multi_get[i].is_some();
                assert_eq!(test_map, test_view);
                assert_eq!(test_map, test_multi_get);
            }
        }
        if save {
            if state_map != new_state_map {
                assert!(view.has_pending_changes().await);
            }
            state_map = new_state_map.clone();
            view.save().await?;
            assert!(!view.has_pending_changes().await);
        }
    }
    Ok(())
}
