// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    memory::create_memory_context,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::{CryptoHashRootView, CryptoHashView, RootView, View, ViewError},
};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

#[derive(CryptoHashRootView)]
struct StateView<C> {
    pub v: ReentrantCollectionView<C, u8, RegisterView<C, u32>>,
}

impl<C> StateView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    async fn key_values(&self) -> BTreeMap<u8, u32> {
        let mut map = BTreeMap::new();
        let keys = self.v.indices().await.unwrap();
        for key in keys {
            let subview = self.v.try_load_entry(&key).await.unwrap().unwrap();
            let value = subview.get();
            map.insert(key, *value);
        }
        map
    }
}

#[tokio::test]
async fn reentrant_collection_view_check() {
    let context = create_memory_context();
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let hash = view.crypto_hash().await.unwrap();
        let key_values = view.key_values().await;
        assert_eq!(key_values, map);
        //
        let save = rng.gen::<bool>();
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _i_op in 0..count_oper {
            let choice = rng.gen_range(0..7);
            if choice == 0 {
                // Deleting some random stuff
                let pos = rng.gen_range(0..nmax);
                view.v.remove_entry(&pos).unwrap();
                new_map.remove(&pos);
            }
            if choice == 1 {
                // Getting an array of reference
                let mut indices = Vec::new();
                let mut set_indices = BTreeSet::new();
                let mut values = Vec::new();
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    indices.push(pos);
                    set_indices.insert(pos);
                    let value = rng.gen::<u32>();
                    values.push(value);
                }
                // Only if all indices are distinct can the query be acceptable
                if set_indices.len() == n_ins {
                    let mut subviews = view.v.try_load_entries_mut(&indices).await.unwrap();
                    for i in 0..n_ins {
                        let index = indices[i];
                        let value = values[i];
                        *subviews[i].get_mut() = value;
                        new_map.insert(index, value);
                    }
                }
            }
            if choice == 2 {
                // Changing some random entries
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let value = rng.gen::<u32>();
                    let mut subview = view.v.try_load_entry_mut(&pos).await.unwrap();
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if choice == 3 {
                // Loading some random entries and setting to 0 if missing
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let test_view = view.v.contains_key(&pos).await.unwrap();
                    let test_map = new_map.contains_key(&pos);
                    assert_eq!(test_view, test_map);
                    let _subview = view.v.try_load_entry_or_insert(&pos).await.unwrap();
                    new_map.entry(pos).or_insert(0);
                }
            }
            if choice == 4 {
                // Loading some random entries and checking correctness
                let n_ins = rng.gen_range(0..5);
                for _i_ins in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let subview: Option<_> = view.v.try_load_entry(&pos).await.unwrap();
                    match new_map.contains_key(&pos) {
                        true => {
                            let subview = subview.unwrap();
                            let value = subview.get();
                            assert_eq!(value, new_map.get(&pos).unwrap());
                        }
                        false => assert!(subview.is_none()),
                    }
                }
            }
            if choice == 5 {
                // Doing the clearing
                view.clear();
                new_map.clear();
            }
            if choice == 6 {
                // Doing the rollback
                view.rollback();
                new_map = map.clone();
            }
            // Checking the hash
            let new_hash = view.crypto_hash().await.unwrap();
            if new_map == map {
                assert_eq!(hash, new_hash);
            } else {
                // Inequality could be a bug or a hash collision (unlikely)
                assert_ne!(hash, new_hash);
            }
            // Checking the keys
            let key_values = view.key_values().await;
            assert_eq!(key_values, new_map);
            // Checking the try_load_entries on all indices
            let indices = key_values.into_iter().map(|x| x.0).collect::<Vec<_>>();
            let subviews = view.v.try_load_entries(&indices).await.unwrap();
            for i in 0..indices.len() {
                let index = indices[i];
                let value_view = *subviews[i].get();
                let value_map = match new_map.get(&index) {
                    None => 0,
                    Some(value) => *value,
                };
                assert_eq!(value_view, value_map);
            }
        }
        if save {
            map = new_map.clone();
            view.save().await.unwrap();
        }
    }
}
