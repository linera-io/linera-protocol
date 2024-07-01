// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use linera_views::{
    collection_view::HashedCollectionView,
    common::Context,
    memory::create_memory_context,
    register_view::RegisterView,
    test_utils,
    views::{CryptoHashRootView, CryptoHashView, RootView, View, ViewError},
};
use rand::Rng as _;

#[derive(CryptoHashRootView)]
struct StateView<C> {
    pub v: HashedCollectionView<C, u8, RegisterView<C, u32>>,
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
async fn classic_collection_view_check() {
    let context = create_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let hash = view.crypto_hash().await.unwrap();
        let save = rng.gen::<bool>();
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..6);
            if choice == 0 {
                // deleting random stuff
                let pos = rng.gen_range(0..nmax);
                view.v.remove_entry(&pos).unwrap();
                new_map.remove(&pos);
            }
            if choice == 1 {
                // changing some random entries
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let value = rng.gen::<u32>();
                    let subview = view.v.load_entry_mut(&pos).await.unwrap();
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if choice == 2 {
                // The load_entry actually changes the entries to default if missing
                let n_load = rng.gen_range(0..5);
                for _i in 0..n_load {
                    let pos = rng.gen_range(0..nmax);
                    let _subview = view.v.load_entry_or_insert(&pos).await.unwrap();
                    new_map.entry(pos).or_insert(0);
                }
            }
            if choice == 3 {
                // The load_entry actually changes the entries to default if missing
                let n_reset = rng.gen_range(0..5);
                for _i in 0..n_reset {
                    let pos = rng.gen_range(0..nmax);
                    view.v.reset_entry_to_default(&pos).await.unwrap();
                    new_map.insert(pos, 0);
                }
            }
            if choice == 4 {
                // Doing the clearing
                view.clear();
                new_map.clear();
            }
            if choice == 5 {
                // Doing the rollback
                view.rollback();
                assert!(!view.has_pending_changes().await);
                new_map = map.clone();
            }
            // Checking the hash
            let new_hash = view.crypto_hash().await.unwrap();
            if map == new_map {
                assert_eq!(new_hash, hash);
            } else {
                assert_ne!(new_hash, hash);
            }
            // Checking the behavior of "try_load_entry"
            for _ in 0..10 {
                let pos = rng.gen::<u8>();
                let test_view = view.v.try_load_entry(&pos).await.unwrap().is_some();
                let test_map = new_map.contains_key(&pos);
                assert_eq!(test_view, test_map);
            }
            // Checking the keys
            let key_values = view.key_values().await;
            assert_eq!(key_values, new_map);
        }
        if save {
            if map != new_map {
                assert!(view.has_pending_changes().await);
            }
            map = new_map.clone();
            view.save().await.unwrap();
            println!("After SAVE");
            assert!(!view.has_pending_changes().await);
        }
    }
}
