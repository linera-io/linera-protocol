// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    memory::create_test_context,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    views::{CryptoHashRootView, RootView, View},
};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub v: ReentrantCollectionView<C, u8, RegisterView<C, u32>>,
}

#[tokio::test]
async fn reentrant_collection_view_check() {
    let context = create_test_context();
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let save = rng.gen::<bool>();
        //
        let count_oper = rng.gen_range(0, 25);
        let mut new_map = map.clone();
        for _ in 0..count_oper {
            let thr = rng.gen_range(0, 5);
            if thr == 0 {
                // deleting random stuff
                let pos = rng.gen_range(0, nmax);
                view.v.remove_entry(&pos).unwrap();
                new_map.remove(&pos);
            }
            if thr == 1 {
                // getting an array of reference
                let mut indices = Vec::new();
                let mut set_indices = BTreeSet::new();
                let mut values = Vec::new();
                let n_ins = rng.gen_range(0, 5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0, nmax);
                    indices.push(pos);
                    set_indices.insert(pos);
                    let value = rng.gen::<u32>();
                    values.push(value);
                }
                // Only if all indices are distinct can the query
                if set_indices.len() == n_ins {
                    let mut subviews = view.v.try_load_entries_mut(indices.clone()).await.unwrap();
                    for i in 0..n_ins {
                        let index = indices[i];
                        let value = values[i];
                        *subviews[i].get_mut() = value;
                        new_map.insert(index, value);
                    }
                }
            }
            if thr == 2 {
                // changing some random entries
                let n_ins = rng.gen_range(0, 5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0, nmax);
                    let value = rng.gen::<u32>();
                    let mut subview = view.v.try_load_entry_mut(&pos).await.unwrap();
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if thr == 3 {
                // Doing the clearing
                view.clear();
                new_map.clear();
            }
            if thr == 4 {
                // Doing the rollback
                view.rollback();
                new_map = map.clone();
            }
            // Checking the keys
            let keys_view = view.v.indices().await.unwrap();
            let keys_view = keys_view.into_iter().collect::<BTreeSet<_>>();
            let keys_map = new_map.clone().into_keys().collect::<BTreeSet<_>>();
            assert_eq!(keys_view, keys_map);
            // Checking the try_load_entries on all indices
            let indices = keys_map.clone().into_iter().collect::<Vec<_>>();
            let subviews = view.v.try_load_entries(indices.clone()).await.unwrap();
            for i in 0..keys_map.len() {
                let index: u8 = indices[i];
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
