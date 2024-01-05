// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    memory::create_memory_context,
    collection_view::CollectionView,
    register_view::RegisterView,
    views::{CryptoHashRootView, RootView, View},
};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub v: CollectionView<C, u8, RegisterView<C, u32>>,
}

#[tokio::test]
async fn collection_view_check() {
    let context = create_memory_context();
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let save = rng.gen::<bool>();
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..4);
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
                // Doing the clearing
                view.clear();
                new_map.clear();
            }
            if choice == 3 {
                // Doing the rollback
                view.rollback();
                new_map = map.clone();
            }
            // Checking the keys
            let keys_view = view.v.indices().await.unwrap();
            let keys_view = keys_view.into_iter().collect::<BTreeSet<_>>();
            let keys_map = new_map.clone().into_keys().collect::<BTreeSet<_>>();
            assert_eq!(keys_view, keys_map);
        }
        if save {
            map = new_map.clone();
            view.save().await.unwrap();
        }
    }
}
