// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    memory::create_memory_context,
    queue_view::HashedQueueView,
    test_utils,
    views::{CryptoHashRootView, CryptoHashView, RootView, View},
};
use rand::Rng as _;

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub queue: HashedQueueView<C, u8>,
}

#[tokio::test]
async fn queue_view_mutability_check() {
    let context = create_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut vector = Vec::new();
    let n = 20;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let hash = view.crypto_hash().await.unwrap();
        let save = rng.gen::<bool>();
        let elements = view.queue.elements().await.unwrap();
        assert_eq!(elements, vector);
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_vector = vector.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..5);
            let count = view.queue.count();
            if choice == 0 {
                // inserting random stuff
                let n_ins = rng.gen_range(0..10);
                for _ in 0..n_ins {
                    let val = rng.gen::<u8>();
                    view.queue.push_back(val);
                    new_vector.push(val);
                }
            }
            if choice == 1 {
                // deleting some entries
                let n_remove = rng.gen_range(0..=count);
                for _ in 0..n_remove {
                    view.queue.delete_front();
                    // slow but we do not care for tests.
                    new_vector.remove(0);
                }
            }
            if choice == 2 && count > 0 {
                // changing some random entries
                let pos = rng.gen_range(0..count);
                let val = rng.gen::<u8>();
                let mut iter = view.queue.iter_mut().await.unwrap();
                (for _ in 0..pos {
                    iter.next();
                });
                if let Some(value) = iter.next() {
                    *value = val;
                }
                if let Some(value) = new_vector.get_mut(pos) {
                    *value = val;
                }
            }
            if choice == 3 {
                // Doing the clearing
                view.clear();
                new_vector.clear();
            }
            if choice == 4 {
                // Doing the rollback
                view.rollback();
                assert!(!view.has_pending_changes().await);
                new_vector.clone_from(&vector);
            }
            let new_elements = view.queue.elements().await.unwrap();
            let new_hash = view.crypto_hash().await.unwrap();
            if elements == new_elements {
                assert_eq!(new_hash, hash);
            } else {
                // If equal it is a bug or a hash collision (unlikely)
                assert_ne!(new_hash, hash);
            }
            assert_eq!(new_elements, new_vector);
        }
        if save {
            if vector != new_vector {
                assert!(view.has_pending_changes().await);
            }
            vector.clone_from(&new_vector);
            view.save().await.unwrap();
            assert!(!view.has_pending_changes().await);
        }
    }
}
