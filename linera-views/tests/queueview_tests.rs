// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    memory::create_test_context,
    queue_view::QueueView,
    views::{CryptoHashRootView, RootView, View},
};
use rand::{Rng, SeedableRng};

#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub queue: QueueView<C, u8>,
}

#[tokio::test]
async fn queue_view_mutability_check() {
    let context = create_test_context().await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let mut vector = Vec::new();
    let n = 20;
    for _ in 0..n {
        let mut view = StateView::load(context.clone()).await.unwrap();
        let save = rng.gen::<bool>();
        let elements = view.queue.elements().await.unwrap();
        assert_eq!(elements, vector);
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_vector = vector.clone();
        for _ in 0..count_oper {
            let thr = rng.gen_range(0..5);
            let count = view.queue.count();
            if thr == 0 {
                // inserting random stuff
                let n_ins = rng.gen_range(0..10);
                for _ in 0..n_ins {
                    let val = rng.gen::<u8>();
                    view.queue.push_back(val);
                    new_vector.push(val);
                }
            }
            if thr == 1 {
                // deleting some entries
                if count > 0 {
                    let n_remove = rng.gen_range(0..count);
                    for _ in 0..n_remove {
                        view.queue.delete_front();
                        // slow but we do not care for tests.
                        new_vector.remove(0);
                    }
                }
            }
            if thr == 2 && count > 0 {
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
            if thr == 3 {
                // Doing the clearing
                view.clear();
                new_vector.clear();
            }
            if thr == 4 {
                // Doing the rollback
                view.rollback();
                new_vector = vector.clone();
            }
            let new_elements = view.queue.elements().await.unwrap();
            assert_eq!(new_elements, new_vector);
        }
        if save {
            vector = new_vector.clone();
            view.save().await.unwrap();
        }
    }
}
