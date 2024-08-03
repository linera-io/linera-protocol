// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use linera_views::{
    collection_view::HashedCollectionView,
    common::Context,
    key_value_store_view::{KeyValueStoreView, SizeData},
    map_view::HashedByteMapView,
    memory::create_test_memory_context,
    queue_view::HashedQueueView,
    reentrant_collection_view::HashedReentrantCollectionView,
    register_view::RegisterView,
    test_utils,
    views::{CryptoHashRootView, CryptoHashView, RootView, View, ViewError},
};
use rand::{distributions::Uniform, Rng, RngCore};

#[derive(CryptoHashRootView)]
struct CollectionStateView<C> {
    pub v: HashedCollectionView<C, u8, RegisterView<C, u32>>,
}

impl<C> CollectionStateView<C>
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
async fn classic_collection_view_check() -> Result<()> {
    let context = create_test_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = CollectionStateView::load(context.clone()).await?;
        let hash = view.crypto_hash().await?;
        let save = rng.gen::<bool>();
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..6);
            if choice == 0 {
                // deleting random stuff
                let pos = rng.gen_range(0..nmax);
                view.v.remove_entry(&pos)?;
                new_map.remove(&pos);
            }
            if choice == 1 {
                // changing some random entries
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let value = rng.gen::<u32>();
                    let subview = view.v.load_entry_mut(&pos).await?;
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if choice == 2 {
                // The load_entry actually changes the entries to default if missing
                let n_load = rng.gen_range(0..5);
                for _i in 0..n_load {
                    let pos = rng.gen_range(0..nmax);
                    let _subview = view.v.load_entry_mut(&pos).await?;
                    new_map.entry(pos).or_insert(0);
                }
            }
            if choice == 3 {
                // The load_entry actually changes the entries to default if missing
                let n_reset = rng.gen_range(0..5);
                for _i in 0..n_reset {
                    let pos = rng.gen_range(0..nmax);
                    view.v.reset_entry_to_default(&pos)?;
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
            let new_hash = view.crypto_hash().await?;
            if map == new_map {
                assert_eq!(new_hash, hash);
            } else {
                assert_ne!(new_hash, hash);
            }
            // Checking the behavior of "try_load_entry"
            for _ in 0..10 {
                let pos = rng.gen::<u8>();
                let test_view = view.v.try_load_entry(&pos).await?.is_some();
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
            view.save().await?;
            assert!(!view.has_pending_changes().await);
        }
    }
    Ok(())
}

#[derive(CryptoHashRootView)]
pub struct KeyValueStateView<C> {
    pub store: KeyValueStoreView<C>,
}

fn remove_by_prefix<V>(map: &mut BTreeMap<Vec<u8>, V>, key_prefix: Vec<u8>) {
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
    let context = create_test_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut state_map = BTreeMap::new();
    let n = 40;
    let mut all_keys = BTreeSet::new();
    for _ in 0..n {
        let mut view = KeyValueStateView::load(context.clone()).await?;
        let save = rng.gen::<bool>();
        let read_state = view.store.index_values().await?;
        let state_vec = state_map.clone().into_iter().collect::<Vec<_>>();
        assert!(read_state.iter().map(|kv| (&kv.0, &kv.1)).eq(&state_map));
        assert_eq!(total_size(&state_vec), view.store.total_size());

        let count_oper = rng.gen_range(0..15);
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

#[derive(CryptoHashRootView)]
pub struct ByteMapStateView<C> {
    pub map: HashedByteMapView<C, u8>,
}

async fn run_map_view_mutability<R: RngCore + Clone>(rng: &mut R) -> Result<()> {
    let context = create_test_memory_context();
    let mut state_map = BTreeMap::new();
    let mut all_keys = BTreeSet::new();
    let n = 10;
    for _ in 0..n {
        let mut view = ByteMapStateView::load(context.clone()).await?;
        let save = rng.gen::<bool>();
        let read_state = view.map.key_values().await?;
        let read_hash = view.crypto_hash().await?;
        let state_vec = state_map.clone().into_iter().collect::<Vec<_>>();
        assert_eq!(state_vec, read_state);
        //
        let count_oper = rng.gen_range(0..25);
        let mut new_state_map = state_map.clone();
        let mut new_state_vec = state_vec.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..7);
            let count = view.map.count().await?;
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
                assert!(!view.has_pending_changes().await);
                new_state_map = state_map.clone();
            }
            if choice == 5 && count > 0 {
                let pos = rng.gen_range(0..count);
                let vec = new_state_vec[pos].clone();
                let key = vec.0;
                let result = view.map.get_mut(&key).await?.unwrap();
                let new_value = rng.gen::<u8>();
                *result = new_value;
                new_state_map.insert(key, new_value);
            }
            if choice == 6 && count > 0 {
                let choice = rng.gen_range(0..count);
                let key = match choice {
                    0 => {
                        // Scenario 1 of using existing key
                        let pos = rng.gen_range(0..count);
                        let vec = new_state_vec[pos].clone();
                        vec.0
                    }
                    _ => {
                        let len = rng.gen_range(1..6);
                        rng.clone()
                            .sample_iter(Uniform::from(0..4))
                            .take(len)
                            .collect::<Vec<_>>()
                    }
                };
                let test_view = view.map.contains_key(&key).await?;
                let test_map = new_state_map.contains_key(&key);
                assert_eq!(test_view, test_map);
                let result = view.map.get_mut_or_default(&key).await?;
                let new_value = rng.gen::<u8>();
                *result = new_value;
                new_state_map.insert(key, new_value);
            }
            new_state_vec = new_state_map.clone().into_iter().collect();
            let new_hash = view.crypto_hash().await?;
            if state_vec == new_state_vec {
                assert_eq!(new_hash, read_hash);
            } else {
                // Hash equality is a bug or a hash collision (unlikely)
                assert_ne!(new_hash, read_hash);
            }
            let new_key_values = view.map.key_values().await?;
            assert_eq!(new_state_vec, new_key_values);
            for u in 0..4 {
                let part_state_vec = new_state_vec
                    .iter()
                    .filter(|&x| x.0[0] == u)
                    .cloned()
                    .collect::<Vec<_>>();
                let part_key_values = view.map.key_values_by_prefix(vec![u]).await?;
                assert_eq!(part_state_vec, part_key_values);
            }
            let keys_vec = all_keys.iter().cloned().collect::<Vec<_>>();
            let values = view.map.multi_get(keys_vec.clone()).await?;
            for i in 0..keys_vec.len() {
                let key = &keys_vec[i];
                let test_map = new_state_map.contains_key(key);
                let test_view1 = view.map.get(key).await?.is_some();
                let test_view2 = view.map.contains_key(key).await?;
                assert_eq!(test_map, test_view1);
                assert_eq!(test_map, test_view2);
                assert_eq!(test_map, values[i].is_some());
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

#[tokio::test]
async fn map_view_mutability() -> Result<()> {
    let mut rng = test_utils::make_deterministic_rng();
    for _ in 0..5 {
        run_map_view_mutability(&mut rng).await?;
    }
    Ok(())
}

#[derive(CryptoHashRootView)]
pub struct QueueStateView<C> {
    pub queue: HashedQueueView<C, u8>,
}

#[tokio::test]
async fn queue_view_mutability_check() -> Result<()> {
    let context = create_test_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut vector = Vec::new();
    let n = 20;
    for _ in 0..n {
        let mut view = QueueStateView::load(context.clone()).await?;
        let hash = view.crypto_hash().await?;
        let save = rng.gen::<bool>();
        let elements = view.queue.elements().await?;
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
                let mut iter = view.queue.iter_mut().await?;
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
            let new_elements = view.queue.elements().await?;
            let new_hash = view.crypto_hash().await?;
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
            view.save().await?;
            assert!(!view.has_pending_changes().await);
        }
    }
    Ok(())
}

#[derive(CryptoHashRootView)]
struct ReentrantCollectionStateView<C> {
    pub v: HashedReentrantCollectionView<C, u8, RegisterView<C, u32>>,
}

impl<C> ReentrantCollectionStateView<C>
where
    C: Send + Context + Sync,
    ViewError: From<C::Error>,
{
    async fn key_values(&self) -> Result<BTreeMap<u8, u32>> {
        let mut map = BTreeMap::new();
        let keys = self.v.indices().await?;
        for key in keys {
            let subview = self.v.try_load_entry(&key).await?.unwrap();
            let value = subview.get();
            map.insert(key, *value);
        }
        Ok(map)
    }
}

#[tokio::test]
async fn reentrant_collection_view_check() -> Result<()> {
    let context = create_test_memory_context();
    let mut rng = test_utils::make_deterministic_rng();
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let mut view = ReentrantCollectionStateView::load(context.clone()).await?;
        let hash = view.crypto_hash().await?;
        let key_values = view.key_values().await?;
        assert_eq!(key_values, map);
        //
        let save = rng.gen::<bool>();
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _i_op in 0..count_oper {
            let choice = rng.gen_range(0..8);
            if choice == 0 {
                // Deleting some random stuff
                let pos = rng.gen_range(0..nmax);
                view.v.remove_entry(&pos)?;
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
                    let mut subviews = view.v.try_load_entries_mut(&indices).await?;
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
                    let mut subview = view.v.try_load_entry_mut(&pos).await?;
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if choice == 3 {
                // Loading some random entries and setting to 0 if missing
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let test_view = view.v.contains_key(&pos).await?;
                    let test_map = new_map.contains_key(&pos);
                    assert_eq!(test_view, test_map);
                    let _subview = view.v.try_load_entry_mut(&pos).await?;
                    new_map.entry(pos).or_insert(0);
                }
            }
            if choice == 4 {
                // Loading some random entries and checking correctness
                let n_ins = rng.gen_range(0..5);
                for _i_ins in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let subview = view.v.try_load_entry(&pos).await?;
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
                assert!(!view.has_pending_changes().await);
                new_map = map.clone();
            }
            if choice == 7 {
                // The load_entry actually changes the entries to default if missing
                let n_reset = rng.gen_range(0..5);
                for _i in 0..n_reset {
                    let pos = rng.gen_range(0..nmax);
                    view.v.try_reset_entry_to_default(&pos)?;
                    new_map.insert(pos, 0);
                }
            }
            // Checking the hash
            let new_hash = view.crypto_hash().await?;
            if new_map == map {
                assert_eq!(hash, new_hash);
            } else {
                // Inequality could be a bug or a hash collision (unlikely)
                assert_ne!(hash, new_hash);
            }
            // Checking the keys
            let key_values = view.key_values().await?;
            assert_eq!(key_values, new_map);
            // Checking the try_load_entries on all indices
            let indices = key_values.into_iter().map(|x| x.0).collect::<Vec<_>>();
            let subviews = view.v.try_load_entries(&indices).await?;
            for i in 0..indices.len() {
                let index = indices[i];
                let subview = subviews[i].as_ref().unwrap();
                let value_view = *subview.get();
                let value_map = match new_map.get(&index) {
                    None => 0,
                    Some(value) => *value,
                };
                assert_eq!(value_view, value_map);
            }
        }
        if save {
            if map != new_map {
                assert!(view.has_pending_changes().await);
            }
            map = new_map.clone();
            view.save().await?;
            assert!(!view.has_pending_changes().await);
        }
    }
    Ok(())
}
