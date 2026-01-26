// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use linera_views::{
    batch::Batch,
    context::{ViewSyncContext, SyncContext as _},
    memory::SyncMemoryStore,
    random::make_deterministic_rng,
    store::SyncWritableKeyValueStore as _,
    sync_views::{
        collection_view::SyncCollectionView,
        log_view::SyncLogView,
        map_view::{SyncByteMapView, SyncMapView},
        queue_view::SyncQueueView,
        register_view::SyncRegisterView,
        set_view::SyncSetView,
        SyncRootView,
        SyncView,
    },
    ViewError,
};
use rand::{distributions::Uniform, Rng, RngCore};

type TestContext = ViewSyncContext<(), SyncMemoryStore>;

fn new_test_context() -> (SyncMemoryStore, TestContext) {
    let store = SyncMemoryStore::new_for_testing();
    let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
    (store, context)
}

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct TestView {
    counter: SyncRegisterView<TestContext, u64>,
    map: SyncMapView<TestContext, String, u64>,
}

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct FullSyncView {
    x1: SyncRegisterView<TestContext, u64>,
    x2: SyncRegisterView<TestContext, u32>,
    log: SyncLogView<TestContext, u32>,
    map: SyncMapView<TestContext, String, usize>,
    set: SyncSetView<TestContext, usize>,
    queue: SyncQueueView<TestContext, u64>,
    collection: SyncCollectionView<TestContext, String, SyncLogView<TestContext, u32>>,
    collection2: SyncCollectionView<
        TestContext,
        String,
        SyncCollectionView<TestContext, String, SyncRegisterView<TestContext, u32>>,
    >,
}

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct ByteMapStateView {
    map: SyncByteMapView<TestContext, u8>,
}

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct CollectionStateView {
    v: SyncCollectionView<TestContext, u8, SyncRegisterView<TestContext, u32>>,
}

impl CollectionStateView {
    fn key_values(&self) -> Result<BTreeMap<u8, u32>, ViewError> {
        let mut map = BTreeMap::new();
        let keys = self.v.indices()?;
        for key in keys {
            let subview = self.v.try_load_entry(&key)?.unwrap();
            let value = subview.get();
            map.insert(key, *value);
        }
        Ok(map)
    }
}

#[derive(SyncRootView)]
#[view(context = TestContext)]
struct QueueStateView {
    queue: SyncQueueView<TestContext, u8>,
}

#[test]
fn sync_view_roundtrip() -> Result<(), ViewError> {
    let (store, context) = new_test_context();

    let mut view = TestView::load(context)?;
    assert_eq!(*view.counter.get(), 0);
    assert_eq!(view.map.get("a")?, None);

    view.counter.set(42);
    view.map.insert("a", 1)?;
    view.map.insert("b", 2)?;
    view.save()?;

    let context = TestContext::new_unchecked(store, Vec::new(), ());
    let view = TestView::load(context)?;
    assert_eq!(*view.counter.get(), 42);
    assert_eq!(view.map.get("a")?, Some(1));
    assert_eq!(view.map.get("b")?, Some(2));

    Ok(())
}

#[test]
fn sync_view_basic_operations() -> Result<(), ViewError> {
    let (store, context) = new_test_context();

    let mut view = FullSyncView::load(context)?;
    assert_eq!(view.x1.get(), &0);
    assert_eq!(view.x2.get(), &0);
    assert_eq!(view.log.read(0..10)?, Vec::<u32>::new());
    assert_eq!(view.queue.read_front(10)?, Vec::<u64>::new());
    assert_eq!(view.map.get("Hello")?, None);
    assert!(!view.set.contains(&42)?);

    view.x1.set(1);
    view.x2.set(2);
    view.log.push(4);
    view.queue.push_back(7);
    view.queue.push_back(8);
    view.queue.delete_front();

    view.map.insert("Hello", 5)?;
    assert_eq!(view.map.indices()?, vec!["Hello".to_string()]);
    let mut map_count = 0;
    view.map.for_each_index(|_index| {
        map_count += 1;
        Ok(())
    })?;
    assert_eq!(map_count, 1);

    view.set.insert(&42)?;
    assert_eq!(view.set.indices()?, vec![42]);
    let mut set_count = 0;
    view.set.for_each_index(|_index| {
        set_count += 1;
        Ok(())
    })?;
    assert_eq!(set_count, 1);

    {
        let subview = view.collection.load_entry_mut("hola")?;
        subview.push(17);
        subview.push(18);
        assert_eq!(view.collection.indices()?, vec!["hola".to_string()]);
        let mut collection_count = 0;
        view.collection.for_each_index(|_index| {
            collection_count += 1;
            Ok(())
        })?;
        assert_eq!(collection_count, 1);
    }

    {
        let subview = view.collection2.load_entry_mut("ciao")?;
        let subsubview = subview.load_entry_mut("!")?;
        subsubview.set(3);
    }

    view.save()?;

    let context = TestContext::new_unchecked(store, Vec::new(), ());
    let mut view = FullSyncView::load(context)?;
    assert_eq!(view.x1.get(), &1);
    assert_eq!(view.x2.get(), &2);
    assert_eq!(view.log.read(0..10)?, vec![4]);
    assert_eq!(view.queue.read_front(10)?, vec![8]);
    assert_eq!(view.map.get("Hello")?, Some(5));
    assert!(view.set.contains(&42)?);

    let subview = view.collection.try_load_entry("hola")?.unwrap();
    assert_eq!(subview.read(0..10)?, vec![17, 18]);

    let subview = view.collection2.try_load_entry("ciao")?.unwrap();
    let subsubview = subview.try_load_entry("!")?.unwrap();
    assert_eq!(subsubview.get(), &3);

    view.queue.push_back(13);
    assert_eq!(view.queue.read_front(2)?, vec![8, 13]);
    assert_eq!(view.queue.read_back(1)?, vec![13]);
    assert_eq!(view.queue.front()?, Some(8));
    assert_eq!(view.queue.back()?, Some(13));
    assert_eq!(view.queue.count(), 2);
    view.queue.delete_front();
    assert_eq!(view.queue.front()?, Some(13));
    view.queue.delete_front();
    assert_eq!(view.queue.front()?, None);
    assert_eq!(view.queue.count(), 0);

    Ok(())
}

#[test]
fn sync_map_mut_access() -> Result<(), ViewError> {
    let (store, context) = new_test_context();

    {
        let mut view = FullSyncView::load(context)?;
        let value = view.map.get_mut_or_default("Geia")?;
        assert_eq!(*value, 0);
        *value = 42;
        let value = view.map.get_mut_or_default("Geia")?;
        assert_eq!(*value, 42);
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let view = FullSyncView::load(context)?;
        assert_eq!(view.map.get("Geia")?, Some(42));
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = FullSyncView::load(context)?;
        let value = view.map.get_mut_or_default("Geia")?;
        assert_eq!(*value, 42);
        *value = 43;
        view.rollback();
        let value = view.map.get_mut_or_default("Geia")?;
        assert_eq!(*value, 42);
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = FullSyncView::load(context)?;
        view.map.insert("Konnichiwa", 5)?;
        let value = view.map.get_mut("Konnichiwa")?.unwrap();
        *value = 6;
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store, Vec::new(), ());
        let view = FullSyncView::load(context)?;
        assert_eq!(view.map.get("Konnichiwa")?, Some(6));
    }

    Ok(())
}

#[test]
fn sync_store_rollback() -> Result<(), ViewError> {
    let (store, context) = new_test_context();

    {
        let mut view = FullSyncView::load(context)?;
        view.queue.push_back(8);
        view.map.insert("Hello", 5)?;
        let subview = view.collection.load_entry_mut("hola")?;
        subview.push(17);
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = FullSyncView::load(context)?;
        view.queue.push_back(7);
        view.map.insert("Hello", 4)?;
        let subview = view.collection.load_entry_mut("DobryDen")?;
        subview.push(16);
        view.rollback();
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = FullSyncView::load(context)?;
        view.queue.clear();
        view.map.clear();
        view.collection.clear();
        view.rollback();
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store, Vec::new(), ());
        let view = FullSyncView::load(context)?;
        assert_eq!(view.queue.front()?, Some(8));
        assert_eq!(view.map.get("Hello")?, Some(5));
        assert_eq!(view.collection.indices()?, vec!["hola".to_string()]);
    }

    Ok(())
}

#[test]
fn sync_collection_removal() -> Result<(), ViewError> {
    type EntryType = SyncRegisterView<TestContext, u8>;
    type CollectionViewType = SyncCollectionView<TestContext, u8, EntryType>;

    let (store, context) = new_test_context();

    // Write a dummy entry into the collection.
    let mut collection = CollectionViewType::load(context)?;
    let entry = collection.load_entry_mut(&1)?;
    entry.set(1);
    let mut batch = Batch::new();
    collection.pre_save(&mut batch)?;
    collection.context().store().write_batch(batch)?;
    collection.post_save();

    // Remove the entry from the collection.
    let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
    let mut collection = CollectionViewType::load(context)?;
    collection.remove_entry(&1)?;
    let mut batch = Batch::new();
    collection.pre_save(&mut batch)?;
    collection.context().store().write_batch(batch)?;
    collection.post_save();

    // Check that the entry was removed.
    let context = TestContext::new_unchecked(store, Vec::new(), ());
    let collection = CollectionViewType::load(context)?;
    assert!(!collection.indices()?.contains(&1));

    Ok(())
}

fn sync_removal_api_first_second_condition(
    first_condition: bool,
    second_condition: bool,
) -> Result<(), ViewError> {
    type EntryType = SyncRegisterView<TestContext, u8>;
    type CollectionViewType = SyncCollectionView<TestContext, u8, EntryType>;

    let (store, context) = new_test_context();

    // First add an entry `1` with value `100` and commit.
    let mut collection = CollectionViewType::load(context)?;
    let entry = collection.load_entry_mut(&1)?;
    entry.set(100);
    let mut batch = Batch::new();
    collection.pre_save(&mut batch)?;
    collection.context().store().write_batch(batch)?;
    collection.post_save();

    // Reload the collection view and remove the entry, but don't commit yet.
    let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
    let mut collection = CollectionViewType::load(context)?;
    collection.remove_entry(&1)?;

    // Now, read the entry with a different value if a certain condition is true.
    if first_condition {
        let entry = collection.load_entry_mut(&1)?;
        entry.set(200);
    }

    // Finally, either commit or rollback based on some other condition.
    if second_condition {
        collection.rollback();
    }

    let mut batch = Batch::new();
    collection.pre_save(&mut batch)?;
    collection.context().store().write_batch(batch)?;
    collection.post_save();

    let context = TestContext::new_unchecked(store, Vec::new(), ());
    let mut collection = CollectionViewType::load(context)?;
    let expected_val = if second_condition {
        Some(100)
    } else if first_condition {
        Some(200)
    } else {
        None
    };
    match expected_val {
        Some(expected_val_i) => {
            let subview = collection.load_entry_mut(&1)?;
            assert_eq!(subview.get(), &expected_val_i);
        }
        None => {
            assert!(!collection.indices()?.contains(&1));
        }
    };
    Ok(())
}

#[test]
fn sync_removal_api() -> Result<(), ViewError> {
    for first_condition in [true, false] {
        for second_condition in [true, false] {
            sync_removal_api_first_second_condition(first_condition, second_condition)?;
        }
    }
    Ok(())
}

#[test]
fn sync_byte_map_view() -> Result<(), ViewError> {
    let (store, context) = new_test_context();

    {
        let mut view = ByteMapStateView::load(context)?;
        view.map.insert(vec![0, 1], 5);
        view.map.insert(vec![2, 3], 23);
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = ByteMapStateView::load(context)?;
        view.map.remove_by_prefix(vec![0]);
        let val = view.map.get_mut_or_default(&[0, 1])?;
        assert_eq!(*val, 0);
        let val = view.map.get_mut(&[2, 3])?;
        assert_eq!(val.map(|value| *value), Some(23));
        view.save()?;
    }

    {
        let context = TestContext::new_unchecked(store, Vec::new(), ());
        let mut view = ByteMapStateView::load(context)?;
        view.map.remove_by_prefix(vec![2]);
        let val = view.map.get_mut(&[2, 3])?;
        assert_eq!(val, None);
    }

    Ok(())
}

#[test]
fn sync_collection_view_random_check() -> Result<(), ViewError> {
    let store = SyncMemoryStore::new_for_testing();
    let mut rng = make_deterministic_rng();
    let mut map = BTreeMap::<u8, u32>::new();
    let n = 20;
    let nmax: u8 = 25;
    for _ in 0..n {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = CollectionStateView::load(context)?;
        let save = rng.gen::<bool>();
        let count_oper = rng.gen_range(0..25);
        let mut new_map = map.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..6);
            if choice == 0 {
                let pos = rng.gen_range(0..nmax);
                view.v.remove_entry(&pos)?;
                new_map.remove(&pos);
            }
            if choice == 1 {
                let n_ins = rng.gen_range(0..5);
                for _i in 0..n_ins {
                    let pos = rng.gen_range(0..nmax);
                    let value = rng.gen::<u32>();
                    let subview = view.v.load_entry_mut(&pos)?;
                    *subview.get_mut() = value;
                    new_map.insert(pos, value);
                }
            }
            if choice == 2 {
                let n_load = rng.gen_range(0..5);
                for _i in 0..n_load {
                    let pos = rng.gen_range(0..nmax);
                    let _subview = view.v.load_entry_mut(&pos)?;
                    new_map.entry(pos).or_insert(0);
                }
            }
            if choice == 3 {
                let n_reset = rng.gen_range(0..5);
                for _i in 0..n_reset {
                    let pos = rng.gen_range(0..nmax);
                    view.v.reset_entry_to_default(&pos)?;
                    new_map.insert(pos, 0);
                }
            }
            if choice == 4 {
                view.clear();
                new_map.clear();
            }
            if choice == 5 {
                view.rollback();
                assert!(!view.has_pending_changes());
                new_map = map.clone();
            }
            for _ in 0..10 {
                let pos = rng.gen::<u8>();
                let test_view = view.v.try_load_entry(&pos)?.is_some();
                let test_map = new_map.contains_key(&pos);
                assert_eq!(test_view, test_map);
            }
            let key_values = view.key_values()?;
            assert_eq!(key_values, new_map);
        }
        if save {
            if map != new_map {
                assert!(view.has_pending_changes());
            }
            map = new_map.clone();
            view.save()?;
            assert!(!view.has_pending_changes());
        }
    }
    Ok(())
}

fn run_sync_byte_map_view_mutability<R: RngCore + Clone>(rng: &mut R) -> Result<(), ViewError> {
    let store = SyncMemoryStore::new_for_testing();
    let mut state_map = BTreeMap::new();
    let mut all_keys = BTreeSet::new();
    let n = 10;
    for _ in 0..n {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = ByteMapStateView::load(context)?;
        let save = rng.gen::<bool>();
        let read_state = view.map.key_values()?;
        let state_vec = state_map.clone().into_iter().collect::<Vec<_>>();
        assert_eq!(state_vec, read_state);
        let count_oper = rng.gen_range(0..25);
        let mut new_state_map = state_map.clone();
        let mut new_state_vec = state_vec.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..7);
            let count = view.map.count()?;
            if choice == 0 {
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
                let n_remove = rng.gen_range(0..count);
                for _ in 0..n_remove {
                    let pos = rng.gen_range(0..count);
                    let vec = new_state_vec[pos].clone();
                    view.map.remove(vec.0.clone());
                    new_state_map.remove(&vec.0);
                }
            }
            if choice == 2 && count > 0 {
                let val = rng.gen_range(0..5) as u8;
                let key_prefix = vec![val];
                view.map.remove_by_prefix(key_prefix.clone());
                remove_by_prefix(&mut new_state_map, key_prefix);
            }
            if choice == 3 {
                view.clear();
                new_state_map.clear();
            }
            if choice == 4 {
                view.rollback();
                assert!(!view.has_pending_changes());
                new_state_map = state_map.clone();
            }
            if choice == 5 && count > 0 {
                let pos = rng.gen_range(0..count);
                let vec = new_state_vec[pos].clone();
                let key = vec.0;
                let result = view.map.get_mut(&key)?.unwrap();
                let new_value = rng.gen::<u8>();
                *result = new_value;
                new_state_map.insert(key, new_value);
            }
            if choice == 6 && count > 0 {
                let choice = rng.gen_range(0..count);
                let key = match choice {
                    0 => {
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
                let test_view = view.map.contains_key(&key)?;
                let test_map = new_state_map.contains_key(&key);
                assert_eq!(test_view, test_map);
                let result = view.map.get_mut_or_default(&key)?;
                let new_value = rng.gen::<u8>();
                *result = new_value;
                new_state_map.insert(key, new_value);
            }
            new_state_vec = new_state_map.clone().into_iter().collect();
            let new_key_values = view.map.key_values()?;
            assert_eq!(new_state_vec, new_key_values);
            for u in 0..4 {
                let part_state_vec = new_state_vec
                    .iter()
                    .filter(|&x| x.0[0] == u)
                    .cloned()
                    .collect::<Vec<_>>();
                let part_key_values = view.map.key_values_by_prefix(vec![u])?;
                assert_eq!(part_state_vec, part_key_values);
            }
            let keys_vec = all_keys.iter().cloned().collect::<Vec<_>>();
            let values = view.map.multi_get(keys_vec.clone())?;
            for i in 0..keys_vec.len() {
                let key = &keys_vec[i];
                let test_map = new_state_map.contains_key(key);
                let test_view1 = view.map.get(key)?.is_some();
                let test_view2 = view.map.contains_key(key)?;
                assert_eq!(test_map, test_view1);
                assert_eq!(test_map, test_view2);
                assert_eq!(test_map, values[i].is_some());
            }
        }
        if save {
            if state_map != new_state_map {
                assert!(view.has_pending_changes());
            }
            state_map = new_state_map.clone();
            view.save()?;
            assert!(!view.has_pending_changes());
        }
    }
    Ok(())
}

fn remove_by_prefix<V>(map: &mut BTreeMap<Vec<u8>, V>, key_prefix: Vec<u8>) {
    map.retain(|key, _| !key.starts_with(&key_prefix));
}

#[test]
fn sync_byte_map_mutability() -> Result<(), ViewError> {
    let mut rng = make_deterministic_rng();
    for _ in 0..5 {
        run_sync_byte_map_view_mutability(&mut rng)?;
    }
    Ok(())
}

#[test]
fn sync_queue_view_mutability_check() -> Result<(), ViewError> {
    let store = SyncMemoryStore::new_for_testing();
    let mut rng = make_deterministic_rng();
    let mut vector = Vec::new();
    let n = 20;
    for _ in 0..n {
        let context = TestContext::new_unchecked(store.clone(), Vec::new(), ());
        let mut view = QueueStateView::load(context)?;
        let save = rng.gen::<bool>();
        let elements = view.queue.elements()?;
        assert_eq!(elements, vector);
        let count_oper = rng.gen_range(0..25);
        let mut new_vector = vector.clone();
        for _ in 0..count_oper {
            let choice = rng.gen_range(0..4);
            let count = view.queue.count();
            if choice == 0 {
                let n_ins = rng.gen_range(0..10);
                for _ in 0..n_ins {
                    let val = rng.gen::<u8>();
                    view.queue.push_back(val);
                    new_vector.push(val);
                }
            }
            if choice == 1 {
                let n_remove = rng.gen_range(0..=count);
                for _ in 0..n_remove {
                    view.queue.delete_front();
                    new_vector.remove(0);
                }
            }
            if choice == 2 {
                view.clear();
                new_vector.clear();
            }
            if choice == 3 {
                view.rollback();
                assert!(!view.has_pending_changes());
                new_vector.clone_from(&vector);
            }
            let front1 = view.queue.front()?;
            let front2 = new_vector.first().copied();
            assert_eq!(front1, front2);
            let new_elements = view.queue.elements()?;
            assert_eq!(new_elements, new_vector);
        }
        if save {
            if vector != new_vector {
                assert!(view.has_pending_changes());
            }
            vector.clone_from(&new_vector);
            view.save()?;
            assert!(!view.has_pending_changes());
        }
    }
    Ok(())
}
