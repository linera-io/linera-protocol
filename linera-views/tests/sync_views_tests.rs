// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    batch::Batch,
    context::{SyncContext, ViewSyncContext},
    memory::SyncMemoryStore,
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
