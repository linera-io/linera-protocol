// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use test_case::test_case;

#[cfg(with_dynamodb)]
use crate::dynamo_db::DynamoDbStore;
#[cfg(with_rocksdb)]
use crate::rocks_db::RocksDbStore;
#[cfg(with_scylladb)]
use crate::scylla_db::ScyllaDbStore;
#[cfg(any(with_scylladb, with_dynamodb, with_rocksdb))]
use crate::store::TestKeyValueStore;
use crate::{
    batch::Batch,
    context::{create_test_memory_context, Context, MemoryContext},
    queue_view::QueueView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::{HashedRegisterView, RegisterView},
    test_utils::test_views::{
        TestCollectionView, TestLogView, TestMapView, TestRegisterView, TestView,
    },
    views::{HashableView, View, ViewError},
};
#[cfg(any(with_rocksdb, with_scylladb, with_dynamodb))]
use crate::{context::ViewContext, random::generate_test_namespace, store::AdminKeyValueStore};

#[tokio::test]
async fn test_queue_operations_with_memory_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(MemoryContextFactory).await
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_queue_operations_with_rocks_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(RocksDbContextFactory).await
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_queue_operations_with_dynamo_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(DynamoDbContextFactory).await
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_queue_operations_with_scylla_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(ScyllaDbContextFactory).await
}

#[derive(Clone, Copy, Debug)]
pub enum Operation {
    DeleteFront,
    PushBack(usize),
    CommitAndReload,
}

async fn run_test_queue_operations_test_cases<C>(mut contexts: C) -> Result<(), anyhow::Error>
where
    C: TestContextFactory,
    ViewError: From<<C::Context as Context>::Error>,
{
    use self::Operation::*;

    let test_cases = [
        vec![DeleteFront],
        vec![PushBack(100)],
        vec![PushBack(200), DeleteFront],
        vec![PushBack(1), PushBack(2), PushBack(3)],
        vec![
            PushBack(1),
            PushBack(2),
            PushBack(3),
            DeleteFront,
            DeleteFront,
            DeleteFront,
        ],
        vec![
            DeleteFront,
            DeleteFront,
            DeleteFront,
            PushBack(1),
            PushBack(2),
            PushBack(3),
        ],
        vec![
            PushBack(1),
            DeleteFront,
            PushBack(2),
            DeleteFront,
            PushBack(3),
            DeleteFront,
        ],
        vec![
            PushBack(1),
            PushBack(2),
            DeleteFront,
            DeleteFront,
            PushBack(100),
        ],
        vec![
            PushBack(1),
            PushBack(2),
            DeleteFront,
            DeleteFront,
            PushBack(100),
            PushBack(3),
            DeleteFront,
        ],
    ];

    for test_case in test_cases {
        for commit_location in 1..test_case.len() {
            let mut tweaked_test_case = test_case.clone();

            tweaked_test_case.insert(commit_location + 1, CommitAndReload);
            tweaked_test_case.push(CommitAndReload);

            run_test_queue_operations(tweaked_test_case, contexts.new_context().await?).await?;
        }
    }

    Ok(())
}

async fn run_test_queue_operations<C>(
    operations: impl IntoIterator<Item = Operation>,
    context: C,
) -> Result<(), anyhow::Error>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    let mut expected_state = VecDeque::new();
    let mut queue = QueueView::load(context.clone()).await?;

    check_queue_state(&mut queue, &expected_state).await?;

    for operation in operations {
        match operation {
            Operation::PushBack(new_item) => {
                queue.push_back(new_item);
                expected_state.push_back(new_item);
            }
            Operation::DeleteFront => {
                queue.delete_front();
                expected_state.pop_front();
            }
            Operation::CommitAndReload => {
                save_view(&context, &mut queue).await?;
                queue = QueueView::load(context.clone()).await?;
            }
        }

        check_queue_state(&mut queue, &expected_state).await?;
    }

    Ok(())
}

async fn check_queue_state<C>(
    queue: &mut QueueView<C, usize>,
    expected_state: &VecDeque<usize>,
) -> Result<(), anyhow::Error>
where
    C: Context + Clone + Send + Sync,
    ViewError: From<C::Error>,
{
    let count = expected_state.len();

    assert_eq!(queue.front().await?, expected_state.front().copied());
    assert_eq!(queue.back().await?, expected_state.back().copied());
    assert_eq!(queue.count(), count);

    check_contents(queue.read_front(count).await?, expected_state);
    check_contents(queue.read_back(count).await?, expected_state);

    Ok(())
}

fn check_contents(contents: Vec<usize>, expected: &VecDeque<usize>) {
    assert_eq!(&contents.into_iter().collect::<VecDeque<_>>(), expected);
}

#[async_trait]
trait TestContextFactory {
    type Context: Context + Clone + Send + Sync + 'static;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error>;
}

struct MemoryContextFactory;

#[async_trait]
impl TestContextFactory for MemoryContextFactory {
    type Context = MemoryContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        Ok(create_test_memory_context())
    }
}

#[cfg(with_rocksdb)]
struct RocksDbContextFactory;

#[cfg(with_rocksdb)]
#[async_trait]
impl TestContextFactory for RocksDbContextFactory {
    type Context = ViewContext<(), RocksDbStore>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let config = RocksDbStore::new_test_config().await?;
        let namespace = generate_test_namespace();
        let root_key = &[];
        let store = RocksDbStore::recreate_and_connect(&config, &namespace, root_key).await?;
        let context = ViewContext::create_root_context(store, ()).await?;

        Ok(context)
    }
}

#[cfg(with_dynamodb)]
struct DynamoDbContextFactory;

#[cfg(with_dynamodb)]
#[async_trait]
impl TestContextFactory for DynamoDbContextFactory {
    type Context = ViewContext<(), DynamoDbStore>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let config = DynamoDbStore::new_test_config().await?;
        let namespace = generate_test_namespace();
        let root_key = &[];
        let store = DynamoDbStore::recreate_and_connect(&config, &namespace, root_key).await?;
        Ok(ViewContext::create_root_context(store, ()).await?)
    }
}

#[cfg(with_scylladb)]
struct ScyllaDbContextFactory;

#[cfg(with_scylladb)]
#[async_trait]
impl TestContextFactory for ScyllaDbContextFactory {
    type Context = ViewContext<(), ScyllaDbStore>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let config = ScyllaDbStore::new_test_config().await?;
        let namespace = generate_test_namespace();
        let root_key = &[];
        let store = ScyllaDbStore::recreate_and_connect(&config, &namespace, root_key).await?;
        let context = ViewContext::create_root_context(store, ()).await?;
        Ok(context)
    }
}

/// Checks if a cloned view contains the staged changes from its source.
#[test_case(PhantomData::<TestCollectionView<_>>; "with CollectionView")]
#[test_case(PhantomData::<TestLogView<_>>; "with LogView")]
#[test_case(PhantomData::<TestMapView<_>>; "with MapView")]
#[test_case(PhantomData::<TestRegisterView<_>>; "with RegisterView")]
#[tokio::test]
async fn test_clone_includes_staged_changes<V>(
    _view_type: PhantomData<V>,
) -> Result<(), anyhow::Error>
where
    V: TestView,
{
    let context = create_test_memory_context();
    let mut original = V::load(context).await?;
    let original_state = original.stage_initial_changes().await?;

    let clone = original.clone_unchecked()?;
    let clone_state = clone.read().await?;

    assert_eq!(original_state, clone_state);

    Ok(())
}

/// Checks if new staged changes are separate between the cloned view and its source.
#[test_case(PhantomData::<TestCollectionView<_>>; "with CollectionView")]
#[test_case(PhantomData::<TestLogView<_>>; "with LogView")]
#[test_case(PhantomData::<TestMapView<_>>; "with MapView")]
#[test_case(PhantomData::<TestRegisterView<_>>; "with RegisterView")]
#[tokio::test]
async fn test_original_and_clone_stage_changes_separately<V>(
    _view_type: PhantomData<V>,
) -> Result<(), anyhow::Error>
where
    V: TestView,
{
    let context = create_test_memory_context();
    let mut original = V::load(context).await?;
    original.stage_initial_changes().await?;

    let mut first_clone = original.clone_unchecked()?;
    let second_clone = original.clone_unchecked()?;

    let original_state = original.stage_changes_to_be_discarded().await?;
    let first_clone_state = first_clone.stage_changes_to_be_persisted().await?;
    let second_clone_state = second_clone.read().await?;

    assert_ne!(original_state, first_clone_state);
    assert_ne!(original_state, second_clone_state);
    assert_ne!(first_clone_state, second_clone_state);

    Ok(())
}

/// Checks if the cached hash value persisted in storage is cleared when flushing a cleared
/// [`HashableRegisterView`].
///
/// Otherwise `rollback` may set the cached staged hash value to an incorrect value.
#[tokio::test]
async fn test_clearing_of_cached_stored_hash() -> anyhow::Result<()> {
    let context = create_test_memory_context();
    let mut view = HashedRegisterView::<_, String>::load(context.clone()).await?;

    let empty_hash = view.hash().await?;
    assert_eq!(view.hash_mut().await?, empty_hash);

    view.set("some value".to_owned());

    let populated_hash = view.hash().await?;
    assert_eq!(view.hash_mut().await?, populated_hash);
    assert_ne!(populated_hash, empty_hash);

    save_view(&context, &mut view).await?;

    assert_eq!(view.hash().await?, populated_hash);
    assert_eq!(view.hash_mut().await?, populated_hash);

    view.clear();

    assert_eq!(view.hash().await?, empty_hash);
    assert_eq!(view.hash_mut().await?, empty_hash);

    save_view(&context, &mut view).await?;

    assert_eq!(view.hash().await?, empty_hash);
    assert_eq!(view.hash_mut().await?, empty_hash);

    view.rollback();

    assert_eq!(view.hash().await?, empty_hash);
    assert_eq!(view.hash_mut().await?, empty_hash);

    Ok(())
}

/// Checks if a [`ReentrantCollectionView`] doesn't have pending changes after loading its
/// entries.
#[tokio::test]
async fn test_reentrant_collection_view_has_no_pending_changes_after_try_load_entries(
) -> anyhow::Result<()> {
    let context = create_test_memory_context();
    let values = [(1, "first".to_owned()), (2, "second".to_owned())];
    let mut view =
        ReentrantCollectionView::<_, u8, RegisterView<_, String>>::load(context.clone()).await?;

    assert!(!view.has_pending_changes().await);
    populate_reentrant_collection_view(&mut view, values.clone()).await?;
    assert!(view.has_pending_changes().await);
    save_view(&context, &mut view).await?;
    assert!(!view.has_pending_changes().await);

    let entries = view.try_load_entries(vec![&1, &2]).await?;
    assert_eq!(entries.len(), 2);
    assert!(entries[0].is_some());
    assert!(entries[1].is_some());
    assert_eq!(entries[0].as_ref().unwrap().get(), &values[0].1);
    assert_eq!(entries[1].as_ref().unwrap().get(), &values[1].1);

    assert!(!view.has_pending_changes().await);

    Ok(())
}

/// Checks if a [`ReentrantCollectionView`] has pending changes after adding an entry.
#[tokio::test]
async fn test_reentrant_collection_view_has_pending_changes_after_new_entry() -> anyhow::Result<()>
{
    let context = create_test_memory_context();
    let values = [(1, "first".to_owned()), (2, "second".to_owned())];
    let mut view =
        ReentrantCollectionView::<_, u8, RegisterView<_, String>>::load(context.clone()).await?;

    populate_reentrant_collection_view(&mut view, values.clone()).await?;
    save_view(&context, &mut view).await?;
    assert!(!view.has_pending_changes().await);

    {
        let entry = view.try_load_entry_mut(&3).await?;
        assert_eq!(entry.get(), "");
        assert!(!entry.has_pending_changes().await);
    }

    assert!(view.has_pending_changes().await);

    Ok(())
}

/// Checks if acquiring a write-lock to a sub-view causes the collection to have pending changes.
#[tokio::test]
async fn test_reentrant_collection_view_has_pending_changes_after_try_load_entry_mut(
) -> anyhow::Result<()> {
    let context = create_test_memory_context();
    let values = [(1, "first".to_owned()), (2, "second".to_owned())];
    let mut view =
        ReentrantCollectionView::<_, u8, RegisterView<_, String>>::load(context.clone()).await?;

    populate_reentrant_collection_view(&mut view, values.clone()).await?;
    save_view(&context, &mut view).await?;
    assert!(!view.has_pending_changes().await);

    let entry = view
        .try_load_entry(&1)
        .await?
        .expect("Missing first entry in collection");
    assert_eq!(entry.get(), &values[0].1);
    assert!(!entry.has_pending_changes().await);

    assert!(!view.has_pending_changes().await);

    drop(entry);
    let entry = view.try_load_entry_mut(&1).await?;
    assert_eq!(entry.get(), &values[0].1);
    assert!(!entry.has_pending_changes().await);

    assert!(view.has_pending_changes().await);

    Ok(())
}

/// Checks if acquiring multiple write-locks to sub-views causes the collection to have pending
/// changes.
#[tokio::test]
async fn test_reentrant_collection_view_has_pending_changes_after_try_load_entries_mut(
) -> anyhow::Result<()> {
    let context = create_test_memory_context();
    let values = [
        (1, "first".to_owned()),
        (2, "second".to_owned()),
        (3, "third".to_owned()),
        (4, "fourth".to_owned()),
    ];
    let mut view =
        ReentrantCollectionView::<_, u8, RegisterView<_, String>>::load(context.clone()).await?;

    populate_reentrant_collection_view(&mut view, values.clone()).await?;
    save_view(&context, &mut view).await?;
    assert!(!view.has_pending_changes().await);

    let entries = view.try_load_entries([&2, &3]).await?;
    assert_eq!(entries.len(), 2);
    assert!(entries[0].is_some());
    assert!(entries[1].is_some());
    assert_eq!(entries[0].as_ref().unwrap().get(), &values[1].1);
    assert_eq!(entries[1].as_ref().unwrap().get(), &values[2].1);
    assert!(!entries[0].as_ref().unwrap().has_pending_changes().await);
    assert!(!entries[1].as_ref().unwrap().has_pending_changes().await);

    assert!(!view.has_pending_changes().await);

    drop(entries);
    let entries = view.try_load_entries_mut([&2, &3]).await?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].get(), &values[1].1);
    assert_eq!(entries[1].get(), &values[2].1);
    assert!(!entries[0].has_pending_changes().await);
    assert!(!entries[1].has_pending_changes().await);

    assert!(view.has_pending_changes().await);

    Ok(())
}

/// Checks if a cleared [`RegisterView`] has no pending changes after flushing.
#[tokio::test]
async fn test_flushing_cleared_register_view() -> anyhow::Result<()> {
    let context = create_test_memory_context();
    let mut view = RegisterView::<_, bool>::load(context.clone()).await?;

    assert!(!view.has_pending_changes().await);
    view.clear();
    assert!(view.has_pending_changes().await);

    save_view(&context, &mut view).await?;
    assert!(!view.has_pending_changes().await);

    Ok(())
}

/// Saves a [`View`] into the [`MemoryContext<()>`] storage simulation.
async fn save_view<C>(context: &C, view: &mut impl View<C>) -> anyhow::Result<()>
where
    C: Context,
{
    let mut batch = Batch::new();
    view.flush(&mut batch)?;
    context.write_batch(batch).await?;
    Ok(())
}

/// Populates a [`ReentrantCollectionView`] with some `entries`.
async fn populate_reentrant_collection_view<C, Key, Value>(
    collection: &mut ReentrantCollectionView<C, Key, RegisterView<C, Value>>,
    entries: impl IntoIterator<Item = (Key, Value)>,
) -> anyhow::Result<()>
where
    C: Context + Send + Sync,
    Key: Serialize + DeserializeOwned + Clone + Debug + Default + Send + Sync,
    Value: Serialize + DeserializeOwned + Default + Send + Sync,
    ViewError: From<C::Error>,
{
    for (key, value) in entries {
        let mut entry = collection.try_load_entry_mut(&key).await?;
        entry.set(value);
    }

    Ok(())
}
