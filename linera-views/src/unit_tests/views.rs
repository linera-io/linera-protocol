// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::Context,
    memory::{create_memory_context, MemoryContext},
    queue_view::QueueView,
    test_utils::test_views::{
        TestCollectionView, TestLogView, TestMapView, TestRegisterView, TestView,
    },
    views::{View, ViewError},
};
use async_trait::async_trait;
use std::{collections::VecDeque, marker::PhantomData};
use test_case::test_case;

#[cfg(any(with_rocksdb, with_scylladb, with_dynamodb))]
use crate::{common::AdminKeyValueStore, test_utils::generate_test_namespace};

#[cfg(with_rocksdb)]
use {
    crate::rocks_db::{create_rocks_db_test_config, RocksDbContext, RocksDbStore},
    tempfile::TempDir,
};

#[cfg(with_dynamodb)]
use crate::dynamo_db::{
    create_dynamo_db_common_config, DynamoDbContext, DynamoDbStore, DynamoDbStoreConfig,
    LocalStackTestContext,
};

#[cfg(with_scylladb)]
use crate::scylla_db::{create_scylla_db_test_config, ScyllaDbContext, ScyllaDbStore};

#[tokio::test]
async fn test_queue_operations_with_memory_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(MemoryContextFactory).await
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_queue_operations_with_rocks_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(RocksDbContextFactory::default()).await
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_queue_operations_with_dynamo_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(DynamoDbContextFactory::default()).await
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
                let context = context.clone();
                let mut batch = Batch::new();
                queue.flush(&mut batch)?;
                context.write_batch(batch).await?;
                queue = QueueView::load(context).await?;
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
        Ok(create_memory_context())
    }
}

#[cfg(with_rocksdb)]
#[derive(Default)]
struct RocksDbContextFactory {
    temporary_directories: Vec<TempDir>,
}

#[cfg(with_rocksdb)]
#[async_trait]
impl TestContextFactory for RocksDbContextFactory {
    type Context = RocksDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let (store_config, directory) = create_rocks_db_test_config().await;
        let namespace = generate_test_namespace();
        let store = RocksDbStore::recreate_and_connect(&store_config, &namespace)
            .await
            .expect("store");
        let dummy_key_prefix = vec![0];
        let context = RocksDbContext::new(store, dummy_key_prefix, ());

        self.temporary_directories.push(directory);

        Ok(context)
    }
}

#[cfg(with_dynamodb)]
#[derive(Default)]
struct DynamoDbContextFactory {
    localstack: Option<LocalStackTestContext>,
}

#[cfg(with_dynamodb)]
#[async_trait]
impl TestContextFactory for DynamoDbContextFactory {
    type Context = DynamoDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        if self.localstack.is_none() {
            self.localstack = Some(LocalStackTestContext::new().await?);
        }
        let config = self.localstack.as_ref().unwrap().dynamo_db_config();

        let namespace = generate_test_namespace();
        let common_config = create_dynamo_db_common_config();
        let store_config = DynamoDbStoreConfig {
            config,
            common_config,
        };
        let store = DynamoDbStore::recreate_and_connect(&store_config, &namespace).await?;
        let dummy_key_prefix = vec![0];
        Ok(DynamoDbContext::new(store, dummy_key_prefix, ()))
    }
}

#[cfg(with_scylladb)]
#[derive(Default)]
struct ScyllaDbContextFactory;

#[cfg(with_scylladb)]
#[async_trait]
impl TestContextFactory for ScyllaDbContextFactory {
    type Context = ScyllaDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let config = create_scylla_db_test_config().await;
        let namespace = generate_test_namespace();
        let store = ScyllaDbStore::recreate_and_connect(&config, &namespace).await?;
        let dummy_key_prefix = vec![0];
        let context = ScyllaDbContext::new(store, dummy_key_prefix, ());
        Ok(context)
    }
}

/// Check if a cloned view contains the staged changes from its source.
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
    let context = create_memory_context();
    let mut original = V::load(context).await?;
    let original_state = original.stage_initial_changes().await?;

    let clone = original.clone_unchecked()?;
    let clone_state = clone.read().await?;

    assert_eq!(original_state, clone_state);

    Ok(())
}

/// Check if new staged changes are separate between the cloned view and its source.
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
    let context = create_memory_context();
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
