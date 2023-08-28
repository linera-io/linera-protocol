// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::Context,
    memory::{create_memory_context, MemoryContext},
    queue_view::QueueView,
    views::{View, ViewError},
};
use async_trait::async_trait;
use std::collections::VecDeque;

#[cfg(feature = "rocksdb")]
use {
    crate::rocks_db::{RocksDbClient, RocksDbContext, TEST_ROCKS_DB_MAX_STREAM_QUERIES},
    tempfile::TempDir,
};

#[cfg(feature = "aws")]
use crate::{
    dynamo_db::LocalStackTestContext,
    dynamo_db::{
        DynamoDbContext, TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES, TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
    },
};

#[cfg(feature = "scylladb")]
use crate::{scylla_db::create_scylla_db_test_client, scylla_db::ScyllaDbContext};

#[cfg(any(feature = "rocksdb", feature = "aws"))]
use crate::lru_caching::TEST_CACHE_SIZE;

#[tokio::test]
async fn test_queue_operations_with_memory_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(MemoryContextFactory).await
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn test_queue_operations_with_rocks_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(RocksDbContextFactory::default()).await
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_queue_operations_with_dynamo_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(DynamoDbContextFactory::default()).await
}

#[cfg(feature = "scylladb")]
#[tokio::test]
async fn test_queue_operations_with_scylla_db_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(ScyllaDbContextFactory::default()).await
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

#[cfg(feature = "rocksdb")]
#[derive(Default)]
struct RocksDbContextFactory {
    temporary_directories: Vec<TempDir>,
}

#[cfg(feature = "rocksdb")]
#[async_trait]
impl TestContextFactory for RocksDbContextFactory {
    type Context = RocksDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let directory = TempDir::new()?;
        let client = RocksDbClient::new(
            directory.path(),
            TEST_ROCKS_DB_MAX_STREAM_QUERIES,
            TEST_CACHE_SIZE,
        );
        let context = RocksDbContext::new(client, vec![], ());

        self.temporary_directories.push(directory);

        Ok(context)
    }
}

#[cfg(feature = "aws")]
#[derive(Default)]
struct DynamoDbContextFactory {
    localstack: Option<LocalStackTestContext>,
    table_counter: usize,
}

#[cfg(feature = "aws")]
#[async_trait]
impl TestContextFactory for DynamoDbContextFactory {
    type Context = DynamoDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        if self.localstack.is_none() {
            self.localstack = Some(LocalStackTestContext::new().await?);
        }
        let config = self.localstack.as_ref().unwrap().dynamo_db_config();

        let table = format!("linera{}", self.table_counter).parse()?;
        self.table_counter += 1;

        let dummy_key_prefix = vec![0];
        let (context, _) = DynamoDbContext::from_config(
            config,
            table,
            Some(TEST_DYNAMO_DB_MAX_CONCURRENT_QUERIES),
            TEST_DYNAMO_DB_MAX_STREAM_QUERIES,
            TEST_CACHE_SIZE,
            dummy_key_prefix,
            (),
        )
        .await?;

        Ok(context)
    }
}

#[cfg(feature = "scylladb")]
#[derive(Default)]
struct ScyllaDbContextFactory {
    table_names: Vec<String>,
}

#[cfg(feature = "scylladb")]
#[async_trait]
impl TestContextFactory for ScyllaDbContextFactory {
    type Context = ScyllaDbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let db = create_scylla_db_test_client().await;
        let table_name = db.get_table_name().await;
        let context = ScyllaDbContext::new(db, vec![], ());

        self.table_names.push(table_name);

        Ok(context)
    }
}
