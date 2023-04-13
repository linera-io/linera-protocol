// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    batch::Batch,
    common::Context,
    memory::MemoryContext,
    queue_view::QueueView,
    rocksdb::RocksdbContext,
    views::{View, ViewError},
    rocksdb::RocksdbClient,
};
use async_lock::Mutex;
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tempfile::TempDir;

#[cfg(feature = "aws")]
use crate::{dynamo_db::DynamoDbContext, test_utils::LocalStackTestContext};

#[tokio::test]
async fn test_queue_operations_with_memory_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(MemoryContextFactory).await
}

#[tokio::test]
async fn test_queue_operations_with_rocksdb_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(RocksdbContextFactory::default()).await
}

#[tokio::test]
#[cfg(feature = "aws")]
async fn test_queue_operations_with_dynamodb_context() -> Result<(), anyhow::Error> {
    run_test_queue_operations_test_cases(DynamoDbContextFactory::default()).await
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
        let dummy_lock = Arc::new(Mutex::new(BTreeMap::new()));
        Ok(MemoryContext::new(dummy_lock.lock_arc().await, ()))
    }
}

#[derive(Default)]
struct RocksdbContextFactory {
    temporary_directories: Vec<TempDir>,
}

#[async_trait]
impl TestContextFactory for RocksdbContextFactory {
    type Context = RocksdbContext<()>;

    async fn new_context(&mut self) -> Result<Self::Context, anyhow::Error> {
        let directory = TempDir::new()?;
        let client = RocksdbClient::new(directory.path());
        let context = RocksdbContext::new(client, vec![], ());

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
        let (context, _) =
            DynamoDbContext::from_config(config, table, dummy_key_prefix, ()).await?;

        Ok(context)
    }
}
