// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_views::{
    dynamo_db::{DynamoDbContext, DynamoDbContextError},
    hash::{HashView, Hasher, HashingContext},
    impl_view,
    memory::{MemoryContext, MemoryStoreMap, MemoryViewError},
    rocksdb::{KeyValueOperations, RocksdbContext, RocksdbViewError, DB},
    test_utils::LocalStackTestContext,
    views::{
        AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations, CollectionView, Context,
        MapOperations, MapView, QueueOperations, QueueView, RegisterOperations, RegisterView,
        ScopedView, View,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

#[allow(clippy::type_complexity)]
pub struct StateView<C> {
    pub x1: ScopedView<0, RegisterView<C, u64>>,
    pub x2: ScopedView<1, RegisterView<C, u32>>,
    pub log: ScopedView<2, AppendOnlyLogView<C, u32>>,
    pub map: ScopedView<3, MapView<C, String, usize>>,
    pub queue: ScopedView<4, QueueView<C, u64>>,
    pub collection: ScopedView<5, CollectionView<C, String, AppendOnlyLogView<C, u32>>>,
    pub collection2:
        ScopedView<6, CollectionView<C, String, CollectionView<C, String, RegisterView<C, u32>>>>,
}

// This also generates `trait StateViewContext: Context ... {}`
impl_view!(StateView { x1, x2, log, map, queue, collection, collection2 };
           RegisterOperations<u64>,
           RegisterOperations<u32>,
           AppendOnlyLogOperations<u32>,
           MapOperations<String, usize>,
           QueueOperations<u64>,
           CollectionOperations<String>
);

#[async_trait]
pub trait StateStore {
    type Context: StateViewContext<Extra = usize>;

    async fn load(
        &mut self,
        id: usize,
    ) -> Result<StateView<Self::Context>, <Self::Context as Context>::Error>;
}

#[derive(Default)]
pub struct MemoryTestStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for MemoryTestStore {
    type Context = MemoryContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, MemoryViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = MemoryContext::new(state.clone().lock_owned().await, id);
        StateView::load(context).await
    }
}

pub struct RocksdbTestStore {
    db: Arc<DB>,
    locks: HashMap<usize, Arc<Mutex<()>>>,
}

impl RocksdbTestStore {
    fn new(db: DB) -> Self {
        Self {
            db: Arc::new(db),
            locks: HashMap::new(),
        }
    }
}

#[async_trait]
impl StateStore for RocksdbTestStore {
    type Context = RocksdbContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, RocksdbViewError> {
        let lock = self
            .locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = RocksdbContext::new(
            self.db.clone(),
            lock.clone().lock_owned().await,
            bcs::to_bytes(&id)?,
            id,
        );
        StateView::load(context).await
    }
}

pub struct DynamoDbTestStore {
    localstack: LocalStackTestContext,
    locks: HashMap<usize, Arc<Mutex<()>>>,
}

impl DynamoDbTestStore {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(DynamoDbTestStore {
            localstack: LocalStackTestContext::new().await?,
            locks: HashMap::new(),
        })
    }
}

#[async_trait]
impl StateStore for DynamoDbTestStore {
    type Context = DynamoDbContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, DynamoDbContextError> {
        log::trace!("Acquiring lock on {:?}", id);
        let lock = self
            .locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        let (context, _) = DynamoDbContext::from_config(
            self.localstack.dynamo_db_config(),
            "test_table".parse().expect("Invalid table name"),
            lock.clone().lock_owned().await,
            vec![0],
            id,
        )
        .await
        .expect("Failed to create DynamoDB context");
        StateView::load(context).await
    }
}

#[cfg(test)]
async fn test_store<S>(store: &mut S) -> <<S::Context as HashingContext>::Hasher as Hasher>::Output
where
    S: StateStore,
{
    let default_hash = {
        let mut view = store.load(1).await.unwrap();
        view.hash().await.unwrap()
    };
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1.extra(), &1);
        let hash = view.hash().await.unwrap();
        assert_eq!(hash, default_hash);
        assert_eq!(view.x1.get(), &0);
        view.x1.set(1);
        view.rollback();
        assert_eq!(view.hash().await.unwrap(), hash);
        view.x2.set(2);
        assert_ne!(view.hash().await.unwrap(), hash);
        view.log.push(4);
        view.queue.push_back(8);
        assert_eq!(view.queue.front().await.unwrap(), Some(8));
        view.queue.push_back(7);
        view.queue.delete_front();
        view.map.insert("Hello".to_string(), 5);
        assert_eq!(view.x1.get(), &0);
        assert_eq!(view.x2.get(), &2);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        assert_eq!(view.queue.read_front(10).await.unwrap(), vec![7]);
        assert_eq!(view.map.get(&"Hello".to_string()).await.unwrap(), Some(5));
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            subview.push(17);
            subview.push(18);
        }
        assert_eq!(
            view.collection.indices().await.unwrap(),
            vec!["hola".to_string()]
        );
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    };
    let staged_hash = {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.hash().await.unwrap(), default_hash);
        assert_eq!(view.x1.get(), &0);
        assert_eq!(view.x2.get(), &0);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![]);
        assert_eq!(view.queue.read_front(10).await.unwrap(), vec![]);
        assert_eq!(view.map.get(&"Hello".to_string()).await.unwrap(), None);
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![]);
        }
        {
            let subview = view
                .collection2
                .load_entry("ciao".to_string())
                .await
                .unwrap();
            let subsubview = subview.load_entry("!".to_string()).await.unwrap();
            subsubview.set(3);
            assert_eq!(subsubview.get(), &3);
        }
        view.x1.set(1);
        view.log.push(4);
        view.queue.push_back(7);
        view.map.insert("Hello".to_string(), 5);
        view.map.insert("Hi".to_string(), 2);
        view.map.remove("Hi".to_string());
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            subview.push(17);
            subview.push(18);
        }
        let hash = view.hash().await.unwrap();
        view.write_commit().await.unwrap();
        hash
    };
    {
        let mut view = store.load(1).await.unwrap();
        let stored_hash = view.hash().await.unwrap();
        assert_eq!(staged_hash, stored_hash);
        assert_eq!(view.x1.get(), &1);
        assert_eq!(view.x2.get(), &0);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        view.queue.push_back(8);
        assert_eq!(view.queue.read_front(10).await.unwrap(), vec![7, 8]);
        assert_eq!(view.queue.read_front(1).await.unwrap(), vec![7]);
        assert_eq!(view.queue.read_back(10).await.unwrap(), vec![7, 8]);
        assert_eq!(view.queue.read_back(1).await.unwrap(), vec![8]);
        assert_eq!(view.queue.front().await.unwrap(), Some(7));
        assert_eq!(view.queue.back().await.unwrap(), Some(8));
        assert_eq!(view.queue.count(), 2);
        view.queue.delete_front();
        assert_eq!(view.queue.front().await.unwrap(), Some(8));
        view.queue.delete_front();
        assert_eq!(view.queue.front().await.unwrap(), None);
        assert_eq!(view.queue.count(), 0);
        assert_eq!(view.map.get(&"Hello".to_string()).await.unwrap(), Some(5));
        assert_eq!(view.map.get(&"Hi".to_string()).await.unwrap(), None);
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
        {
            let subview = view
                .collection2
                .load_entry("ciao".to_string())
                .await
                .unwrap();
            let subsubview = subview.load_entry("!".to_string()).await.unwrap();
            assert_eq!(subsubview.get(), &3);
        }
        assert_eq!(
            view.collection.indices().await.unwrap(),
            vec!["hola".to_string()]
        );
        view.collection.remove_entry("hola".to_string());
        assert_ne!(view.hash().await.unwrap(), stored_hash);
        view.write_commit().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![]);
        }
        view.write_delete().await.unwrap();
    }
    staged_hash
}

#[tokio::test]
async fn test_collection_removal() -> anyhow::Result<()> {
    type EntryType = RegisterView<MemoryContext<()>, u8>;
    type CollectionViewType = CollectionView<MemoryContext<()>, u8, EntryType>;

    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let context = MemoryContext::new(state.lock_owned().await, ());

    // Write a dummy entry into the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    let entry = collection.load_entry(1).await?;
    entry.set(1);
    collection.commit(&mut ()).await?;

    // Remove the entry from the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    collection.remove_entry(1);
    collection.commit(&mut ()).await?;

    // Check that the entry was removed.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    assert!(!collection.indices().await?.contains(&1));

    Ok(())
}

#[tokio::test]
async fn test_removal_api() -> anyhow::Result<()> {
    async fn test_case(first_condition: bool, second_condition: bool) -> anyhow::Result<()> {
        type EntryType = RegisterView<MemoryContext<()>, u8>;
        type CollectionViewType = CollectionView<MemoryContext<()>, u8, EntryType>;

        let state = Arc::new(Mutex::new(BTreeMap::new()));
        let context = MemoryContext::new(state.lock_owned().await, ());

        // First add an entry `1` with value `100` and commit
        let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
        let entry = collection.load_entry(1).await?;
        entry.set(100);
        collection.commit(&mut ()).await?;

        // Reload the collection view and remove the entry, but don't commit yet
        let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
        collection.remove_entry(1);

        // Now, read the entry with a different value if a certain condition is true
        if first_condition {
            let entry = collection.load_entry(1).await?;
            entry.set(200);
        }

        // Finally, either commit or rollback based on some other condition
        if second_condition {
            // If rolling back, then the entry `1` still exists with value `100`.
            collection.rollback();
        }

        // We commit
        collection.commit(&mut ()).await?;

        let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
        let expected_val = if second_condition {
            Some(100)
        } else {
            if first_condition {
                Some(200)
            } else {
                None
            }
        };
        match expected_val {
            Some(expected_val_i) => {
                let subview = collection.load_entry(1).await?;
                assert_eq!(subview.get(), &expected_val_i);
            },
            None => {
                assert!(!collection.indices().await?.contains(&1));
            }
        };
        Ok(())
    }
    test_case(true , true).await?;
    test_case(false, true).await?;
    test_case(true , false).await?;
    test_case(false, false).await?;
    Ok(())
}

#[tokio::test]
async fn test_views_in_memory() {
    let mut store = MemoryTestStore::default();
    test_store(&mut store).await;
    assert_eq!(store.states.len(), 1);
    let entry = store.states.get(&1).unwrap().clone();
    assert!(entry.lock().await.is_empty());
}

#[tokio::test]
async fn test_views_in_rocksdb() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);

    let db = DB::open(&options, &dir).unwrap();
    let mut store = RocksdbTestStore::new(db);
    let hash = test_store(&mut store).await;
    assert_eq!(store.locks.len(), 1);
    assert_eq!(store.db.count_keys().await.unwrap(), 0);

    let mut store = MemoryTestStore::default();
    let hash2 = test_store(&mut store).await;
    assert_eq!(hash, hash2);
}

#[tokio::test]
#[ignore]
async fn test_views_in_dynamo_db() -> Result<(), anyhow::Error> {
    let mut store = DynamoDbTestStore::new().await?;
    let hash = test_store(&mut store).await;
    assert_eq!(store.locks.len(), 1);

    let mut store = MemoryTestStore::default();
    let hash2 = test_store(&mut store).await;
    assert_eq!(hash, hash2);

    Ok(())
}
