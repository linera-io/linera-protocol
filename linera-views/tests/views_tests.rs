// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_lock::Mutex;
use async_trait::async_trait;
use linera_views::{
    batch::{
        Batch, WriteOperation,
        WriteOperation::{Delete, DeletePrefix, Put},
    },
    collection_view::CollectionView,
    common::Context,
    key_value_store_view::{KeyValueStoreMemoryContext, KeyValueStoreView},
    log_view::LogView,
    lru_caching::{LruCachingMemoryContext, TEST_CACHE_SIZE},
    map_view::MapView,
    memory::{MemoryContext, MemoryStoreMap},
    queue_view::QueueView,
    reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView,
    rocksdb::{RocksdbClient, RocksdbContext},
    set_view::SetView,
    test_utils::{
        get_random_byte_vector, get_random_key_value_operations, get_random_key_value_vec,
        random_shuffle, span_random_reordering_put_delete,
    },
    views::{CryptoHashRootView, HashableView, Hasher, RootView, View, ViewError},
};
use rand::{Rng, RngCore, SeedableRng};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

#[cfg(feature = "aws")]
use linera_views::{dynamo_db::DynamoDbContext, test_utils::LocalStackTestContext};

#[allow(clippy::type_complexity)]
#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub x1: RegisterView<C, u64>,
    pub x2: RegisterView<C, u32>,
    pub log: LogView<C, u32>,
    pub map: MapView<C, String, usize>,
    pub set: SetView<C, usize>,
    pub queue: QueueView<C, u64>,
    pub collection: CollectionView<C, String, LogView<C, u32>>,
    pub collection2: CollectionView<C, String, CollectionView<C, String, RegisterView<C, u32>>>,
    pub collection3: CollectionView<C, String, QueueView<C, u64>>,
    pub collection4: ReentrantCollectionView<C, String, QueueView<C, u64>>,
    pub key_value_store: KeyValueStoreView<C>,
}

#[async_trait]
pub trait StateStore {
    type Context: Context<Extra = usize> + Clone + Send + Sync + 'static;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError>;
}

#[derive(Default)]
pub struct MemoryTestStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for MemoryTestStore {
    type Context = MemoryContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let context = MemoryContext::new(state.clone().lock_arc().await, id);
        StateView::load(context).await
    }
}

#[derive(Default)]
pub struct KeyValueStoreTestStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for KeyValueStoreTestStore {
    type Context = KeyValueStoreMemoryContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = state.clone().lock_arc().await;
        let base_key = bcs::to_bytes(&id)?;
        let context = KeyValueStoreMemoryContext::new(guard, base_key, id).await?;
        StateView::load(context).await
    }
}

#[derive(Default)]
pub struct LruMemoryStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for LruMemoryStore {
    type Context = LruCachingMemoryContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = state.clone().lock_arc().await;
        let base_key = bcs::to_bytes(&id)?;
        let n = 1000;
        let context = LruCachingMemoryContext::new(guard, base_key, id, n).await?;
        StateView::load(context).await
    }
}

pub struct RocksdbTestStore {
    db: RocksdbClient,
    accessed_chains: BTreeSet<usize>,
}

impl RocksdbTestStore {
    fn new(db: RocksdbClient) -> Self {
        Self {
            db,
            accessed_chains: BTreeSet::new(),
        }
    }
}

#[async_trait]
impl StateStore for RocksdbTestStore {
    type Context = RocksdbContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        self.accessed_chains.insert(id);
        // TODO(#643): Actually acquire a lock.
        tracing::trace!("Acquiring lock on {:?}", id);
        let context = RocksdbContext::new(self.db.clone(), bcs::to_bytes(&id)?, id);
        StateView::load(context).await
    }
}

#[cfg(feature = "aws")]
pub struct DynamoDbTestStore {
    localstack: LocalStackTestContext,
    accessed_chains: BTreeSet<usize>,
}

#[cfg(feature = "aws")]
impl DynamoDbTestStore {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Ok(DynamoDbTestStore {
            localstack: LocalStackTestContext::new().await?,
            accessed_chains: BTreeSet::new(),
        })
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl StateStore for DynamoDbTestStore {
    type Context = DynamoDbContext<usize>;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        self.accessed_chains.insert(id);
        // TODO(#643): Actually acquire a lock.
        tracing::trace!("Acquiring lock on {:?}", id);
        let (context, _) = DynamoDbContext::from_config(
            self.localstack.dynamo_db_config(),
            "test_table".parse().expect("Invalid table name"),
            TEST_CACHE_SIZE,
            vec![0],
            id,
        )
        .await
        .expect("Failed to create DynamoDB context");
        StateView::load(context).await
    }
}

#[derive(Debug)]
pub struct TestConfig {
    with_x1: bool,
    with_x2: bool,
    with_flush: bool,
    with_map: bool,
    with_set: bool,
    with_queue: bool,
    with_log: bool,
    with_collection: bool,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            with_x1: true,
            with_x2: true,
            with_flush: true,
            with_map: true,
            with_set: true,
            with_queue: true,
            with_log: true,
            with_collection: true,
        }
    }
}

impl TestConfig {
    fn samples() -> Vec<TestConfig> {
        vec![
            TestConfig {
                with_x1: false,
                with_x2: false,
                with_flush: false,
                with_map: false,
                with_set: false,
                with_queue: false,
                with_log: false,
                with_collection: false,
            },
            TestConfig {
                with_x1: true,
                with_x2: true,
                with_flush: false,
                with_map: false,
                with_set: false,
                with_queue: false,
                with_log: false,
                with_collection: false,
            },
            TestConfig {
                with_x1: false,
                with_x2: false,
                with_flush: true,
                with_map: false,
                with_set: false,
                with_queue: true,
                with_log: true,
                with_collection: false,
            },
            TestConfig {
                with_x1: false,
                with_x2: false,
                with_flush: true,
                with_map: true,
                with_set: true,
                with_queue: false,
                with_log: false,
                with_collection: true,
            },
            TestConfig::default(),
        ]
    }
}

#[cfg(test)]
async fn test_store<S>(store: &mut S, config: &TestConfig) -> <sha3::Sha3_256 as Hasher>::Output
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let default_hash = {
        let view = store.load(1).await.unwrap();
        view.hash().await.unwrap()
    };
    {
        let mut view = store.load(1).await.unwrap();
        if config.with_x1 {
            assert_eq!(view.x1.extra(), &1);
        }
        let hash = view.hash().await.unwrap();
        assert_eq!(hash, default_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
            view.x1.set(1);
        }
        view.rollback();
        assert_eq!(view.hash().await.unwrap(), hash);
        if config.with_x2 {
            view.x2.set(2);
        }
        if config.with_x2 {
            assert_ne!(view.hash().await.unwrap(), hash);
        }
        if config.with_log {
            view.log.push(4);
        }
        if config.with_queue {
            view.queue.push_back(8);
            assert_eq!(view.queue.front().await.unwrap(), Some(8));
            view.queue.push_back(7);
            view.queue.delete_front();
        }
        if config.with_map {
            view.map.insert("Hello", 5).unwrap();
            assert_eq!(view.map.indices().await.unwrap(), vec!["Hello".to_string()]);
            let mut count = 0;
            view.map
                .for_each_index(|_index| {
                    count += 1;
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(count, 1);
        }
        if config.with_set {
            view.set.insert(&42).unwrap();
            assert_eq!(view.set.indices().await.unwrap(), vec![42]);
            let mut count = 0;
            view.set
                .for_each_index(|_index| {
                    count += 1;
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(count, 1);
        }
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &2);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        }
        if config.with_queue {
            assert_eq!(view.queue.read_front(10).await.unwrap(), vec![7]);
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await.unwrap(), Some(5));
        }
        if config.with_set {
            assert!(view.set.contains(&42).await.unwrap());
        }
        if config.with_collection {
            {
                let subview = view.collection.load_entry_mut("hola").await.unwrap();
                subview.push(17);
                subview.push(18);
                assert_eq!(
                    view.collection.indices().await.unwrap(),
                    vec!["hola".to_string()]
                );
                let mut count = 0;
                view.collection
                    .for_each_index(|_index| {
                        count += 1;
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(count, 1);
            }
            let subview = view.collection.try_load_entry("hola").await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    };
    let staged_hash = {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.hash().await.unwrap(), default_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &0);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await.unwrap(), Vec::<u32>::new());
        }
        if config.with_queue {
            assert_eq!(view.queue.read_front(10).await.unwrap(), Vec::<u64>::new());
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await.unwrap(), None);
        }
        if config.with_set {
            assert!(!view.set.contains(&42).await.unwrap());
        }
        if config.with_collection {
            let subview = view.collection.try_load_entry("hola").await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), Vec::<u32>::new());
            let subview = view.collection2.load_entry_mut("ciao").await.unwrap();
            let subsubview = subview.load_entry_mut("!").await.unwrap();
            subsubview.set(3);
            assert_eq!(subsubview.get(), &3);
        }
        if config.with_x1 {
            view.x1.set(1);
        }
        if config.with_log {
            view.log.push(4);
        }
        if config.with_queue {
            view.queue.push_back(7);
        }
        if config.with_map {
            view.map.insert("Hello", 5).unwrap();
            view.map.insert("Hi", 2).unwrap();
            view.map.remove("Hi").unwrap();
        }
        if config.with_set {
            view.set.insert(&42).unwrap();
            view.set.insert(&59).unwrap();
            view.set.remove(&59).unwrap();
        }
        if config.with_collection {
            let subview = view.collection.load_entry_mut("hola").await.unwrap();
            subview.push(17);
            subview.push(18);
        }
        if config.with_flush {
            view.save().await.unwrap();
        }
        let hash1 = view.hash().await.unwrap();
        let hash2 = view.hash().await.unwrap();
        view.save().await.unwrap();
        let hash3 = view.hash().await.unwrap();
        assert_eq!(hash1, hash2);
        assert_eq!(hash1, hash3);
        hash1
    };
    {
        let mut view = store.load(1).await.unwrap();
        let stored_hash = view.hash().await.unwrap();
        assert_eq!(staged_hash, stored_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &1);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &0);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        }
        if config.with_queue {
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
            view.queue.push_back(13);
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await.unwrap(), Some(5));
            assert_eq!(view.map.get("Hi").await.unwrap(), None);
        }
        if config.with_set {
            assert!(view.set.contains(&42).await.unwrap());
            assert!(!view.set.contains(&59).await.unwrap());
        }
        if config.with_collection {
            let subview = view.collection.try_load_entry("hola").await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
            assert_eq!(subview.read(..).await.unwrap(), vec![17, 18]);
            assert_eq!(subview.read(1..).await.unwrap(), vec![18]);
            assert_eq!(subview.read(..=0).await.unwrap(), vec![17]);
        }
        if config.with_flush {
            view.save().await.unwrap();
        }
        if config.with_collection {
            let subview = view.collection2.load_entry_mut("ciao").await.unwrap();
            let subsubview = subview.try_load_entry("!").await.unwrap();
            assert!(subview.try_load_entry("!").await.is_err());
            assert_eq!(subsubview.get(), &3);
            assert_eq!(
                view.collection.indices().await.unwrap(),
                vec!["hola".to_string()]
            );
            view.collection.remove_entry("hola").unwrap();
        }
        if config.with_x1
            && config.with_x2
            && config.with_map
            && config.with_set
            && config.with_queue
            && config.with_log
            && config.with_collection
        {
            assert_ne!(view.hash().await.unwrap(), stored_hash);
        }
        view.save().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        if config.with_collection {
            {
                let mut subview = view.collection4.try_load_entry_mut("hola").await.unwrap();
                assert_eq!(subview.read_front(10).await.unwrap(), Vec::<u64>::new());
                assert!(view.collection4.try_load_entry_mut("hola").await.is_err());
                if config.with_queue {
                    subview.push_back(13);
                    assert_eq!(subview.front().await.unwrap(), Some(13));
                    subview.delete_front();
                    assert_eq!(subview.front().await.unwrap(), None);
                    assert_eq!(subview.count(), 0);
                }
            }
            {
                let subview = view.collection4.try_load_entry("hola").await.unwrap();
                assert_eq!(subview.count(), 0);
                assert!(view.collection4.try_load_entry("hola").await.is_ok());
            }
        }
    }
    if config.with_map {
        {
            let mut view = store.load(1).await.unwrap();
            let value = view.map.get_mut_or_default("Geia").await.unwrap();
            assert_eq!(*value, 0);
            *value = 42;
            let value = view.map.get_mut_or_default("Geia").await.unwrap();
            assert_eq!(*value, 42);
            view.save().await.unwrap();
        }
        {
            let view = store.load(1).await.unwrap();
            assert_eq!(view.map.get("Geia").await.unwrap(), Some(42));
        }
        {
            let mut view = store.load(1).await.unwrap();
            let value = view.map.get_mut_or_default("Geia").await.unwrap();
            assert_eq!(*value, 42);
            *value = 43;
            view.rollback();
            let value = view.map.get_mut_or_default("Geia").await.unwrap();
            assert_eq!(*value, 42);
        }
    }
    if config.with_map {
        {
            let mut view = store.load(1).await.unwrap();
            view.map.insert("Konnichiwa", 5).unwrap();
            let value = view.map.get_mut("Konnichiwa").await.unwrap().unwrap();
            *value = 6;
            view.save().await.unwrap();
        }
        {
            let view = store.load(1).await.unwrap();
            assert_eq!(view.map.get("Konnichiwa").await.unwrap(), Some(6));
        }
    }
    {
        let mut view = store.load(1).await.unwrap();
        if config.with_collection {
            let subview = view.collection.try_load_entry("hola").await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), Vec::<u32>::new());
        }
        if config.with_queue {
            assert_eq!(view.queue.front().await.unwrap(), Some(13));
            view.queue.delete_front();
            assert_eq!(view.queue.front().await.unwrap(), None);
            assert_eq!(view.queue.count(), 0);
        }
        view.write_delete().await.unwrap();
    }
    staged_hash
}

#[cfg(test)]
async fn test_views_in_lru_memory_param(config: &TestConfig) {
    tracing::warn!("Testing config {:?} with lru memory", config);
    let mut store = LruMemoryStore::default();
    test_store(&mut store, config).await;
    assert_eq!(store.states.len(), 1);
    let entry = store.states.get(&1).unwrap().clone();
    assert!(entry.lock().await.is_empty());
}

#[tokio::test]
async fn test_views_in_lru_memory() {
    for config in TestConfig::samples() {
        test_views_in_lru_memory_param(&config).await
    }
}

#[cfg(test)]
async fn test_views_in_memory_param(config: &TestConfig) {
    tracing::warn!("Testing config {:?} with memory", config);
    let mut store = MemoryTestStore::default();
    test_store(&mut store, config).await;
    assert_eq!(store.states.len(), 1);
    let entry = store.states.get(&1).unwrap().clone();
    assert!(entry.lock().await.is_empty());
}

#[tokio::test]
async fn test_views_in_memory() {
    for config in TestConfig::samples() {
        test_views_in_memory_param(&config).await
    }
}

#[cfg(test)]
async fn test_views_in_key_value_store_view_memory_param(config: &TestConfig) {
    tracing::warn!(
        "Testing config {:?} with key_value_store_view on memory",
        config
    );
    let mut store = KeyValueStoreTestStore::default();
    test_store(&mut store, config).await;
}

#[tokio::test]
async fn test_views_in_key_value_store_view_memory() {
    for config in TestConfig::samples() {
        test_views_in_key_value_store_view_memory_param(&config).await
    }
}

#[cfg(test)]
async fn test_views_in_rocksdb_param(config: &TestConfig) {
    tracing::warn!("Testing config {:?} with rocksdb", config);
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbClient::new(&dir, TEST_CACHE_SIZE);

    let mut store = RocksdbTestStore::new(client);
    let hash = test_store(&mut store, config).await;
    assert_eq!(store.accessed_chains.len(), 1);

    let mut store = MemoryTestStore::default();
    let hash2 = test_store(&mut store, config).await;
    assert_eq!(hash, hash2);
}

#[tokio::test]
async fn test_views_in_rocksdb() {
    for config in TestConfig::samples() {
        test_views_in_rocksdb_param(&config).await
    }
}

#[cfg(feature = "aws")]
#[tokio::test]
async fn test_views_in_dynamo_db() -> Result<(), anyhow::Error> {
    let mut store = DynamoDbTestStore::new().await?;
    let config = TestConfig::default();
    let hash = test_store(&mut store, &config).await;
    assert_eq!(store.accessed_chains.len(), 1);

    let mut store = MemoryTestStore::default();
    let hash2 = test_store(&mut store, &config).await;
    assert_eq!(hash, hash2);

    Ok(())
}

#[cfg(test)]
async fn test_store_rollback_kernel<S>(store: &mut S)
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    {
        let mut view = store.load(1).await.unwrap();
        view.queue.push_back(8);
        view.map.insert("Hello", 5).unwrap();
        let subview = view.collection.load_entry_mut("hola").await.unwrap();
        subview.push(17);
        view.save().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        view.queue.push_back(7);
        view.map.insert("Hello", 4).unwrap();
        let subview = view.collection.load_entry_mut("DobryDen").await.unwrap();
        subview.push(16);
        view.rollback();
        view.save().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        view.queue.clear();
        view.map.clear();
        view.collection.clear();
        view.rollback();
        view.save().await.unwrap();
    }
    {
        let view = store.load(1).await.unwrap();
        assert_eq!(view.queue.front().await.unwrap(), Some(8));
        assert_eq!(view.map.get("Hello").await.unwrap(), Some(5));
        assert_eq!(
            view.collection.indices().await.unwrap(),
            vec!["hola".to_string()]
        );
    };
}

#[tokio::test]
async fn test_store_rollback() {
    let mut store = MemoryTestStore::default();
    test_store_rollback_kernel(&mut store).await;

    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbClient::new(&dir, TEST_CACHE_SIZE);

    let mut store = RocksdbTestStore::new(client);
    test_store_rollback_kernel(&mut store).await;
}

#[tokio::test]
async fn test_collection_removal() -> anyhow::Result<()> {
    type EntryType = RegisterView<MemoryContext<()>, u8>;
    type CollectionViewType = CollectionView<MemoryContext<()>, u8, EntryType>;

    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let context = MemoryContext::new(state.lock_arc().await, ());

    // Write a dummy entry into the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    let entry = collection.load_entry_mut(&1).await?;
    entry.set(1);
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    // Remove the entry from the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    collection.remove_entry(&1).unwrap();
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    // Check that the entry was removed.
    let collection = CollectionViewType::load(context.clone()).await?;
    assert!(!collection.indices().await?.contains(&1));

    Ok(())
}

async fn test_removal_api_first_second_condition(
    first_condition: bool,
    second_condition: bool,
) -> anyhow::Result<()> {
    type EntryType = RegisterView<MemoryContext<()>, u8>;
    type CollectionViewType = CollectionView<MemoryContext<()>, u8, EntryType>;

    let state = Arc::new(Mutex::new(BTreeMap::new()));
    let context = MemoryContext::new(state.lock_arc().await, ());

    // First add an entry `1` with value `100` and commit
    let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
    let entry = collection.load_entry_mut(&1).await?;
    entry.set(100);
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    // Reload the collection view and remove the entry, but don't commit yet
    let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
    collection.remove_entry(&1).unwrap();

    // Now, read the entry with a different value if a certain condition is true
    if first_condition {
        let entry = collection.load_entry_mut(&1).await?;
        entry.set(200);
    }

    // Finally, either commit or rollback based on some other condition
    if second_condition {
        // If rolling back, then the entry `1` still exists with value `100`.
        collection.rollback();
    }

    // We commit
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    let mut collection: CollectionViewType = CollectionView::load(context.clone()).await?;
    let expected_val = if second_condition {
        Some(100)
    } else if first_condition {
        Some(200)
    } else {
        None
    };
    match expected_val {
        Some(expected_val_i) => {
            let subview = collection.load_entry_mut(&1).await?;
            assert_eq!(subview.get(), &expected_val_i);
        }
        None => {
            assert!(!collection.indices().await?.contains(&1));
        }
    };
    Ok(())
}

#[tokio::test]
async fn test_removal_api() -> anyhow::Result<()> {
    for first_condition in [true, false] {
        for second_condition in [true, false] {
            test_removal_api_first_second_condition(first_condition, second_condition).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
async fn compute_hash_unordered_put_view<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    key_value_vector: Vec<(Vec<u8>, Vec<u8>)>,
) -> <sha3::Sha3_256 as Hasher>::Output
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await.unwrap();
    for key_value in key_value_vector {
        let key = key_value.0;
        let value = key_value.1;
        let key_str = format!("{:?}", &key);
        let value_usize = (*value.first().unwrap()) as usize;
        view.map.insert(&key_str, value_usize).unwrap();
        view.key_value_store.insert(key, value);
        {
            let subview = view.collection.load_entry_mut(&key_str).await.unwrap();
            subview.push(value_usize as u32);
        }
        //
        let thr = rng.gen_range(0..20);
        if thr == 0 {
            view.save().await.unwrap();
        }
    }
    view.hash().await.unwrap()
}

#[cfg(test)]
async fn compute_hash_unordered_putdelete_view<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    operations: Vec<WriteOperation>,
) -> <sha3::Sha3_256 as Hasher>::Output
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await.unwrap();
    for operation in operations {
        match operation {
            Put { key, value } => {
                let key_str = format!("{:?}", &key);
                let first_value = *value.first().unwrap();
                let first_value_usize = first_value as usize;
                let first_value_u64 = first_value as u64;
                let mut tmp = *view.x1.get();
                tmp += first_value_u64;
                view.x1.set(tmp);
                view.map.insert(&key_str, first_value_usize).unwrap();
                view.key_value_store.insert(key, value);
                {
                    let subview = view.collection.load_entry_mut(&key_str).await.unwrap();
                    subview.push(first_value as u32);
                }
            }
            Delete { key } => {
                let key_str = format!("{:?}", &key);
                view.map.remove(&key_str).unwrap();
                view.key_value_store.remove(key);
            }
            DeletePrefix { key_prefix: _ } => {}
        }
        //
        let thr = rng.gen_range(0..10);
        if thr == 0 {
            view.save().await.unwrap();
        }
    }
    view.hash().await.unwrap()
}

#[cfg(test)]
async fn compute_hash_ordered_view<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    key_value_vector: Vec<(Vec<u8>, Vec<u8>)>,
) -> <sha3::Sha3_256 as Hasher>::Output
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await.unwrap();
    for key_value in key_value_vector {
        let value = key_value.1;
        let value_usize = (*value.first().unwrap()) as usize;
        view.log.push(value_usize as u32);
        view.queue.push_back(value_usize as u64);
        //
        let thr = rng.gen_range(0..20);
        if thr == 0 {
            view.save().await.unwrap();
        }
    }
    view.hash().await.unwrap()
}

#[cfg(test)]
async fn compute_hash_view_iter<R: RngCore>(rng: &mut R, n: usize, k: usize) {
    let mut unord1_hashes = Vec::new();
    let mut unord2_hashes = Vec::new();
    let mut ord_hashes = Vec::new();
    let key_value_vector = get_random_key_value_vec(rng, n);
    let info_op = get_random_key_value_operations(rng, n, k);
    let n_iter = 4;
    for _ in 0..n_iter {
        let mut key_value_vector_b = key_value_vector.clone();
        random_shuffle(rng, &mut key_value_vector_b);
        let operations = span_random_reordering_put_delete(rng, info_op.clone());
        //
        let mut store1 = MemoryTestStore::default();
        unord1_hashes
            .push(compute_hash_unordered_put_view(rng, &mut store1, key_value_vector_b).await);
        let mut store2 = MemoryTestStore::default();
        unord2_hashes
            .push(compute_hash_unordered_putdelete_view(rng, &mut store2, operations).await);
        let mut store3 = MemoryTestStore::default();
        ord_hashes
            .push(compute_hash_ordered_view(rng, &mut store3, key_value_vector.clone()).await);
    }
    for i in 1..n_iter {
        assert_eq!(unord1_hashes.get(0).unwrap(), unord1_hashes.get(i).unwrap());
        assert_eq!(unord2_hashes.get(0).unwrap(), unord2_hashes.get(i).unwrap());
        assert_eq!(ord_hashes.get(0).unwrap(), ord_hashes.get(i).unwrap());
    }
}

#[tokio::test]
async fn compute_hash_view_iter_large() {
    let n_iter = 2;
    let n = 100;
    let k = 30;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    for _ in 0..n_iter {
        compute_hash_view_iter(&mut rng, n, k).await;
    }
}

#[cfg(test)]
async fn check_hash_memoization_persistence<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    key_value_vector: Vec<(Vec<u8>, Vec<u8>)>,
) where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut hash = {
        let view = store.load(1).await.unwrap();
        view.hash().await.unwrap()
    };
    for pair in key_value_vector {
        let str0 = format!("{:?}", &pair.0);
        let str1 = format!("{:?}", &pair.1);
        let pair0_first_u8 = *pair.0.first().unwrap();
        let pair1_first_u8 = *pair.1.first().unwrap();
        let thr = rng.gen_range(0..7);
        if thr < 3 {
            let mut view = store.load(1).await.unwrap();
            view.x1.set(pair0_first_u8 as u64);
            view.x2.set(pair1_first_u8 as u32);
            view.log.push(pair0_first_u8 as u32);
            view.log.push(pair1_first_u8 as u32);
            view.queue.push_back(pair0_first_u8 as u64);
            view.queue.push_back(pair1_first_u8 as u64);
            view.map.insert(&str0, pair1_first_u8 as usize).unwrap();
            view.map.insert(&str1, pair0_first_u8 as usize).unwrap();
            view.key_value_store.insert(pair.0.clone(), pair.1.clone());
            if thr == 0 {
                view.rollback();
                let hash_new = view.hash().await.unwrap();
                assert_eq!(hash, hash_new);
            } else {
                let hash_new = view.hash().await.unwrap();
                assert_ne!(hash, hash_new);
                if thr == 2 {
                    view.save().await.unwrap();
                    hash = hash_new;
                }
            }
        }
        if thr == 3 {
            let view = store.load(1).await.unwrap();
            let hash_new = view.hash().await.unwrap();
            assert_eq!(hash, hash_new);
        }
        if thr == 4 {
            let mut view = store.load(1).await.unwrap();
            let subview = view.collection.load_entry_mut(&str0).await.unwrap();
            subview.push(pair1_first_u8 as u32);
            let hash_new = view.hash().await.unwrap();
            assert_ne!(hash, hash_new);
            view.save().await.unwrap();
            hash = hash_new;
        }
        if thr == 5 {
            let mut view = store.load(1).await.unwrap();
            if view.queue.count() > 0 {
                view.queue.delete_front();
                let hash_new = view.hash().await.unwrap();
                assert_ne!(hash, hash_new);
                view.save().await.unwrap();
                hash = hash_new;
            }
        }
        if thr == 6 {
            let mut view = store.load(1).await.unwrap();
            let indices = view.collection.indices().await.unwrap();
            let siz = indices.len();
            if siz > 0 {
                let pos = rng.gen_range(0..siz);
                let x = &indices[pos];
                view.collection.remove_entry(x).unwrap();
                let hash_new = view.hash().await.unwrap();
                assert_ne!(hash, hash_new);
                view.save().await.unwrap();
                hash = hash_new;
            }
        }
    }
}

#[tokio::test]
async fn check_hash_memoization_persistence_large() {
    let n = 100;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let key_value_vector = get_random_key_value_vec(&mut rng, n);
    let mut store = MemoryTestStore::default();
    check_hash_memoization_persistence(&mut rng, &mut store, key_value_vector).await;
}

#[cfg(test)]
async fn check_large_write<S>(store: &mut S, vector: Vec<u8>)
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let hash1 = {
        let mut view = store.load(1).await.unwrap();
        for val in vector {
            view.log.push(val as u32);
        }
        let hash = view.hash().await.unwrap();
        view.save().await.unwrap();
        hash
    };
    let view = store.load(1).await.unwrap();
    let hash2 = view.hash().await.unwrap();
    assert_eq!(hash1, hash2);
}

#[tokio::test]
#[cfg(feature = "aws")]
async fn check_large_write_dynamo_db() -> Result<(), anyhow::Error> {
    // By writing 1000 elements we seriously check the Amazon journaling
    // writing system.
    let n = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut store = DynamoDbTestStore::new().await?;
    check_large_write(&mut store, vector).await;
    Ok(())
}

#[tokio::test]
async fn check_large_write_memory() -> Result<(), anyhow::Error> {
    let n = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(2);
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut store = MemoryTestStore::default();
    check_large_write(&mut store, vector).await;
    Ok(())
}
