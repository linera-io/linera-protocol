// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(any(with_dynamodb, with_rocksdb, with_scylladb))]
use std::collections::BTreeSet;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Result;
use async_lock::{Mutex, RwLock};
use async_trait::async_trait;
#[cfg(with_scylladb)]
use linera_views::scylla_db::{create_scylla_db_test_store, ScyllaDbContext, ScyllaDbStore};
use linera_views::{
    batch::{
        Batch, WriteOperation,
        WriteOperation::{Delete, DeletePrefix, Put},
    },
    collection_view::HashedCollectionView,
    common::Context,
    key_value_store_view::{KeyValueStoreMemoryContext, KeyValueStoreView, ViewContainer},
    log_view::HashedLogView,
    lru_caching::{LruCachingMemoryContext, LruCachingStore},
    map_view::HashedMapView,
    memory::{
        create_memory_context, MemoryContext, MemoryStore, MemoryStoreMap,
        TEST_MEMORY_MAX_STREAM_QUERIES,
    },
    queue_view::HashedQueueView,
    reentrant_collection_view::HashedReentrantCollectionView,
    register_view::HashedRegisterView,
    set_view::HashedSetView,
    test_utils::{
        self, get_random_byte_vector, get_random_key_value_operations, get_random_key_values,
        random_shuffle, span_random_reordering_put_delete,
    },
    views::{CryptoHashRootView, HashableView, Hasher, RootView, View, ViewError},
};
#[cfg(with_dynamodb)]
use linera_views::{
    common::{AdminKeyValueStore, CommonStoreConfig},
    dynamo_db::{
        create_dynamo_db_common_config, DynamoDbContext, DynamoDbStore, DynamoDbStoreConfig,
        LocalStackTestContext,
    },
    test_utils::generate_test_namespace,
};
use rand::{Rng, RngCore};
#[cfg(with_rocksdb)]
use {
    linera_views::rocks_db::{create_rocks_db_test_store, RocksDbContext, RocksDbStore},
    tempfile::TempDir,
};

#[allow(clippy::type_complexity)]
#[derive(CryptoHashRootView)]
pub struct StateView<C> {
    pub x1: HashedRegisterView<C, u64>,
    pub x2: HashedRegisterView<C, u32>,
    pub log: HashedLogView<C, u32>,
    pub map: HashedMapView<C, String, usize>,
    pub set: HashedSetView<C, usize>,
    pub queue: HashedQueueView<C, u64>,
    pub collection: HashedCollectionView<C, String, HashedLogView<C, u32>>,
    pub collection2: HashedCollectionView<
        C,
        String,
        HashedCollectionView<C, String, HashedRegisterView<C, u32>>,
    >,
    pub collection3: HashedCollectionView<C, String, HashedQueueView<C, u64>>,
    pub collection4: HashedReentrantCollectionView<C, String, HashedQueueView<C, u64>>,
    pub key_value_store: KeyValueStoreView<C>,
}

#[async_trait]
pub trait StateStore {
    type Context: Context<Extra = usize> + Clone + Send + Sync + 'static;

    async fn new() -> Self;

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError>;
}

pub struct MemoryTestStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for MemoryTestStore {
    type Context = MemoryContext<usize>;

    async fn new() -> Self {
        MemoryTestStore {
            states: HashMap::new(),
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = state.clone().lock_arc().await;
        let map = Arc::new(RwLock::new(guard));
        let store = MemoryStore {
            map,
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
        };
        let base_key = bcs::to_bytes(&id)?;
        let context = MemoryContext {
            store,
            base_key,
            extra: id,
        };
        StateView::load(context).await
    }
}

pub struct KeyValueStoreTestStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for KeyValueStoreTestStore {
    type Context = KeyValueStoreMemoryContext<usize>;

    async fn new() -> Self {
        KeyValueStoreTestStore {
            states: HashMap::new(),
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = state.clone().lock_arc().await;
        let map = Arc::new(RwLock::new(guard));
        let store = MemoryStore {
            map,
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
        };
        let context = MemoryContext {
            store,
            base_key: Vec::new(),
            extra: (),
        };
        let base_key = bcs::to_bytes(&id)?;
        let store = ViewContainer::new(context).await?;
        let context = KeyValueStoreMemoryContext {
            store,
            base_key,
            extra: id,
        };
        StateView::load(context).await
    }
}

pub struct LruMemoryStore {
    states: HashMap<usize, Arc<Mutex<MemoryStoreMap>>>,
}

#[async_trait]
impl StateStore for LruMemoryStore {
    type Context = LruCachingMemoryContext<usize>;

    async fn new() -> Self {
        LruMemoryStore {
            states: HashMap::new(),
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        tracing::trace!("Acquiring lock on {:?}", id);
        let guard = state.clone().lock_arc().await;
        let map = Arc::new(RwLock::new(guard));
        let store = MemoryStore {
            map,
            max_stream_queries: TEST_MEMORY_MAX_STREAM_QUERIES,
        };
        let n = 1000;
        let store = LruCachingStore::new(store, n);
        let base_key = bcs::to_bytes(&id)?;
        let context = LruCachingMemoryContext {
            store,
            base_key,
            extra: id,
        };
        StateView::load(context).await
    }
}

#[cfg(with_rocksdb)]
pub struct RocksDbTestStore {
    store: RocksDbStore,
    accessed_chains: BTreeSet<usize>,
    _dir: TempDir,
}

#[cfg(with_rocksdb)]
#[async_trait]
impl StateStore for RocksDbTestStore {
    type Context = RocksDbContext<usize>;

    async fn new() -> Self {
        let (store, dir) = create_rocks_db_test_store().await;
        let accessed_chains = BTreeSet::new();
        RocksDbTestStore {
            store,
            accessed_chains,
            _dir: dir,
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        self.accessed_chains.insert(id);
        // TODO(#643): Actually acquire a lock.
        tracing::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&id)?;
        let context = RocksDbContext::new(self.store.clone(), base_key, id);
        StateView::load(context).await
    }
}

#[cfg(with_scylladb)]
pub struct ScyllaDbTestStore {
    store: ScyllaDbStore,
    accessed_chains: BTreeSet<usize>,
}

#[cfg(with_scylladb)]
#[async_trait]
impl StateStore for ScyllaDbTestStore {
    type Context = ScyllaDbContext<usize>;

    async fn new() -> Self {
        let store = create_scylla_db_test_store().await;
        let accessed_chains = BTreeSet::new();
        ScyllaDbTestStore {
            store,
            accessed_chains,
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        self.accessed_chains.insert(id);
        // TODO(#643): Actually acquire a lock.
        tracing::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&id)?;
        let context = ScyllaDbContext::new(self.store.clone(), base_key, id);
        StateView::load(context).await
    }
}

#[cfg(with_dynamodb)]
pub struct DynamoDbTestStore {
    localstack: LocalStackTestContext,
    namespace: String,
    is_created: bool,
    common_config: CommonStoreConfig,
    accessed_chains: BTreeSet<usize>,
}

#[cfg(with_dynamodb)]
#[async_trait]
impl StateStore for DynamoDbTestStore {
    type Context = DynamoDbContext<usize>;

    async fn new() -> Self {
        let localstack = LocalStackTestContext::new().await.expect("localstack");
        let namespace = generate_test_namespace();
        let is_created = false;
        let common_config = create_dynamo_db_common_config();
        let accessed_chains = BTreeSet::new();
        DynamoDbTestStore {
            localstack,
            namespace,
            is_created,
            common_config,
            accessed_chains,
        }
    }

    async fn load(&mut self, id: usize) -> Result<StateView<Self::Context>, ViewError> {
        self.accessed_chains.insert(id);
        // TODO(#643): Actually acquire a lock.
        tracing::trace!("Acquiring lock on {:?}", id);
        let base_key = bcs::to_bytes(&id)?;
        let store_config = DynamoDbStoreConfig {
            config: self.localstack.dynamo_db_config(),
            common_config: self.common_config.clone(),
        };
        let namespace = &self.namespace;

        let store = if self.is_created {
            DynamoDbStore::connect(&store_config, namespace)
                .await
                .expect("failed to connect")
        } else {
            DynamoDbStore::recreate_and_connect(&store_config, namespace)
                .await
                .expect("failed to create from scratch")
        };
        let context = DynamoDbContext::new(store, base_key, id);
        self.is_created = true;
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
async fn test_store<S>(
    store: &mut S,
    config: &TestConfig,
) -> Result<<sha3::Sha3_256 as Hasher>::Output>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let default_hash = {
        let view = store.load(1).await?;
        view.hash().await?
    };
    {
        let mut view = store.load(1).await?;
        if config.with_x1 {
            assert_eq!(view.x1.extra(), &1);
        }
        let hash = view.hash().await?;
        assert_eq!(hash, default_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
            view.x1.set(1);
        }
        view.rollback();
        assert_eq!(view.hash().await?, hash);
        if config.with_x2 {
            view.x2.set(2);
        }
        if config.with_x2 {
            assert_ne!(view.hash().await?, hash);
        }
        if config.with_log {
            view.log.push(4);
        }
        if config.with_queue {
            view.queue.push_back(8);
            assert_eq!(view.queue.front().await?, Some(8));
            view.queue.push_back(7);
            view.queue.delete_front();
        }
        if config.with_map {
            view.map.insert("Hello", 5)?;
            assert_eq!(view.map.indices().await?, vec!["Hello".to_string()]);
            let mut count = 0;
            view.map
                .for_each_index(|_index| {
                    count += 1;
                    Ok(())
                })
                .await?;
            assert_eq!(count, 1);
        }
        if config.with_set {
            view.set.insert(&42)?;
            assert_eq!(view.set.indices().await?, vec![42]);
            let mut count = 0;
            view.set
                .for_each_index(|_index| {
                    count += 1;
                    Ok(())
                })
                .await?;
            assert_eq!(count, 1);
        }
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &2);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await?, vec![4]);
        }
        if config.with_queue {
            assert_eq!(view.queue.read_front(10).await?, vec![7]);
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await?, Some(5));
        }
        if config.with_set {
            assert!(view.set.contains(&42).await?);
        }
        if config.with_collection {
            {
                let subview = view.collection.load_entry_mut("hola").await?;
                subview.push(17);
                subview.push(18);
                assert_eq!(view.collection.indices().await?, vec!["hola".to_string()]);
                let mut count = 0;
                view.collection
                    .for_each_index(|_index| {
                        count += 1;
                        Ok(())
                    })
                    .await?;
                assert_eq!(count, 1);
            }
            let subview = view.collection.try_load_entry("hola").await?.unwrap();
            assert_eq!(subview.read(0..10).await?, vec![17, 18]);
        }
    };
    let staged_hash = {
        let mut view = store.load(1).await?;
        assert_eq!(view.hash().await?, default_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &0);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &0);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await?, Vec::<u32>::new());
        }
        if config.with_queue {
            assert_eq!(view.queue.read_front(10).await?, Vec::<u64>::new());
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await?, None);
        }
        if config.with_set {
            assert!(!view.set.contains(&42).await?);
        }
        if config.with_collection {
            let subview = view.collection.load_entry_or_insert("hola").await?;
            assert_eq!(subview.read(0..10).await?, Vec::<u32>::new());
            let subview = view.collection2.load_entry_mut("ciao").await?;
            let subsubview = subview.load_entry_mut("!").await?;
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
            view.map.insert("Hello", 5)?;
            view.map.insert("Hi", 2)?;
            view.map.remove("Hi")?;
        }
        if config.with_set {
            view.set.insert(&42)?;
            view.set.insert(&59)?;
            view.set.remove(&59)?;
        }
        if config.with_collection {
            let subview = view.collection.load_entry_mut("hola").await?;
            subview.push(17);
            subview.push(18);
        }
        if config.with_flush {
            view.save().await?;
        }
        let hash1 = view.hash().await?;
        let hash2 = view.hash().await?;
        view.save().await?;
        let hash3 = view.hash().await?;
        assert_eq!(hash1, hash2);
        assert_eq!(hash1, hash3);
        hash1
    };
    {
        let mut view = store.load(1).await?;
        let stored_hash = view.hash().await?;
        assert_eq!(staged_hash, stored_hash);
        if config.with_x1 {
            assert_eq!(view.x1.get(), &1);
        }
        if config.with_x2 {
            assert_eq!(view.x2.get(), &0);
        }
        if config.with_log {
            assert_eq!(view.log.read(0..10).await?, vec![4]);
        }
        if config.with_queue {
            view.queue.push_back(8);
            assert_eq!(view.queue.read_front(10).await?, vec![7, 8]);
            assert_eq!(view.queue.read_front(1).await?, vec![7]);
            assert_eq!(view.queue.read_back(10).await?, vec![7, 8]);
            assert_eq!(view.queue.read_back(1).await?, vec![8]);
            assert_eq!(view.queue.front().await?, Some(7));
            assert_eq!(view.queue.back().await?, Some(8));
            assert_eq!(view.queue.count(), 2);
            view.queue.delete_front();
            assert_eq!(view.queue.front().await?, Some(8));
            view.queue.delete_front();
            assert_eq!(view.queue.front().await?, None);
            assert_eq!(view.queue.count(), 0);
            view.queue.push_back(13);
        }
        if config.with_map {
            assert_eq!(view.map.get("Hello").await?, Some(5));
            assert_eq!(view.map.get("Hi").await?, None);
        }
        if config.with_set {
            assert!(view.set.contains(&42).await?);
            assert!(!view.set.contains(&59).await?);
        }
        if config.with_collection {
            let subview = view.collection.try_load_entry("hola").await?.unwrap();
            assert_eq!(subview.read(0..10).await?, vec![17, 18]);
            assert_eq!(subview.read(..).await?, vec![17, 18]);
            assert_eq!(subview.read(1..).await?, vec![18]);
            assert_eq!(subview.read(..=0).await?, vec![17]);
        }
        if config.with_flush {
            view.save().await?;
        }
        if config.with_collection {
            let subview = view.collection2.load_entry_mut("ciao").await?;
            let subsubview = subview.try_load_entry("!").await?.unwrap();
            assert!(subview.try_load_entry("!").await.is_err());
            assert_eq!(subsubview.get(), &3);
            assert_eq!(view.collection.indices().await?, vec!["hola".to_string()]);
            view.collection.remove_entry("hola")?;
        }
        if config.with_x1
            && config.with_x2
            && config.with_map
            && config.with_set
            && config.with_queue
            && config.with_log
            && config.with_collection
        {
            assert_ne!(view.hash().await?, stored_hash);
        }
        view.save().await?;
    }
    {
        let mut view = store.load(1).await?;
        if config.with_collection {
            {
                let mut subview = view.collection4.try_load_entry_mut("hola").await?;
                assert_eq!(subview.read_front(10).await?, Vec::<u64>::new());
                assert!(view.collection4.try_load_entry_mut("hola").await.is_err());
                if config.with_queue {
                    subview.push_back(13);
                    assert_eq!(subview.front().await?, Some(13));
                    subview.delete_front();
                    assert_eq!(subview.front().await?, None);
                    assert_eq!(subview.count(), 0);
                }
            }
            {
                let subview = view.collection4.try_load_entry("hola").await?.unwrap();
                assert_eq!(subview.count(), 0);
                assert!(view.collection4.try_load_entry("hola").await.is_ok());
            }
        }
    }
    if config.with_map {
        {
            let mut view = store.load(1).await?;
            let value = view.map.get_mut_or_default("Geia").await?;
            assert_eq!(*value, 0);
            *value = 42;
            let value = view.map.get_mut_or_default("Geia").await?;
            assert_eq!(*value, 42);
            view.save().await?;
        }
        {
            let view = store.load(1).await?;
            assert_eq!(view.map.get("Geia").await?, Some(42));
        }
        {
            let mut view = store.load(1).await?;
            let value = view.map.get_mut_or_default("Geia").await?;
            assert_eq!(*value, 42);
            *value = 43;
            view.rollback();
            let value = view.map.get_mut_or_default("Geia").await?;
            assert_eq!(*value, 42);
        }
    }
    if config.with_map {
        {
            let mut view = store.load(1).await?;
            view.map.insert("Konnichiwa", 5)?;
            let value = view.map.get_mut("Konnichiwa").await?.unwrap();
            *value = 6;
            view.save().await?;
        }
        {
            let view = store.load(1).await?;
            assert_eq!(view.map.get("Konnichiwa").await?, Some(6));
        }
    }
    {
        let mut view = store.load(1).await?;
        if config.with_collection {
            let subview = view.collection.load_entry_or_insert("hola").await?;
            assert_eq!(subview.read(0..10).await?, Vec::<u32>::new());
        }
        if config.with_queue {
            assert_eq!(view.queue.front().await?, Some(13));
            view.queue.delete_front();
            assert_eq!(view.queue.front().await?, None);
            assert_eq!(view.queue.count(), 0);
        }
        view.clear();
        view.save().await?;
    }
    Ok(staged_hash)
}

#[cfg(test)]
async fn test_views_in_lru_memory_param(config: &TestConfig) -> Result<()> {
    tracing::warn!("Testing config {:?} with lru memory", config);
    let mut store = LruMemoryStore::new().await;
    test_store(&mut store, config).await?;
    assert_eq!(store.states.len(), 1);
    let entry = store.states.get(&1).unwrap().clone();
    assert!(entry.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_views_in_lru_memory() -> Result<()> {
    for config in TestConfig::samples() {
        test_views_in_lru_memory_param(&config).await?;
    }
    Ok(())
}

#[cfg(test)]
async fn test_views_in_memory_param(config: &TestConfig) -> Result<()> {
    tracing::warn!("Testing config {:?} with memory", config);
    let mut store = MemoryTestStore::new().await;
    test_store(&mut store, config).await?;
    assert_eq!(store.states.len(), 1);
    let entry = store.states.get(&1).unwrap().clone();
    assert!(entry.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_views_in_memory() -> Result<()> {
    for config in TestConfig::samples() {
        test_views_in_memory_param(&config).await?;
    }
    Ok(())
}

#[cfg(test)]
async fn test_views_in_key_value_store_view_memory_param(config: &TestConfig) -> Result<()> {
    tracing::warn!(
        "Testing config {:?} with key_value_store_view on memory",
        config
    );
    let mut store = KeyValueStoreTestStore::new().await;
    test_store(&mut store, config).await?;
    Ok(())
}

#[tokio::test]
async fn test_views_in_key_value_store_view_memory() -> Result<()> {
    for config in TestConfig::samples() {
        test_views_in_key_value_store_view_memory_param(&config).await?;
    }
    Ok(())
}

#[cfg(with_rocksdb)]
#[cfg(test)]
async fn test_views_in_rocks_db_param(config: &TestConfig) -> Result<()> {
    tracing::warn!("Testing config {:?} with rocks_db", config);

    let mut store = RocksDbTestStore::new().await;
    let hash = test_store(&mut store, config).await?;
    assert_eq!(store.accessed_chains.len(), 1);

    let mut store = MemoryTestStore::new().await;
    let hash2 = test_store(&mut store, config).await?;
    assert_eq!(hash, hash2);
    Ok(())
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_views_in_rocks_db() {
    for config in TestConfig::samples() {
        test_views_in_rocks_db_param(&config).await
    }
}

#[cfg(with_scylladb)]
#[cfg(test)]
async fn test_views_in_scylla_db_param(config: &TestConfig) -> Result<()> {
    tracing::warn!("Testing config {:?} with scylla_db", config);

    let mut store = ScyllaDbTestStore::new().await;
    let hash = test_store(&mut store, config).await?;
    assert_eq!(store.accessed_chains.len(), 1);

    let mut store = MemoryTestStore::new().await;
    let hash2 = test_store(&mut store, config).await?;
    assert_eq!(hash, hash2);
    Ok(())
}

#[cfg(with_scylladb)]
#[tokio::test]
async fn test_views_in_scylla_db() {
    for config in TestConfig::samples() {
        test_views_in_scylla_db_param(&config).await
    }
}

#[cfg(with_dynamodb)]
#[tokio::test]
async fn test_views_in_dynamo_db() -> Result<()> {
    let mut store = DynamoDbTestStore::new().await;
    let config = TestConfig::default();
    let hash = test_store(&mut store, &config).await?;
    assert_eq!(store.accessed_chains.len(), 1);

    let mut store = MemoryTestStore::new().await;
    let hash2 = test_store(&mut store, &config).await?;
    assert_eq!(hash, hash2);
    Ok(())
}

#[cfg(with_rocksdb)]
#[cfg(test)]
async fn test_store_rollback_kernel<S>(store: &mut S) -> Result<()>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    {
        let mut view = store.load(1).await?;
        view.queue.push_back(8);
        view.map.insert("Hello", 5)?;
        let subview = view.collection.load_entry_mut("hola").await?;
        subview.push(17);
        view.save().await?;
    }
    {
        let mut view = store.load(1).await?;
        view.queue.push_back(7);
        view.map.insert("Hello", 4)?;
        let subview = view.collection.load_entry_mut("DobryDen").await?;
        subview.push(16);
        view.rollback();
        view.save().await?;
    }
    {
        let mut view = store.load(1).await?;
        view.queue.clear();
        view.map.clear();
        view.collection.clear();
        view.rollback();
        view.save().await?;
    }
    {
        let view = store.load(1).await?;
        assert_eq!(view.queue.front().await?, Some(8));
        assert_eq!(view.map.get("Hello").await?, Some(5));
        assert_eq!(view.collection.indices().await?, vec!["hola".to_string()]);
    };
}

#[cfg(with_rocksdb)]
#[tokio::test]
async fn test_store_rollback() -> Result<()> {
    let mut store = MemoryTestStore::new().await;
    test_store_rollback_kernel(&mut store).await?;

    let mut store = RocksDbTestStore::new().await;
    test_store_rollback_kernel(&mut store).await?;
    Ok(())
}

#[tokio::test]
async fn test_collection_removal() -> Result<()> {
    type EntryType = HashedRegisterView<MemoryContext<()>, u8>;
    type CollectionViewType = HashedCollectionView<MemoryContext<()>, u8, EntryType>;

    let context = create_memory_context();

    // Write a dummy entry into the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    let entry = collection.load_entry_mut(&1).await?;
    entry.set(1);
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    // Remove the entry from the collection.
    let mut collection = CollectionViewType::load(context.clone()).await?;
    collection.remove_entry(&1)?;
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
) -> Result<()> {
    type EntryType = HashedRegisterView<MemoryContext<()>, u8>;
    type CollectionViewType = HashedCollectionView<MemoryContext<()>, u8, EntryType>;

    let context = create_memory_context();

    // First add an entry `1` with value `100` and commit
    let mut collection: CollectionViewType = HashedCollectionView::load(context.clone()).await?;
    let entry = collection.load_entry_mut(&1).await?;
    entry.set(100);
    let mut batch = Batch::new();
    collection.flush(&mut batch)?;
    collection.context().write_batch(batch).await?;

    // Reload the collection view and remove the entry, but don't commit yet
    let mut collection: CollectionViewType = HashedCollectionView::load(context.clone()).await?;
    collection.remove_entry(&1)?;

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

    let mut collection: CollectionViewType = HashedCollectionView::load(context.clone()).await?;
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
async fn test_removal_api() -> Result<()> {
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
) -> Result<<sha3::Sha3_256 as Hasher>::Output>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await?;
    for key_value in key_value_vector {
        let key = key_value.0;
        let value = key_value.1;
        let key_str = format!("{:?}", &key);
        let value_usize = (*value.first().unwrap()) as usize;
        view.map.insert(&key_str, value_usize)?;
        view.key_value_store.insert(key, value).await?;
        {
            let subview = view.collection.load_entry_mut(&key_str).await?;
            subview.push(value_usize as u32);
        }
        //
        let choice = rng.gen_range(0..20);
        if choice == 0 {
            view.save().await?;
        }
    }
    Ok(view.hash().await?)
}

#[cfg(test)]
async fn compute_hash_unordered_putdelete_view<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    operations: Vec<WriteOperation>,
) -> Result<<sha3::Sha3_256 as Hasher>::Output>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await?;
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
                view.map.insert(&key_str, first_value_usize)?;
                view.key_value_store.insert(key, value).await?;
                {
                    let subview = view.collection.load_entry_mut(&key_str).await?;
                    subview.push(first_value as u32);
                }
            }
            Delete { key } => {
                let key_str = format!("{:?}", &key);
                view.map.remove(&key_str)?;
                view.key_value_store.remove(key).await?;
            }
            DeletePrefix { key_prefix: _ } => {}
        }
        //
        let choice = rng.gen_range(0..10);
        if choice == 0 {
            view.save().await?;
        }
    }
    Ok(view.hash().await?)
}

#[cfg(test)]
async fn compute_hash_ordered_view<S>(
    rng: &mut impl RngCore,
    store: &mut S,
    key_value_vector: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<<sha3::Sha3_256 as Hasher>::Output>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut view = store.load(1).await?;
    for key_value in key_value_vector {
        let value = key_value.1;
        let value_usize = (*value.first().unwrap()) as usize;
        view.log.push(value_usize as u32);
        view.queue.push_back(value_usize as u64);
        //
        let choice = rng.gen_range(0..20);
        if choice == 0 {
            view.save().await?;
        }
    }
    Ok(view.hash().await?)
}

#[cfg(test)]
async fn compute_hash_view_iter<R: RngCore>(rng: &mut R, n: usize, k: usize) -> Result<()> {
    let mut unord1_hashes = Vec::new();
    let mut unord2_hashes = Vec::new();
    let mut ord_hashes = Vec::new();
    let key_value_vector = get_random_key_values(rng, n);
    let info_op = get_random_key_value_operations(rng, n, k);
    let n_iter = 4;
    for _ in 0..n_iter {
        let mut key_value_vector_b = key_value_vector.clone();
        random_shuffle(rng, &mut key_value_vector_b);
        let operations = span_random_reordering_put_delete(rng, info_op.clone());
        //
        let mut store1 = MemoryTestStore::new().await;
        unord1_hashes
            .push(compute_hash_unordered_put_view(rng, &mut store1, key_value_vector_b).await?);
        let mut store2 = MemoryTestStore::new().await;
        unord2_hashes
            .push(compute_hash_unordered_putdelete_view(rng, &mut store2, operations).await?);
        let mut store3 = MemoryTestStore::new().await;
        ord_hashes
            .push(compute_hash_ordered_view(rng, &mut store3, key_value_vector.clone()).await?);
    }
    for i in 1..n_iter {
        assert_eq!(
            unord1_hashes.first().unwrap(),
            unord1_hashes.get(i).unwrap(),
        );
        assert_eq!(
            unord2_hashes.first().unwrap(),
            unord2_hashes.get(i).unwrap(),
        );
        assert_eq!(ord_hashes.first().unwrap(), ord_hashes.get(i).unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn compute_hash_view_iter_large() -> Result<()> {
    let n_iter = 2;
    let n = 100;
    let k = 30;
    let mut rng = test_utils::make_deterministic_rng();
    for _ in 0..n_iter {
        compute_hash_view_iter(&mut rng, n, k).await?;
    }
    Ok(())
}

#[cfg(test)]
async fn check_hash_memoization_persistence<S>(rng: &mut impl RngCore, store: &mut S, key_value_vector: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let mut hash = {
        let view = store.load(1).await?;
        view.hash().await?
    };
    for pair in key_value_vector {
        let str0 = format!("{:?}", &pair.0);
        let str1 = format!("{:?}", &pair.1);
        let pair0_first_u8 = *pair.0.first().unwrap();
        let pair1_first_u8 = *pair.1.first().unwrap();
        let choice = rng.gen_range(0..7);
        if choice < 3 {
            let mut view = store.load(1).await?;
            view.x1.set(pair0_first_u8 as u64);
            view.x2.set(pair1_first_u8 as u32);
            view.log.push(pair0_first_u8 as u32);
            view.log.push(pair1_first_u8 as u32);
            view.queue.push_back(pair0_first_u8 as u64);
            view.queue.push_back(pair1_first_u8 as u64);
            view.map.insert(&str0, pair1_first_u8 as usize)?;
            view.map.insert(&str1, pair0_first_u8 as usize)?;
            view.key_value_store
                .insert(pair.0.clone(), pair.1.clone())
                .await?;
            if choice == 0 {
                view.rollback();
                let hash_new = view.hash().await?;
                assert_eq!(hash, hash_new);
            } else {
                let hash_new = view.hash().await?;
                assert_ne!(hash, hash_new);
                if choice == 2 {
                    view.save().await?;
                    hash = hash_new;
                }
            }
        }
        if choice == 3 {
            let view = store.load(1).await?;
            let hash_new = view.hash().await?;
            assert_eq!(hash, hash_new);
        }
        if choice == 4 {
            let mut view = store.load(1).await?;
            let subview = view.collection.load_entry_mut(&str0).await?;
            subview.push(pair1_first_u8 as u32);
            let hash_new = view.hash().await?;
            assert_ne!(hash, hash_new);
            view.save().await?;
            hash = hash_new;
        }
        if choice == 5 {
            let mut view = store.load(1).await?;
            if view.queue.count() > 0 {
                view.queue.delete_front();
                let hash_new = view.hash().await?;
                assert_ne!(hash, hash_new);
                view.save().await?;
                hash = hash_new;
            }
        }
        if choice == 6 {
            let mut view = store.load(1).await?;
            let indices = view.collection.indices().await?;
            let size = indices.len();
            if size > 0 {
                let pos = rng.gen_range(0..size);
                let x = &indices[pos];
                view.collection.remove_entry(x)?;
                let hash_new = view.hash().await?;
                assert_ne!(hash, hash_new);
                view.save().await?;
                hash = hash_new;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn check_hash_memoization_persistence_large() -> Result<()> {
    let n = 100;
    let mut rng = test_utils::make_deterministic_rng();
    let key_value_vector = get_random_key_values(&mut rng, n);
    let mut store = MemoryTestStore::new().await;
    check_hash_memoization_persistence(&mut rng, &mut store, key_value_vector).await
}

#[cfg(test)]
async fn check_large_write<S>(store: &mut S, vector: Vec<u8>) -> Result<()>
where
    S: StateStore,
    ViewError: From<<<S as StateStore>::Context as Context>::Error>,
{
    let hash1 = {
        let mut view = store.load(1).await?;
        for val in vector {
            view.log.push(val as u32);
        }
        let hash = view.hash().await?;
        view.save().await?;
        hash
    };
    let view = store.load(1).await?;
    let hash2 = view.hash().await?;
    assert_eq!(hash1, hash2);
    Ok(())
}

#[tokio::test]
#[cfg(with_dynamodb)]
async fn check_large_write_dynamo_db() -> Result<()> {
    // By writing 1000 elements we seriously check the Amazon journaling
    // writing system.
    let n = 1000;
    let mut rng = test_utils::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut store = DynamoDbTestStore::new().await;
    check_large_write(&mut store, vector).await
}

#[tokio::test]
async fn check_large_write_memory() -> Result<()> {
    let n = 1000;
    let mut rng = test_utils::make_deterministic_rng();
    let vector = get_random_byte_vector(&mut rng, &[], n);
    let mut store = MemoryTestStore::new().await;
    check_large_write(&mut store, vector).await
}
