// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_views::{
    hash::{HashView, Hasher, HashingContext},
    impl_view,
    memory::{EntryMap, MemoryContext, MemoryViewError},
    rocksdb::{RocksdbContext, RocksdbViewError},
    views::{
        AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations, CollectionView, Context,
        MapOperations, MapView, QueueOperations, QueueView, RegisterOperations, RegisterView,
        ScopedOperations, ScopedView, View,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

pub struct StateView<C> {
    pub x1: ScopedView<0, RegisterView<C, u64>>,
    pub x2: ScopedView<1, RegisterView<C, u32>>,
    pub log: ScopedView<2, AppendOnlyLogView<C, u32>>,
    pub map: ScopedView<3, MapView<C, String, usize>>,
    pub queue: ScopedView<4, QueueView<C, u64>>,
    pub collection: ScopedView<5, CollectionView<C, String, AppendOnlyLogView<C, u32>>>,
}

impl_view!(StateView { x1, x2, log, map, queue, collection };
           RegisterOperations<u64>,
           RegisterOperations<u32>,
           AppendOnlyLogOperations<u32>,
           MapOperations<String, usize>,
           QueueOperations<u64>,
           CollectionOperations<String>
);

#[async_trait]
pub trait StateStore {
    type Context: Context<Extra = usize>
        + HashingContext
        + Send
        + Sync
        + Clone
        + 'static
        + RegisterOperations<u64>
        + RegisterOperations<u32>
        + AppendOnlyLogOperations<u32>
        + MapOperations<String, usize>
        + QueueOperations<u64>
        + CollectionOperations<String>
        + ScopedOperations;

    async fn load(
        &mut self,
        id: usize,
    ) -> Result<StateView<Self::Context>, <Self::Context as Context>::Error>;
}

#[derive(Default)]
pub struct MemoryTestStore {
    states: HashMap<usize, Arc<Mutex<EntryMap>>>,
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
    db: Arc<rocksdb::DB>,
    locks: HashMap<usize, Arc<Mutex<()>>>,
}

impl RocksdbTestStore {
    fn new(db: rocksdb::DB) -> Self {
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

#[cfg(test)]
async fn test_store<S>(store: &mut S)
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
        view.reset_changes();
        assert_eq!(view.hash().await.unwrap(), hash);
        view.x2.set(2);
        assert!(view.hash().await.unwrap() != hash);
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
    let stored_hash = {
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
        view.commit().await.unwrap();
        hash
    };
    {
        let mut view = store.load(1).await.unwrap();
        let hash = view.hash().await.unwrap();
        assert_eq!(hash, stored_hash);
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
        view.collection.remove_entry("hola".to_string());
        assert!(view.hash().await.unwrap() != hash);
        view.commit().await.unwrap();
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
        view.delete().await.unwrap();
    }
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
    let db = rocksdb::DB::open(&options, dir).unwrap();
    let mut store = RocksdbTestStore::new(db);
    test_store(&mut store).await;
    assert_eq!(store.locks.len(), 1);
}
