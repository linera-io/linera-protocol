// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_views::{
    hash::{HashView, Hasher, HashingContext},
    memory::{EntryMap, InMemoryContext, MemoryViewError},
    rocksdb::{RocksdbContext, RocksdbViewError},
    views::{
        AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations, CollectionView, Context,
        MapOperations, MapView, QueueOperations, QueueView, RegisterOperations, RegisterView,
        ScopedOperations, ScopedView, View,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    io::Write,
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

#[async_trait]
impl<C> View<C> for StateView<C>
where
    C: Context
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
        + ScopedOperations,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        let x1 = ScopedView::load(context.clone()).await?;
        let x2 = ScopedView::load(context.clone()).await?;
        let log = ScopedView::load(context.clone()).await?;
        let map = ScopedView::load(context.clone()).await?;
        let queue = ScopedView::load(context.clone()).await?;
        let collection = ScopedView::load(context).await?;
        Ok(Self {
            x1,
            x2,
            log,
            map,
            queue,
            collection,
        })
    }

    fn reset_changes(&mut self) {
        self.x1.reset_changes();
        self.x2.reset_changes();
        self.log.reset_changes();
        self.map.reset_changes();
        self.queue.reset_changes();
        self.collection.reset_changes();
    }

    async fn commit(self) -> Result<(), C::Error> {
        self.x1.commit().await?;
        self.x2.commit().await?;
        self.log.commit().await?;
        self.map.commit().await?;
        self.queue.commit().await?;
        self.collection.commit().await?;
        Ok(())
    }

    async fn delete(self) -> Result<(), C::Error> {
        self.x1.delete().await?;
        self.x2.delete().await?;
        self.log.delete().await?;
        self.map.delete().await?;
        self.queue.delete().await?;
        self.collection.delete().await?;
        Ok(())
    }
}

#[async_trait]
impl<C> HashView<C> for StateView<C>
where
    C: HashingContext
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
        + ScopedOperations,
{
    async fn hash(&mut self) -> Result<<C::Hasher as Hasher>::Output, C::Error> {
        let mut hasher = C::Hasher::default();
        hasher.write_all(self.x1.hash().await?.as_ref())?;
        hasher.write_all(self.x2.hash().await?.as_ref())?;
        hasher.write_all(self.log.hash().await?.as_ref())?;
        hasher.write_all(self.map.hash().await?.as_ref())?;
        hasher.write_all(self.queue.hash().await?.as_ref())?;
        hasher.write_all(self.collection.hash().await?.as_ref())?;
        Ok(hasher.finalize())
    }
}

#[async_trait]
pub trait Store<Key> {
    type View;
    type Error: Debug;

    async fn load(&mut self, id: Key) -> Result<Self::View, Self::Error>;
}

pub trait StateStore: Store<usize, View = StateView<<Self as StateStore>::Context>> {
    type Context: HashingContext
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
}

#[derive(Default)]
pub struct InMemoryTestStore {
    states: HashMap<usize, Arc<Mutex<EntryMap>>>,
}

impl StateStore for InMemoryTestStore {
    type Context = InMemoryContext;
}

#[async_trait]
impl Store<usize> for InMemoryTestStore {
    type View = StateView<InMemoryContext>;
    type Error = MemoryViewError;

    async fn load(&mut self, id: usize) -> Result<Self::View, Self::Error> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(BTreeMap::new())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = InMemoryContext::new(state.clone().lock_owned().await);
        Self::View::load(context).await
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

impl StateStore for RocksdbTestStore {
    type Context = RocksdbContext;
}

#[async_trait]
impl Store<usize> for RocksdbTestStore {
    type View = StateView<RocksdbContext>;
    type Error = RocksdbViewError;

    async fn load(&mut self, id: usize) -> Result<Self::View, Self::Error> {
        let lock = self
            .locks
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = RocksdbContext::new(
            self.db.clone(),
            lock.clone().lock_owned().await,
            bcs::to_bytes(&id)?,
        );
        Self::View::load(context).await
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
        assert_eq!(view.queue.read_front(10).await.unwrap(), vec![7]);
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
    let mut store = InMemoryTestStore::default();
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
