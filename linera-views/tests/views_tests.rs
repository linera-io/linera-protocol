// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_views::{
    memory::{EntryMap, InMemoryContext},
    views::{
        AppendOnlyLogKey, AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations,
        CollectionView, Context, MapOperations, MapView, RegisterOperations, RegisterView, View,
    },
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::Mutex;

pub struct StateView<C> {
    pub id: usize,
    pub x1: RegisterView<C, u64>,
    pub x2: RegisterView<C, u32>,
    pub log: AppendOnlyLogView<C, u32>,
    pub map: MapView<C, String, usize>,
    pub collection: CollectionView<C, String, AppendOnlyLogKey<u32>, AppendOnlyLogView<C, u32>>,
}

impl<C> StateView<C>
where
    C: Context
        + Send
        + Sync
        + Clone
        + RegisterOperations<u64>
        + RegisterOperations<u32>
        + AppendOnlyLogOperations<u32>
        + MapOperations<String, usize>
        + CollectionOperations<String, AppendOnlyLogKey<u32>>,
{
    pub fn reset(&mut self) {
        self.x1.reset();
        self.x2.reset();
        self.log.reset();
        self.map.reset();
        self.collection.reset();
    }

    pub async fn commit(self) -> Result<(), C::Error> {
        self.x1.commit().await?;
        self.x2.commit().await?;
        self.log.commit().await?;
        self.map.commit().await?;
        self.collection.commit().await?;
        Ok(())
    }
}

#[async_trait]
pub trait Store<Key> {
    type View;
    type Error: Debug;

    async fn load(&mut self, id: Key) -> Result<Self::View, Self::Error>;
}

pub trait StateStore: Store<usize, View = StateView<<Self as StateStore>::C>> {
    type C: Context
        + Send
        + Sync
        + Clone
        + RegisterOperations<u64>
        + RegisterOperations<u32>
        + AppendOnlyLogOperations<u32>
        + MapOperations<String, usize>
        + CollectionOperations<String, AppendOnlyLogKey<u32>>;
}

#[derive(Default)]
pub struct InMemoryTestStore {
    states: HashMap<usize, Arc<Mutex<EntryMap>>>,
}

pub type InMemoryStateView = StateView<InMemoryContext>;

impl InMemoryStateView {
    async fn load(id: usize, context: InMemoryContext) -> Result<Self, std::convert::Infallible> {
        let x1 = RegisterView::load(context.clone(), vec![0].into()).await?;
        let x2 = RegisterView::load(context.clone(), vec![1].into()).await?;
        let log = AppendOnlyLogView::load(context.clone(), vec![2].into()).await?;
        let map = MapView::load(context.clone(), vec![3].into()).await?;
        let collection = CollectionView::load(context, vec![4].into()).await?;
        Ok(Self {
            id,
            x1,
            x2,
            log,
            map,
            collection,
        })
    }
}

#[async_trait]
impl Store<usize> for InMemoryTestStore {
    type View = InMemoryStateView;
    type Error = std::convert::Infallible;

    async fn load(&mut self, id: usize) -> Result<Self::View, Self::Error> {
        let state = self
            .states
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())));
        log::trace!("Acquiring lock on {:?}", id);
        let context = InMemoryContext::new(state.clone().lock_owned().await);
        Self::View::load(id, context).await
    }
}

impl StateStore for InMemoryTestStore {
    type C = InMemoryContext;
}

#[cfg(test)]
async fn test_store<S>(mut store: S)
where
    S: StateStore,
{
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1.get(), &0);
        view.x1.set(1);
        view.reset();
        view.x2.set(2);
        view.log.push(4);
        view.map.insert("Hello".to_string(), 5);
        assert_eq!(view.x1.get(), &0);
        assert_eq!(view.x2.get(), &2);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        assert_eq!(view.map.get("Hello").await.unwrap(), Some(5));
        {
            let subview = view.collection.view("hola".to_string()).await.unwrap();
            subview.push(17);
            subview.push(18);
        }
        {
            let subview = view.collection.view("hola".to_string()).await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    }
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1.get(), &0);
        assert_eq!(view.x2.get(), &0);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![]);
        assert_eq!(view.map.get("Hello").await.unwrap(), None);
        {
            let subview = view.collection.view("hola".to_string()).await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![]);
        }
        view.x1.set(1);
        view.log.push(4);
        view.map.insert("Hello".to_string(), 5);
        view.map.insert("Hi".to_string(), 2);
        view.map.remove("Hi".to_string());
        {
            let subview = view.collection.view("hola".to_string()).await.unwrap();
            subview.push(17);
            subview.push(18);
        }
        view.commit().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1.get(), &1);
        assert_eq!(view.x2.get(), &0);
        assert_eq!(view.log.read(0..10).await.unwrap(), vec![4]);
        assert_eq!(view.map.get("Hello").await.unwrap(), Some(5));
        assert_eq!(view.map.get("Hi").await.unwrap(), None);
        {
            let subview = view.collection.view("hola".to_string()).await.unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    }
}

#[tokio::test]
async fn test_traits() {
    test_store(InMemoryTestStore::default()).await;
}
