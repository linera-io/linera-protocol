// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use getset::{Getters, MutGetters};
use linera_views::{
    memory::{EntryMap, InMemoryContext},
    views::{
        AppendOnlyLogOperations, AppendOnlyLogView, CollectionOperations, CollectionView, Context,
        MapOperations, MapView, RegisterOperations, RegisterView, ScopedOperations, ScopedView,
        View,
    },
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::Mutex;

#[derive(Getters, MutGetters)]
pub struct StateView<C> {
    #[getset(get = "pub", get_mut = "pub")]
    x1: ScopedView<0, RegisterView<C, u64>>,
    #[getset(get = "pub", get_mut = "pub")]
    x2: ScopedView<1, RegisterView<C, u32>>,
    #[getset(get = "pub", get_mut = "pub")]
    log: ScopedView<2, AppendOnlyLogView<C, u32>>,
    #[getset(get = "pub", get_mut = "pub")]
    map: ScopedView<3, MapView<C, String, usize>>,
    #[getset(get = "pub", get_mut = "pub")]
    collection: ScopedView<4, CollectionView<C, String, AppendOnlyLogView<C, u32>>>,
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
        + CollectionOperations<String>
        + ScopedOperations,
{
    async fn load(context: C) -> Result<Self, C::Error> {
        let x1 = ScopedView::load(context.clone()).await?;
        let x2 = ScopedView::load(context.clone()).await?;
        let log = ScopedView::load(context.clone()).await?;
        let map = ScopedView::load(context.clone()).await?;
        let collection = ScopedView::load(context).await?;
        Ok(Self {
            x1,
            x2,
            log,
            map,
            collection,
        })
    }

    fn reset(&mut self) {
        self.x1.reset();
        self.x2.reset();
        self.log.reset();
        self.map.reset();
        self.collection.reset();
    }

    async fn commit(self) -> Result<(), C::Error> {
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
        + 'static
        + RegisterOperations<u64>
        + RegisterOperations<u32>
        + AppendOnlyLogOperations<u32>
        + MapOperations<String, usize>
        + CollectionOperations<String>
        + ScopedOperations;
}

#[derive(Default)]
pub struct InMemoryTestStore {
    states: HashMap<usize, Arc<Mutex<EntryMap>>>,
}

pub type InMemoryStateView = StateView<InMemoryContext>;

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
        Self::View::load(context).await
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
        assert_eq!(view.x1().get(), &0);
        view.x1_mut().set(1);
        view.reset();
        view.x2_mut().set(2);
        view.log_mut().push(4);
        view.map_mut().insert("Hello".to_string(), 5);
        assert_eq!(view.x1().get(), &0);
        assert_eq!(view.x2().get(), &2);
        assert_eq!(view.log_mut().read(0..10).await.unwrap(), vec![4]);
        assert_eq!(view.map_mut().get("Hello").await.unwrap(), Some(5));
        {
            let subview = view
                .collection_mut()
                .load_entry("hola".to_string())
                .await
                .unwrap();
            subview.push(17);
            subview.push(18);
        }
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    }
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1().get(), &0);
        assert_eq!(view.x2().get(), &0);
        assert_eq!(view.log_mut().read(0..10).await.unwrap(), vec![]);
        assert_eq!(view.map_mut().get("Hello").await.unwrap(), None);
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![]);
        }
        view.x1_mut().set(1);
        view.log_mut().push(4);
        view.map_mut().insert("Hello".to_string(), 5);
        view.map_mut().insert("Hi".to_string(), 2);
        view.map_mut().remove("Hi".to_string());
        {
            let subview = view
                .collection
                .load_entry("hola".to_string())
                .await
                .unwrap();
            subview.push(17);
            subview.push(18);
        }
        view.commit().await.unwrap();
    }
    {
        let mut view = store.load(1).await.unwrap();
        assert_eq!(view.x1().get(), &1);
        assert_eq!(view.x2().get(), &0);
        assert_eq!(view.log_mut().read(0..10).await.unwrap(), vec![4]);
        assert_eq!(view.map_mut().get("Hello").await.unwrap(), Some(5));
        assert_eq!(view.map_mut().get("Hi").await.unwrap(), None);
        {
            let subview = view
                .collection_mut()
                .load_entry("hola".to_string())
                .await
                .unwrap();
            assert_eq!(subview.read(0..10).await.unwrap(), vec![17, 18]);
        }
    }
}

#[tokio::test]
async fn test_traits() {
    test_store(InMemoryTestStore::default()).await;
}
