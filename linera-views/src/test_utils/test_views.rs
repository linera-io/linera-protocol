// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Some [`View`][`crate::views::View`]s that are easy to use with test cases.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use async_trait::async_trait;

use crate::{
    self as linera_views,
    bucket_queue_view::BucketQueueView,
    collection_view::CollectionView,
    context::MemoryContext,
    log_view::LogView,
    map_view::MapView,
    queue_view::QueueView,
    register_view::RegisterView,
    set_view::SetView,
    views::{ClonableView, RootView, ViewError},
};

/// A [`View`][`crate::views::View`] to be used in test cases.
#[async_trait]
pub trait TestView:
    RootView<MemoryContext<()>> + ClonableView<MemoryContext<()>> + Send + Sync + 'static
{
    /// Representation of the view's state.
    type State: Debug + Eq + Send;

    /// Performs some initial changes to the view, staging them, and returning a representation of
    /// the view's state.
    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError>;

    /// Stages some changes to the view that won't be persisted during the test.
    ///
    /// Assumes that the current view state is the initially staged changes. Returns the updated
    /// state.
    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError>;

    /// Stages some changes to the view that will be persisted during the test.
    ///
    /// Assumes that the current view state is the initially staged changes. Returns the updated
    /// state.
    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError>;

    /// Reads the view's current state.
    async fn read(&self) -> Result<Self::State, ViewError>;
}

/// Wrapper to test with a [`RegisterView`].
#[derive(RootView, ClonableView)]
pub struct TestRegisterView<C> {
    byte: RegisterView<C, u8>,
}

#[async_trait]
impl TestView for TestRegisterView<MemoryContext<()>> {
    type State = u8;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        let dummy_value = 82;
        self.byte.set(dummy_value);
        Ok(dummy_value)
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let dummy_value = 209;
        self.byte.set(dummy_value);
        Ok(dummy_value)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let dummy_value = 15;
        self.byte.set(dummy_value);
        Ok(dummy_value)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        Ok(*self.byte.get())
    }
}

const INITIAL_LOG_QUEUE_VIEW_CHANGES: &[u16] = &[1, 2, 3, 4, 5];

/// Wrapper to test with a [`LogView`].
#[derive(RootView, ClonableView)]
pub struct TestLogView<C> {
    log: LogView<C, u16>,
}

#[async_trait]
impl TestView for TestLogView<MemoryContext<()>> {
    type State = Vec<u16>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for value in INITIAL_LOG_QUEUE_VIEW_CHANGES {
            self.log.push(*value);
        }

        Ok(INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let new_values = [10_000, 20_000, 30_000];

        for value in new_values {
            self.log.push(value);
        }

        Ok(INITIAL_LOG_QUEUE_VIEW_CHANGES
            .iter()
            .cloned()
            .chain(new_values)
            .collect())
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let new_values = [201, 1, 50_050];

        for value in new_values {
            self.log.push(value);
        }

        Ok(INITIAL_LOG_QUEUE_VIEW_CHANGES
            .iter()
            .cloned()
            .chain(new_values)
            .collect())
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        self.log.read(..).await
    }
}

const INITIAL_MAP_COLLECTION_VIEW_CHANGES: &[(i32, &str)] = &[
    (0, "zero"),
    (-1, "minus one"),
    (2, "two"),
    (-3, "minus three"),
    (4, "four"),
    (-5, "minus five"),
];

/// Wrapper to test with a [`MapView`].
#[derive(RootView, ClonableView)]
pub struct TestMapView<C> {
    map: MapView<C, i32, String>,
}

#[async_trait]
impl TestView for TestMapView<MemoryContext<()>> {
    type State = HashMap<i32, String>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for (key, value) in INITIAL_MAP_COLLECTION_VIEW_CHANGES {
            self.map.insert(key, value.to_string())?;
        }

        Ok(INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .map(|(key, value)| (*key, value.to_string()))
            .collect::<HashMap<_, _>>())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let new_entries = [(-1_000_000, "foo"), (2_000_000, "bar")]
            .into_iter()
            .map(|(key, value)| (key, value.to_owned()));

        let entries_to_remove = [0, -3];

        for (key, value) in new_entries.clone() {
            self.map.insert(&key, value)?;
        }

        for key in entries_to_remove {
            self.map.remove(&key)?;
        }

        let new_state = INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (*key, value.to_string()))
            .chain(new_entries)
            .collect();

        Ok(new_state)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let new_entries = [(1_234, "first new entry"), (-2_101_010, "second_new_entry")]
            .into_iter()
            .map(|(key, value)| (key, value.to_owned()));

        let entries_to_remove = [-1, 2, 4];

        for (key, value) in new_entries.clone() {
            self.map.insert(&key, value)?;
        }

        for key in entries_to_remove {
            self.map.remove(&key)?;
        }

        let new_state = INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (*key, value.to_string()))
            .chain(new_entries)
            .collect();

        Ok(new_state)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        let mut state = HashMap::new();
        self.map
            .for_each_index_value(|key, value| {
                let value = value.into_owned();
                state.insert(key, value);
                Ok(())
            })
            .await?;
        Ok(state)
    }
}

/// Wrapper to test with a [`SetView`].
#[derive(RootView, ClonableView)]
pub struct TestSetView<C> {
    set: SetView<C, i32>,
}

const INITIAL_SET_VIEW_CHANGES: &[i32] = &[0, -1, 2, -3, 4, -5];

#[async_trait]
impl TestView for TestSetView<MemoryContext<()>> {
    type State = HashSet<i32>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for key in INITIAL_SET_VIEW_CHANGES {
            self.set.insert(key)?;
        }

        Ok(INITIAL_SET_VIEW_CHANGES.iter().cloned().collect())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let mut state = INITIAL_SET_VIEW_CHANGES
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let new_entries = [-1_000_000, 2_000_000];

        let entries_to_remove = [0, -3];

        for key in new_entries {
            self.set.insert(&key)?;
            state.insert(key);
        }

        for key in entries_to_remove {
            self.set.remove(&key)?;
            state.remove(&key);
        }

        Ok(state)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let mut state = INITIAL_SET_VIEW_CHANGES
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let new_entries = [1_234, -2_101_010];

        let entries_to_remove = [-1, 2, 4];

        for key in new_entries {
            self.set.insert(&key)?;
            state.insert(key);
        }

        for key in entries_to_remove {
            self.set.remove(&key)?;
            state.remove(&key);
        }

        Ok(state)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        let indices = self.set.indices().await?;
        Ok(indices.into_iter().collect())
    }
}

/// Wrapper to test with a [`CollectionView`].
#[derive(RootView, ClonableView)]
pub struct TestCollectionView<C> {
    collection: CollectionView<C, i32, RegisterView<C, String>>,
}

#[async_trait]
impl TestView for TestCollectionView<MemoryContext<()>> {
    type State = HashMap<i32, String>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for (key, value) in INITIAL_MAP_COLLECTION_VIEW_CHANGES {
            self.collection
                .load_entry_mut(key)
                .await?
                .set(value.to_string());
        }

        Ok(INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .map(|(key, value)| (*key, value.to_string()))
            .collect::<HashMap<_, _>>())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let new_entries = [(-1_000_000, "foo"), (2_000_000, "bar")]
            .into_iter()
            .map(|(key, value)| (key, value.to_owned()));

        let entries_to_remove = [0, -3];

        for (key, value) in new_entries.clone() {
            self.collection.load_entry_mut(&key).await?.set(value);
        }

        for key in entries_to_remove {
            self.collection.remove_entry(&key)?;
        }

        let new_state = INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (*key, value.to_string()))
            .chain(new_entries)
            .collect();

        Ok(new_state)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let new_entries = [(1_234, "first new entry"), (-2_101_010, "second_new_entry")]
            .into_iter()
            .map(|(key, value)| (key, value.to_owned()));

        let entries_to_remove = [-1, 2, 4];

        for (key, value) in new_entries.clone() {
            self.collection.load_entry_mut(&key).await?.set(value);
        }

        for key in entries_to_remove {
            self.collection.remove_entry(&key)?;
        }

        let new_state = INITIAL_MAP_COLLECTION_VIEW_CHANGES
            .iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (*key, value.to_string()))
            .chain(new_entries)
            .collect();

        Ok(new_state)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        let indices = self.collection.indices().await?;
        let mut state = HashMap::with_capacity(indices.len());

        for index in indices {
            if let Some(value) = self.collection.try_load_entry(&index).await? {
                state.insert(index, value.get().clone());
            }
        }

        Ok(state)
    }
}

/// Wrapper to test with a [`QueueView`].
#[derive(RootView, ClonableView)]
pub struct TestQueueView<C> {
    queue: QueueView<C, u16>,
}

#[async_trait]
impl TestView for TestQueueView<MemoryContext<()>> {
    type State = Vec<u16>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for value in INITIAL_LOG_QUEUE_VIEW_CHANGES {
            self.queue.push_back(*value);
        }

        Ok(INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let mut initial_state = INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec();
        let new_values = [10_000, 20_000, 30_000];

        for value in new_values {
            self.queue.push_back(value);
            initial_state.push(value);
        }
        self.queue.delete_front();
        initial_state.remove(0);
        self.queue.delete_front();
        initial_state.remove(0);

        Ok(initial_state)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let mut initial_state = INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec();
        let new_values = [201, 1, 50_050, 203];

        for value in new_values {
            self.queue.push_back(value);
            initial_state.push(value);
        }
        self.queue.delete_front();
        initial_state.remove(0);

        Ok(initial_state)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        self.queue.elements().await
    }
}

/// Wrapper to test with a [`BucketQueueView`].
#[derive(RootView, ClonableView)]
pub struct TestBucketQueueView<C> {
    queue: BucketQueueView<C, u16, 2>,
}

#[async_trait]
impl TestView for TestBucketQueueView<MemoryContext<()>> {
    type State = Vec<u16>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        for value in INITIAL_LOG_QUEUE_VIEW_CHANGES {
            self.queue.push_back(*value);
        }

        Ok(INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let mut initial_state = INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec();
        let new_values = [10_000, 20_000, 30_000];

        for value in new_values {
            self.queue.push_back(value);
            initial_state.push(value);
        }
        self.queue.delete_front().await?;
        initial_state.remove(0);
        self.queue.delete_front().await?;
        initial_state.remove(0);

        Ok(initial_state)
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let mut initial_state = INITIAL_LOG_QUEUE_VIEW_CHANGES.to_vec();
        let new_values = [201, 1, 50_050, 203];

        for value in new_values {
            self.queue.push_back(value);
            initial_state.push(value);
        }
        self.queue.delete_front().await?;
        initial_state.remove(0);

        Ok(initial_state)
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        self.queue.elements().await
    }
}
