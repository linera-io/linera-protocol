// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Some [`View`][`crate::views::View`]s that are easy to use with test cases.

use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;

use crate::{
    self as linera_views,
    collection_view::CollectionView,
    context::MemoryContext,
    log_view::LogView,
    map_view::MapView,
    register_view::RegisterView,
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

/// Wrapper to test with a [`LogView`].
#[derive(RootView, ClonableView)]
pub struct TestLogView<C> {
    log: LogView<C, u16>,
}

#[async_trait]
impl TestView for TestLogView<MemoryContext<()>> {
    type State = Vec<u16>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        let dummy_values = [1, 2, 3, 4, 5];

        for value in dummy_values {
            self.log.push(value);
        }

        Ok(dummy_values.to_vec())
    }

    async fn stage_changes_to_be_discarded(&mut self) -> Result<Self::State, ViewError> {
        let initial_state = [1, 2, 3, 4, 5];
        let new_values = [10_000, 20_000, 30_000];

        for value in new_values {
            self.log.push(value);
        }

        Ok(initial_state.into_iter().chain(new_values).collect())
    }

    async fn stage_changes_to_be_persisted(&mut self) -> Result<Self::State, ViewError> {
        let initial_state = [1, 2, 3, 4, 5];
        let new_values = [201, 1, 50_050];

        for value in new_values {
            self.log.push(value);
        }

        Ok(initial_state.into_iter().chain(new_values).collect())
    }

    async fn read(&self) -> Result<Self::State, ViewError> {
        self.log.read(..).await
    }
}

/// Wrapper to test with a [`MapView`].
#[derive(RootView, ClonableView)]
pub struct TestMapView<C> {
    map: MapView<C, i32, String>,
}

#[async_trait]
impl TestView for TestMapView<MemoryContext<()>> {
    type State = HashMap<i32, String>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        let dummy_values = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ]
        .into_iter()
        .map(|(key, value)| (key, value.to_owned()));

        for (key, value) in dummy_values.clone() {
            self.map.insert(&key, value)?;
        }

        Ok(dummy_values.collect())
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

        let initial_state = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ];

        let new_state = initial_state
            .into_iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (key, value.to_owned()))
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

        let initial_state = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ];

        let new_state = initial_state
            .into_iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (key, value.to_owned()))
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

/// Wrapper to test with a [`CollectionView`].
#[derive(RootView, ClonableView)]
pub struct TestCollectionView<C> {
    collection: CollectionView<C, i32, RegisterView<C, String>>,
}

#[async_trait]
impl TestView for TestCollectionView<MemoryContext<()>> {
    type State = HashMap<i32, String>;

    async fn stage_initial_changes(&mut self) -> Result<Self::State, ViewError> {
        let dummy_values = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ]
        .into_iter()
        .map(|(key, value)| (key, value.to_owned()));

        for (key, value) in dummy_values.clone() {
            self.collection.load_entry_mut(&key).await?.set(value);
        }

        Ok(dummy_values.collect())
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

        let initial_state = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ];

        let new_state = initial_state
            .into_iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (key, value.to_owned()))
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

        let initial_state = [
            (0, "zero"),
            (-1, "minus one"),
            (2, "two"),
            (-3, "minus three"),
            (4, "four"),
            (-5, "minus five"),
        ];

        let new_state = initial_state
            .into_iter()
            .filter(|(key, _)| !entries_to_remove.contains(key))
            .map(|(key, value)| (key, value.to_owned()))
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
