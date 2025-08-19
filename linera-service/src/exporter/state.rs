// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use linera_base::{
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use linera_sdk::{
    ensure,
    views::{RootView, View},
};
use linera_service::config::DestinationId;
use linera_views::{
    context::Context, log_view::LogView, map_view::MapView, register_view::RegisterView,
    set_view::SetView, views::ClonableView,
};
use serde::{Deserialize, Serialize};

use crate::common::{BlockId, CanonicalBlock, ExporterError, LiteBlockId};

/// State of the linera exporter as a view.
#[derive(Debug, RootView, ClonableView)]
pub struct BlockExporterStateView<C> {
    /// Ordered collection of block hashes from all microchains, as indexed by the exporter.
    canonical_state: LogView<C, CanonicalBlock>,
    /// The global blob state.
    /// These blobs have been seen, processed
    /// and indexed by the exporter at least once.
    blob_state: SetView<C, BlobId>,
    /// Tracks the highest block already processed with its hash.
    chain_states: MapView<C, ChainId, LiteBlockId>,
    /// The exporter state per destination.
    destination_states: RegisterView<C, DestinationStates>,
}

impl<C> BlockExporterStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    pub async fn initiate(
        context: C,
        destinations: Vec<DestinationId>,
    ) -> Result<(Self, LogView<C, CanonicalBlock>, DestinationStates), ExporterError> {
        let mut view = BlockExporterStateView::load(context)
            .await
            .map_err(ExporterError::StateError)?;

        let destinations_len = destinations.len();

        if view.destination_states.get().states.is_empty() {
            let states = DestinationStates::new(destinations);
            view.destination_states.set(states);
        }

        ensure!(
            view.destination_states.get().states.len() == destinations_len,
            ExporterError::GenericError(
                "inconsistent number of destinations in the toml file".into()
            )
        );

        let states = view.destination_states.get().clone();
        let canonical_state = view.canonical_state.clone_unchecked();

        Ok((view, canonical_state, states))
    }

    pub fn index_blob(&mut self, blob: BlobId) -> Result<(), ExporterError> {
        Ok(self.blob_state.insert(&blob)?)
    }

    pub async fn index_block(&mut self, block: BlockId) -> Result<bool, ExporterError> {
        if let Some(last_processed) = self.chain_states.get_mut(&block.chain_id).await? {
            if block.height
                == last_processed
                    .height
                    .try_add_one()
                    .map_err(|e| ExporterError::GenericError(e.into()))?
            {
                *last_processed = block.into();
                return Ok(true);
            }

            Ok(false)
        } else {
            Err(ExporterError::UnprocessedChain)
        }
    }

    pub async fn initialize_chain(&mut self, block: BlockId) -> Result<(), ExporterError> {
        ensure!(
            block.height == BlockHeight::ZERO,
            ExporterError::BadInitialization
        );

        if self.chain_states.contains_key(&block.chain_id).await? {
            Err(ExporterError::ChainAlreadyExists(block.chain_id))?
        }

        let chain_id = block.chain_id;
        self.chain_states.insert(&chain_id, block.into())?;
        Ok(())
    }

    pub async fn get_chain_status(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<LiteBlockId>, ExporterError> {
        Ok(self.chain_states.get(chain_id).await?)
    }

    pub async fn is_blob_indexed(&self, blob: BlobId) -> Result<bool, ExporterError> {
        Ok(self.blob_state.contains(&blob).await?)
    }

    pub fn set_destination_states(&mut self, destination_states: DestinationStates) {
        self.destination_states.set(destination_states);
    }
}

#[derive(Debug, Clone)]
pub(super) struct DestinationStates {
    states: Arc<DashMap<DestinationId, Arc<AtomicU64>>>,
}

impl Default for DestinationStates {
    fn default() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "DestinationStates")]
struct SerializableDestinationStates {
    states: HashMap<DestinationId, u64>,
}

impl Serialize for DestinationStates {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let states = self
            .states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Acquire)))
            .collect::<HashMap<_, _>>();

        SerializableDestinationStates::serialize(
            &SerializableDestinationStates { states },
            serializer,
        )
    }
}

impl<'de> Deserialize<'de> for DestinationStates {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let SerializableDestinationStates { states } =
            SerializableDestinationStates::deserialize(deserializer)?;
        let map = DashMap::new();
        for (id, state) in states {
            map.insert(id, Arc::new(AtomicU64::new(state)));
        }
        Ok(Self {
            states: Arc::from(map),
        })
    }
}

impl DestinationStates {
    fn new(destinations: Vec<DestinationId>) -> Self {
        let states = destinations
            .into_iter()
            .map(|id| (id, Arc::new(AtomicU64::new(0))))
            .collect::<DashMap<_, _>>();
        Self {
            states: Arc::from(states),
        }
    }

    pub fn load_state(&self, id: &DestinationId) -> Arc<AtomicU64> {
        self.states
            .get(id)
            .unwrap_or_else(|| panic!("{:?} not found in DestinationStates", id))
            .clone()
    }

    pub fn get(&self, id: &DestinationId) -> Option<Arc<AtomicU64>> {
        self.states.get(id).map(|state| state.clone())
    }

    pub fn insert(&mut self, id: DestinationId, state: Arc<AtomicU64>) {
        self.states.insert(id, state);
    }
}
