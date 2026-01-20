// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

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
    /// The latest committee blob ID processed by the exporter.
    /// Used to restore committee exporters on startup.
    latest_committee_blob: RegisterView<C, Option<BlobId>>,
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

        tracing::info!(
            init_destinations=?destinations,
            "initialized exporter state with destinations",
        );

        // Update stored destination states with new if not exist
        for destination in &destinations {
            if !view
                .destination_states
                .get()
                .states
                .pin()
                .contains_key(destination)
            {
                let mut states = view.destination_states.get().clone();
                states.insert(destination.clone(), Arc::new(AtomicU64::new(0)));
                view.destination_states.set(states);
            }
        }

        // Return the stored states (which include any newly added destinations)
        let states = view.destination_states.get().clone();
        let canonical_state = view.canonical_state.clone_unchecked()?;

        Ok((view, canonical_state, states))
    }

    pub fn index_blob(&mut self, blob: BlobId) -> Result<(), ExporterError> {
        Ok(self.blob_state.insert(&blob)?)
    }

    pub async fn index_block(&mut self, block: BlockId) -> Result<bool, ExporterError> {
        if let Some(last_processed) = self.chain_states.get_mut(&block.chain_id).await? {
            let expected_block_height = last_processed
                .height
                .try_add_one()
                .map_err(|e| ExporterError::GenericError(e.into()))?;
            if block.height == expected_block_height {
                *last_processed = block.into();
                return Ok(true);
            }
            tracing::warn!(
                ?expected_block_height,
                ?block,
                "attempted to index a block out of order",
            );
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

    pub fn set_latest_committee_blob(&mut self, blob_id: BlobId) {
        self.latest_committee_blob.set(Some(blob_id));
    }

    pub fn get_latest_committee_blob(&self) -> Option<BlobId> {
        *self.latest_committee_blob.get()
    }
}

#[derive(Debug, Clone)]
pub(super) struct DestinationStates {
    states: Arc<papaya::HashMap<DestinationId, Arc<AtomicU64>>>,
}

impl Default for DestinationStates {
    fn default() -> Self {
        Self {
            states: Arc::new(papaya::HashMap::new()),
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
        let states = {
            let pinned = self.states.pin();
            pinned
                .iter()
                .map(|(key, value)| (key.clone(), value.load(Ordering::Acquire)))
                .collect::<HashMap<_, _>>()
        };

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
        let map = papaya::HashMap::new();
        {
            let pinned = map.pin();
            for (id, state) in states {
                pinned.insert(id, Arc::new(AtomicU64::new(state)));
            }
        }
        Ok(Self {
            states: Arc::from(map),
        })
    }
}

impl DestinationStates {
    pub fn load_state(&self, id: &DestinationId) -> Arc<AtomicU64> {
        let pinned = self.states.pin();
        pinned
            .get(id)
            .unwrap_or_else(|| panic!("{:?} not found in DestinationStates", id))
            .clone()
    }

    pub fn get(&self, id: &DestinationId) -> Option<Arc<AtomicU64>> {
        self.states.pin().get(id).cloned()
    }

    pub fn insert(&mut self, id: DestinationId, state: Arc<AtomicU64>) {
        self.states.pin().insert(id, state);
    }
}
