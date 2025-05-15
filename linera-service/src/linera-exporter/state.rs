// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use linera_base::{
    data_types::BlockHeight,
    identifiers::{BlobId, ChainId},
};
use linera_client::config::DestinationId;
use linera_sdk::{
    ensure,
    views::{RootView, View},
};
use linera_views::{
    context::Context, log_view::LogView, map_view::MapView, register_view::RegisterView,
    set_view::SetView, views::ClonableView,
};
use serde::{de::Visitor, ser::SerializeSeq as _, Deserialize, Serialize};

use crate::common::{BlockId, CanonicalBlock, ExporterError, LiteBlockId};

/// State of the linera exporter as a view.
#[derive(Debug, RootView, ClonableView)]
pub struct BlockExporterStateView<C> {
    /// The causal state.
    canonical_state: LogView<C, CanonicalBlock>,
    /// The global blob state.
    /// These blobs have been seen, processed
    /// and indexed by the exporter at least once.
    blob_state: SetView<C, BlobId>,
    /// The chain status, by chain ID.
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
        number_of_destinations: u16,
    ) -> Result<(Self, LogView<C, CanonicalBlock>, DestinationStates), ExporterError> {
        let mut view = BlockExporterStateView::load(context)
            .await
            .map_err(ExporterError::StateError)?;
        if view.destination_states.get().states.is_empty() {
            let states = DestinationStates::new(number_of_destinations);
            view.destination_states.set(states);
        }

        let states = view.destination_states.get().clone();
        let canonical_state = view.canonical_state.clone_unchecked()?;

        Ok((view, canonical_state, states))
    }

    pub fn index_blob(&mut self, blob: BlobId) -> Result<(), ExporterError> {
        Ok(self.blob_state.insert(&blob)?)
    }

    pub async fn index_block(&mut self, block: BlockId) -> Result<bool, ExporterError> {
        if let Some(status) = self.chain_states.get_mut(&block.chain_id).await? {
            if block.height > status.height {
                *status = block.into();
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
        let some = self.chain_states.get(chain_id).await?;
        Ok(some)
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
    states: Arc<[AtomicU64]>,
}

impl Default for DestinationStates {
    fn default() -> Self {
        Self {
            states: Arc::from([]),
        }
    }
}

impl Serialize for DestinationStates {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let slice = self
            .states
            .iter()
            .map(|x| x.load(std::sync::atomic::Ordering::Acquire))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let mut seq = serializer.serialize_seq(Some(slice.len()))?;
        for item in slice {
            seq.serialize_element(&item)?;
        }

        seq.end()
    }
}

impl<'de> Deserialize<'de> for DestinationStates {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DestinationStatesVisitor {
            marker: PhantomData<DestinationStates>,
        }

        impl<'de> Visitor<'de> for DestinationStatesVisitor {
            type Value = DestinationStates;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an unsigned integer with 64 bits of entropy")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element()? {
                    items.push(AtomicU64::new(item));
                }

                let states: Arc<[AtomicU64]> = Arc::from(items);
                let states = DestinationStates { states };
                Ok(states)
            }
        }

        let visitor = DestinationStatesVisitor {
            marker: PhantomData,
        };

        deserializer.deserialize_seq(visitor)
    }
}

impl DestinationStates {
    pub fn new(number_of_destinations: u16) -> Self {
        let slice = vec![0u64; number_of_destinations.into()]
            .into_iter()
            .map(AtomicU64::new)
            .collect::<Vec<_>>();
        let states: Arc<[AtomicU64]> = Arc::from(slice);
        Self { states }
    }

    pub fn increment_destination(&self, id: DestinationId) {
        if let Some(atomic) = self.states.get(id as usize) {
            let _ = atomic.fetch_add(1, Ordering::Release);
        }
    }

    pub fn load_state(&self, id: DestinationId) -> u64 {
        self.states
            .get(id as usize)
            .expect("DestinationId should correspond")
            .load(Ordering::Acquire)
    }
}
