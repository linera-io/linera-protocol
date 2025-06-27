// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
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
    /// The exporter state for the current committee.
    committee_state: RegisterView<C, CommitteeState>,
}

impl<C> BlockExporterStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    pub async fn initiate(
        context: C,
        number_of_destinations: u16,
    ) -> Result<
        (
            Self,
            LogView<C, CanonicalBlock>,
            DestinationStates,
            CommitteeState,
        ),
        ExporterError,
    > {
        let mut view = BlockExporterStateView::load(context)
            .await
            .map_err(ExporterError::StateError)?;
        if view.destination_states.get().states.is_empty() {
            let states = DestinationStates::new(number_of_destinations);
            view.destination_states.set(states);
        }

        ensure!(
            view.destination_states.get().states.len() == number_of_destinations as usize,
            ExporterError::GenericError(
                "inconsistent number of destinations in the toml file".into()
            )
        );

        let states = view.destination_states.get().clone();
        let canonical_state = view.canonical_state.clone_unchecked()?;

        let committee_state = view.committee_state.get().clone();

        Ok((view, canonical_state, states, committee_state))
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

    pub fn set_committee_state(&mut self, committee_state: CommitteeState) {
        self.committee_state.set(committee_state);
    }

    pub fn replace_current_committee(&self, addresses: Vec<String>) {
        let state = Arc::new(
            addresses
                .into_iter()
                .map(CommitteeMember::new)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        self.committee_state.get().states.swap(state);
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

#[derive(Serialize, Deserialize)]
#[serde(rename = "DestinationStates")]
struct SerializableDestinationStates {
    states: Vec<u64>,
}

impl Serialize for DestinationStates {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let states = self
            .states
            .iter()
            .map(|x| x.load(std::sync::atomic::Ordering::Acquire))
            .collect::<Vec<_>>();

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
        let states = states
            .iter()
            .map(|state| AtomicU64::new(*state))
            .collect::<Arc<_>>();
        Ok(Self { states })
    }
}

impl DestinationStates {
    pub fn new(number_of_destinations: u16) -> Self {
        let slice = vec![0u64; number_of_destinations.into()]
            .into_iter()
            .map(AtomicU64::new)
            .collect::<Vec<_>>();
        let states = Arc::from(slice);
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

#[derive(Debug, Clone, Default)]
pub(crate) struct CommitteeState {
    states: Arc<ArcSwap<Box<[CommitteeMember]>>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "CommitteeState")]
struct SerializableCommitteeState {
    states: Vec<SerializedCommitteeMember>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "CommitteeMember")]
struct SerializedCommitteeMember {
    address: String,
    export_state: u64,
}

impl Serialize for CommitteeState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let states = self
            .states
            .load()
            .iter()
            .map(|member| SerializedCommitteeMember {
                address: member.address.clone(),
                export_state: member.export_state.load(Ordering::Acquire),
            })
            .collect();

        SerializableCommitteeState::serialize(&SerializableCommitteeState { states }, serializer)
    }
}

impl<'de> Deserialize<'de> for CommitteeState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let SerializableCommitteeState { states } =
            SerializableCommitteeState::deserialize(deserializer)?;
        let states = states
            .iter()
            .map(|member| CommitteeMember {
                address: member.address.clone(),
                export_state: member.export_state.into(),
            })
            .collect::<Box<[_]>>();
        Ok(Self {
            states: Arc::new(ArcSwap::new(states.into())),
        })
    }
}

impl CommitteeState {
    pub fn load_state(&self, id: DestinationId) -> u64 {
        self.states
            .load()
            .get(id as usize)
            .expect("DestinationId should correspond")
            .load_state()
    }

    pub fn increment_destination(&self, id: DestinationId) {
        if let Some(member) = self.states.load().get(id as usize) {
            member.increment_state();
        }
    }

    pub fn get_address(&self, id: DestinationId) -> String {
        self.states
            .load()
            .get(id as usize)
            .expect("DestinationId should correspond")
            .get_address()
    }
}

#[derive(Debug)]
struct CommitteeMember {
    address: String,
    export_state: AtomicU64,
}

impl CommitteeMember {
    fn new(address: String) -> Self {
        Self {
            address,
            export_state: AtomicU64::new(0),
        }
    }

    fn increment_state(&self) {
        self.export_state.fetch_add(1, Ordering::Release);
    }

    fn load_state(&self) -> u64 {
        self.export_state.load(Ordering::Acquire)
    }

    fn get_address(&self) -> String {
        self.address.clone()
    }
}
