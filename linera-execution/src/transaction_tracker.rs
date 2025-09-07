// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    mem, vec,
};

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Blob, BlobContent, Event, OracleResponse, StreamUpdate, Timestamp},
    ensure,
    identifiers::{ApplicationId, BlobId, ChainId, StreamId},
};

use crate::{ExecutionError, OutgoingMessage};

type AppStreamUpdates = BTreeMap<(ChainId, StreamId), (u32, u32)>;

/// Tracks oracle responses and execution outcomes of an ongoing transaction execution, as well
/// as replayed oracle responses.
#[derive(Debug, Default)]
pub struct TransactionTracker {
    #[debug(skip_if = Option::is_none)]
    replaying_oracle_responses: Option<vec::IntoIter<OracleResponse>>,
    #[debug(skip_if = Vec::is_empty)]
    oracle_responses: Vec<OracleResponse>,
    #[debug(skip_if = Vec::is_empty)]
    outgoing_messages: Vec<OutgoingMessage>,
    /// The current local time.
    local_time: Timestamp,
    /// The index of the current transaction in the block.
    transaction_index: u32,
    next_application_index: u32,
    next_chain_index: u32,
    /// Events recorded by contracts' `emit` calls.
    events: Vec<Event>,
    /// Blobs created by contracts.
    ///
    /// As of right now, blobs created by the contracts are one of the following types:
    /// - [`Data`]
    /// - [`ContractBytecode`]
    /// - [`ServiceBytecode`]
    /// - [`EvmBytecode`]
    /// - [`ApplicationDescription`]
    /// - [`ChainDescription`]
    blobs: BTreeMap<BlobId, BlobContent>,
    /// The blobs created in the previous transactions.
    previously_created_blobs: BTreeMap<BlobId, BlobContent>,
    /// Operation result.
    operation_result: Option<Vec<u8>>,
    /// Streams that have been updated but not yet processed during this transaction.
    streams_to_process: BTreeMap<ApplicationId, AppStreamUpdates>,
    /// Published blobs this transaction refers to by [`BlobId`].
    blobs_published: BTreeSet<BlobId>,
}

/// The [`TransactionTracker`] contents after a transaction has finished.
#[derive(Debug, Default)]
pub struct TransactionOutcome {
    #[debug(skip_if = Vec::is_empty)]
    pub oracle_responses: Vec<OracleResponse>,
    #[debug(skip_if = Vec::is_empty)]
    pub outgoing_messages: Vec<OutgoingMessage>,
    pub next_application_index: u32,
    pub next_chain_index: u32,
    /// Events recorded by contracts' `emit` calls.
    pub events: Vec<Event>,
    /// Blobs created by contracts.
    pub blobs: Vec<Blob>,
    /// Operation result.
    pub operation_result: Vec<u8>,
    /// Blobs published by this transaction.
    pub blobs_published: BTreeSet<BlobId>,
}

impl TransactionTracker {
    pub fn new(
        local_time: Timestamp,
        transaction_index: u32,
        next_application_index: u32,
        next_chain_index: u32,
        oracle_responses: Option<Vec<OracleResponse>>,
        blobs: &[Vec<Blob>],
    ) -> Self {
        let mut previously_created_blobs = BTreeMap::new();
        for tx_blobs in blobs {
            for blob in tx_blobs {
                previously_created_blobs.insert(blob.id(), blob.content().clone());
            }
        }
        TransactionTracker {
            local_time,
            transaction_index,
            next_application_index,
            next_chain_index,
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            previously_created_blobs,
            ..Self::default()
        }
    }

    pub fn with_blobs(mut self, blobs: BTreeMap<BlobId, BlobContent>) -> Self {
        self.blobs = blobs;
        self
    }

    pub fn local_time(&self) -> Timestamp {
        self.local_time
    }

    pub fn set_local_time(&mut self, local_time: Timestamp) {
        self.local_time = local_time;
    }

    pub fn transaction_index(&self) -> u32 {
        self.transaction_index
    }

    pub fn next_application_index(&mut self) -> u32 {
        let index = self.next_application_index;
        self.next_application_index += 1;
        index
    }

    pub fn next_chain_index(&mut self) -> u32 {
        let index = self.next_chain_index;
        self.next_chain_index += 1;
        index
    }

    pub fn add_outgoing_message(&mut self, message: OutgoingMessage) {
        self.outgoing_messages.push(message);
    }

    pub fn add_outgoing_messages(&mut self, messages: impl IntoIterator<Item = OutgoingMessage>) {
        for message in messages {
            self.add_outgoing_message(message);
        }
    }

    pub fn add_event(&mut self, stream_id: StreamId, index: u32, value: Vec<u8>) {
        self.events.push(Event {
            stream_id,
            index,
            value,
        });
    }

    pub fn get_blob_content(&self, blob_id: &BlobId) -> Option<&BlobContent> {
        if let Some(content) = self.blobs.get(blob_id) {
            return Some(content);
        }
        self.previously_created_blobs.get(blob_id)
    }

    pub fn add_created_blob(&mut self, blob: Blob) {
        self.blobs.insert(blob.id(), blob.into_content());
    }

    pub fn add_published_blob(&mut self, blob_id: BlobId) {
        self.blobs_published.insert(blob_id);
    }

    pub fn created_blobs(&self) -> &BTreeMap<BlobId, BlobContent> {
        &self.blobs
    }

    pub fn add_oracle_response(&mut self, oracle_response: OracleResponse) {
        self.oracle_responses.push(oracle_response);
    }

    pub fn add_operation_result(&mut self, result: Option<Vec<u8>>) {
        self.operation_result = result
    }

    pub fn add_stream_to_process(
        &mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        stream_id: StreamId,
        previous_index: u32,
        next_index: u32,
    ) {
        if next_index == previous_index {
            return; // No new events in the stream.
        }
        self.streams_to_process
            .entry(application_id)
            .or_default()
            .entry((chain_id, stream_id))
            .and_modify(|(pi, ni)| {
                *pi = (*pi).min(previous_index);
                *ni = (*ni).max(next_index);
            })
            .or_insert_with(|| (previous_index, next_index));
    }

    pub fn remove_stream_to_process(
        &mut self,
        application_id: ApplicationId,
        chain_id: ChainId,
        stream_id: StreamId,
    ) {
        let Some(streams) = self.streams_to_process.get_mut(&application_id) else {
            return;
        };
        if streams.remove(&(chain_id, stream_id)).is_some() && streams.is_empty() {
            self.streams_to_process.remove(&application_id);
        }
    }

    pub fn take_streams_to_process(&mut self) -> BTreeMap<ApplicationId, Vec<StreamUpdate>> {
        mem::take(&mut self.streams_to_process)
            .into_iter()
            .map(|(app_id, streams)| {
                let updates = streams
                    .into_iter()
                    .map(
                        |((chain_id, stream_id), (previous_index, next_index))| StreamUpdate {
                            chain_id,
                            stream_id,
                            previous_index,
                            next_index,
                        },
                    )
                    .collect();
                (app_id, updates)
            })
            .collect()
    }

    /// Adds the oracle response to the record.
    /// If replaying, it also checks that it matches the next replayed one and returns `true`.
    pub fn replay_oracle_response(
        &mut self,
        oracle_response: OracleResponse,
    ) -> Result<bool, ExecutionError> {
        let replaying = if let Some(recorded_response) = self.next_replayed_oracle_response()? {
            ensure!(
                recorded_response == oracle_response,
                ExecutionError::OracleResponseMismatch
            );
            true
        } else {
            false
        };
        self.add_oracle_response(oracle_response);
        Ok(replaying)
    }

    /// If in replay mode, returns the next oracle response, or an error if it is missing.
    ///
    /// If not in replay mode, `None` is returned, and the caller must execute the actual oracle
    /// to obtain the value.
    ///
    /// In both cases, the value (returned or obtained from the oracle) must be recorded using
    /// `add_oracle_response`.
    pub fn next_replayed_oracle_response(
        &mut self,
    ) -> Result<Option<OracleResponse>, ExecutionError> {
        let Some(responses) = &mut self.replaying_oracle_responses else {
            return Ok(None); // Not in replay mode.
        };
        let response = responses
            .next()
            .ok_or_else(|| ExecutionError::MissingOracleResponse)?;
        Ok(Some(response))
    }

    pub fn into_outcome(self) -> Result<TransactionOutcome, ExecutionError> {
        let TransactionTracker {
            replaying_oracle_responses,
            oracle_responses,
            outgoing_messages,
            local_time: _,
            transaction_index: _,
            next_application_index,
            next_chain_index,
            events,
            blobs,
            previously_created_blobs: _,
            operation_result,
            streams_to_process,
            blobs_published,
        } = self;
        ensure!(
            streams_to_process.is_empty(),
            ExecutionError::UnprocessedStreams
        );
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        let blobs = blobs
            .into_iter()
            .map(|(blob_id, content)| Blob::new_with_hash_unchecked(blob_id, content))
            .collect::<Vec<_>>();
        Ok(TransactionOutcome {
            outgoing_messages,
            oracle_responses,
            next_application_index,
            next_chain_index,
            events,
            blobs,
            operation_result: operation_result.unwrap_or_default(),
            blobs_published,
        })
    }
}

#[cfg(with_testing)]
impl TransactionTracker {
    /// Creates a new [`TransactionTracker`] for testing, with default values and the given
    /// oracle responses.
    pub fn new_replaying(oracle_responses: Vec<OracleResponse>) -> Self {
        TransactionTracker::new(Timestamp::from(0), 0, 0, 0, Some(oracle_responses), &[])
    }

    /// Creates a new [`TransactionTracker`] for testing, with default values and oracle responses
    /// for the given blobs.
    pub fn new_replaying_blobs<T>(blob_ids: T) -> Self
    where
        T: IntoIterator,
        T::Item: std::borrow::Borrow<BlobId>,
    {
        use std::borrow::Borrow;

        let oracle_responses = blob_ids
            .into_iter()
            .map(|blob_id| OracleResponse::Blob(*blob_id.borrow()))
            .collect();
        TransactionTracker::new_replaying(oracle_responses)
    }
}
