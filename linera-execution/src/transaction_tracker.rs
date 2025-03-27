// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, vec};

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{ArithmeticError, Blob, Event, OracleResponse, Timestamp},
    ensure,
    identifiers::{BlobId, ChainId, ChannelFullName, StreamId},
};

use crate::{ExecutionError, OutgoingMessage};

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
    next_message_index: u32,
    next_application_index: u32,
    /// Events recorded by contracts' `emit` calls.
    events: Vec<Event>,
    /// Blobs created by contracts.
    blobs: BTreeMap<BlobId, Blob>,
    /// Subscribe chains to channels.
    subscribe: Vec<(ChannelFullName, ChainId)>,
    /// Unsubscribe chains from channels.
    unsubscribe: Vec<(ChannelFullName, ChainId)>,
    /// Operation result.
    operation_result: Option<Vec<u8>>,
}

/// The [`TransactionTracker`] contents after a transaction has finished.
#[derive(Debug, Default)]
pub struct TransactionOutcome {
    #[debug(skip_if = Vec::is_empty)]
    pub oracle_responses: Vec<OracleResponse>,
    #[debug(skip_if = Vec::is_empty)]
    pub outgoing_messages: Vec<OutgoingMessage>,
    pub next_message_index: u32,
    pub next_application_index: u32,
    /// Events recorded by contracts' `emit` calls.
    pub events: Vec<Event>,
    /// Blobs created by contracts.
    pub blobs: Vec<Blob>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelFullName, ChainId)>,
    /// Unsubscribe chains from channels.
    pub unsubscribe: Vec<(ChannelFullName, ChainId)>,
    /// Operation result.
    pub operation_result: Vec<u8>,
}

impl TransactionTracker {
    pub fn new(
        local_time: Timestamp,
        transaction_index: u32,
        next_message_index: u32,
        next_application_index: u32,
        oracle_responses: Option<Vec<OracleResponse>>,
    ) -> Self {
        TransactionTracker {
            local_time,
            transaction_index,
            next_message_index,
            next_application_index,
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            ..Self::default()
        }
    }

    pub fn with_blobs(mut self, blobs: BTreeMap<BlobId, Blob>) -> Self {
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

    pub fn next_message_index(&self) -> u32 {
        self.next_message_index
    }

    pub fn next_application_index(&mut self) -> u32 {
        let index = self.next_application_index;
        self.next_application_index += 1;
        index
    }

    pub fn add_outgoing_message(
        &mut self,
        message: OutgoingMessage,
    ) -> Result<(), ArithmeticError> {
        self.next_message_index = self
            .next_message_index
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.outgoing_messages.push(message);
        Ok(())
    }

    pub fn add_outgoing_messages(
        &mut self,
        messages: impl IntoIterator<Item = OutgoingMessage>,
    ) -> Result<(), ArithmeticError> {
        for message in messages {
            self.add_outgoing_message(message)?;
        }
        Ok(())
    }

    pub fn add_event(&mut self, stream_id: StreamId, index: u32, value: Vec<u8>) {
        self.events.push(Event {
            stream_id,
            index,
            value,
        });
    }

    pub fn add_created_blob(&mut self, blob: Blob) {
        self.blobs.insert(blob.id(), blob);
    }

    pub fn created_blobs(&self) -> &BTreeMap<BlobId, Blob> {
        &self.blobs
    }

    pub fn subscribe(&mut self, name: ChannelFullName, subscriber: ChainId) {
        self.subscribe.push((name, subscriber));
    }

    pub fn unsubscribe(&mut self, name: ChannelFullName, subscriber: ChainId) {
        self.unsubscribe.push((name, subscriber));
    }

    pub fn add_oracle_response(&mut self, oracle_response: OracleResponse) {
        self.oracle_responses.push(oracle_response);
    }

    pub fn add_operation_result(&mut self, result: Option<Vec<u8>>) {
        self.operation_result = result
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
            next_message_index,
            next_application_index,
            events,
            blobs,
            subscribe,
            unsubscribe,
            operation_result,
        } = self;
        if let Some(mut responses) = replaying_oracle_responses {
            ensure!(
                responses.next().is_none(),
                ExecutionError::UnexpectedOracleResponse
            );
        }
        Ok(TransactionOutcome {
            outgoing_messages,
            oracle_responses,
            next_message_index,
            next_application_index,
            events,
            blobs: blobs.into_values().collect(),
            subscribe,
            unsubscribe,
            operation_result: operation_result.unwrap_or_default(),
        })
    }
}

#[cfg(with_testing)]
impl TransactionTracker {
    /// Creates a new [`TransactionTracker`] for testing, with default values and the given
    /// oracle responses.
    pub fn new_replaying(oracle_responses: Vec<OracleResponse>) -> Self {
        TransactionTracker::new(Timestamp::from(0), 0, 0, 0, Some(oracle_responses))
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
