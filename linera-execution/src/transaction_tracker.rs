// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, vec};

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Amount, ArithmeticError, Blob, Event, OracleResponse},
    ensure,
    identifiers::{ApplicationId, BlobId, ChainId, ChannelFullName, StreamId},
};

use crate::{
    ExecutionError, Message, OutgoingMessage, RawExecutionOutcome, RawOutgoingMessage,
    SystemMessage,
};

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
        next_message_index: u32,
        next_application_index: u32,
        oracle_responses: Option<Vec<OracleResponse>>,
    ) -> Self {
        TransactionTracker {
            replaying_oracle_responses: oracle_responses.map(Vec::into_iter),
            next_message_index,
            next_application_index,
            ..Self::default()
        }
    }

    pub fn with_blobs(mut self, blobs: BTreeMap<BlobId, Blob>) -> Self {
        self.blobs = blobs;
        self
    }

    pub fn next_message_index(&self) -> u32 {
        self.next_message_index
    }

    pub fn next_application_index(&mut self) -> u32 {
        let index = self.next_application_index;
        self.next_application_index += 1;
        index
    }

    pub fn add_system_outcome(
        &mut self,
        outcome: RawExecutionOutcome<SystemMessage>,
    ) -> Result<(), ArithmeticError> {
        self.add_outcome(Message::System, outcome)
    }

    pub fn add_user_outcome(
        &mut self,
        application_id: ApplicationId,
        outcome: RawExecutionOutcome<Vec<u8>>,
    ) -> Result<(), ArithmeticError> {
        self.add_outcome(
            |bytes| Message::User {
                application_id,
                bytes,
            },
            outcome,
        )
    }

    fn add_outcome<M, F>(
        &mut self,
        lift: F,
        outcome: RawExecutionOutcome<M>,
    ) -> Result<(), ArithmeticError>
    where
        F: Fn(M) -> Message,
    {
        for RawOutgoingMessage {
            destination,
            authenticated,
            grant,
            kind,
            message,
        } in outcome.messages
        {
            let authenticated_signer = outcome.authenticated_signer.filter(|_| authenticated);
            let refund_grant_to = outcome.refund_grant_to.filter(|_| grant > Amount::ZERO);
            self.add_outgoing_message(OutgoingMessage {
                destination,
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                message: lift(message),
            })?;
        }
        Ok(())
    }

    fn add_outgoing_message(&mut self, message: OutgoingMessage) -> Result<(), ArithmeticError> {
        self.next_message_index = self
            .next_message_index
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.outgoing_messages.push(message);
        Ok(())
    }

    pub fn add_event(&mut self, stream_id: StreamId, key: Vec<u8>, value: Vec<u8>) {
        self.events.push(Event {
            stream_id,
            key,
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
