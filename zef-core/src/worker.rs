// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use zef_base::{
    base_types::*, chain::Outcome, committee::Committee, ensure, error::Error, messages::*,
};
use zef_storage::Storage;

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// Interface provided by each physical shard (aka "worker") of an validator or a local node.
/// * All commands return either the current chain info or an error.
/// * Repeating commands produces no changes and returns no error.
/// * Some handlers may return cross-shard requests, that is, messages
///   to be communicated to other workers of the same validator.
#[async_trait]
pub trait ValidatorWorker {
    /// Initiate a new request.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error>;

    /// Process a certificate, for instance to execute a confirmed request.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossShardRequest>), Error>;

    /// Handle information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error>;

    /// Handle (trusted!) cross shard request.
    async fn handle_cross_shard_request(
        &mut self,
        request: CrossShardRequest,
    ) -> Result<Vec<CrossShardRequest>, Error>;
}

impl<Client> WorkerState<Client> {
    pub fn new(key_pair: Option<KeyPair>, storage: Client) -> Self {
        WorkerState { key_pair, storage }
    }

    pub(crate) fn storage_client(&self) -> &Client {
        &self.storage
    }
}

/// State of a worker in an validator or a local node.
pub struct WorkerState<StorageClient> {
    /// The signature key pair of the validator. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of chains).
    key_pair: Option<KeyPair>,
    /// Access to local persistent storage.
    storage: StorageClient,
}

impl<Client> WorkerState<Client>
where
    Client: Storage + Clone + 'static,
{
    // NOTE: This only works for non-sharded workers!
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, zef_base::error::Error> {
        let (response, mut requests) = self.handle_certificate(certificate).await?;
        while let Some(request) = requests.pop() {
            requests.extend(self.handle_cross_shard_request(request).await?);
        }
        Ok(response)
    }

    /// (Trusted) Process a confirmed request issued from a chain.
    async fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(ChainInfoResponse, Vec<CrossShardRequest>), Error> {
        assert_eq!(
            &certificate.value.confirmed_request().unwrap().operation,
            &request.operation
        );
        // Obtain the sender's chain.
        let sender = request.chain_id.clone();
        // Check that the chain is active and ready for this confirmation.
        let mut chain = self.storage.read_active_chain(&sender).await?;
        if chain.next_sequence_number < request.sequence_number {
            return Err(Error::MissingEarlierConfirmations {
                current_sequence_number: chain.next_sequence_number,
            });
        }
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        certificate
            .check(chain.state.committee.as_ref().expect("chain is active"))
            .map_err(|_| Error::InvalidCertificate)?;
        // Load pending cross-shard requests.
        let mut continuation = self
            .storage
            .read_certificates(chain.keep_sending.iter().cloned())
            .await?
            .into_iter()
            .map(|certificate| CrossShardRequest::UpdateRecipient {
                committee: chain
                    .state
                    .committee
                    .as_ref()
                    .expect("Chain is active")
                    .clone(),
                certificate,
            })
            .collect();
        if chain.next_sequence_number > request.sequence_number {
            // Request was already confirmed.
            let info = chain.make_chain_info(self.key_pair.as_ref());
            return Ok((info, continuation));
        }
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Execute the sender's side of the operation.
        chain.apply_operation_as_sender(&request.operation, certificate.hash)?;
        // Advance to next sequence number.
        chain.next_sequence_number.try_add_assign_one()?;
        chain.state.manager.reset();
        // Final touch on the sender's chain.
        let info = chain.make_chain_info(self.key_pair.as_ref());
        // Schedule cross-shard request if any.
        let operation = &certificate.value.confirmed_request().unwrap().operation;
        if operation.recipient().is_some() {
            // Schedule a new cross-shard request to update recipient.
            chain.keep_sending.insert(certificate.hash);
            continuation.push(CrossShardRequest::UpdateRecipient {
                committee: chain
                    .state
                    .committee
                    .as_ref()
                    .expect("chain is active")
                    .clone(),
                certificate,
            });
        }
        // Persist chain.
        self.storage.write_chain(chain.clone()).await?;
        Ok((info, continuation))
    }

    /// (Trusted) Process a validated request issued from a multi-owner chain.
    async fn process_validated_request(
        &mut self,
        request: Request,
        certificate: Certificate,
    ) -> Result<ChainInfoResponse, Error> {
        assert_eq!(certificate.value.validated_request().unwrap(), &request);
        // Check that the chain is active and ready for this confirmation.
        let mut chain = self.storage.read_active_chain(&request.chain_id).await?;
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        certificate
            .check(chain.state.committee.as_ref().expect("chain is active"))
            .map_err(|_| Error::InvalidCertificate)?;
        if chain
            .state
            .manager
            .check_validated_request(chain.next_sequence_number, &request)?
            == Outcome::Skip
        {
            // If we just processed the same pending request, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair.as_ref()));
        }
        chain
            .state
            .manager
            .create_final_vote(request, certificate, self.key_pair.as_ref());
        let info = chain.make_chain_info(self.key_pair.as_ref());
        self.storage.write_chain(chain).await?;
        Ok(info)
    }

    /// (Trusted) Try to update the recipient chain in a confirmed request.
    async fn update_recipient_chain(
        &mut self,
        operation: Operation,
        committee: Committee,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            let request = certificate.value.confirmed_request().unwrap();
            assert_eq!(&request.operation, &operation);
            // Execute the recipient's side of the operation.
            let mut chain = self.storage.read_chain_or_default(recipient).await?;
            let need_update = chain.apply_operation_as_recipient(
                &operation,
                committee,
                certificate.hash,
                request.chain_id.clone(),
                request.sequence_number,
            )?;
            if need_update {
                self.storage.write_certificate(certificate).await?;
                self.storage.write_chain(chain).await?;
            }
        }
        // This concludes the confirmation of `certificate`.
        Ok(())
    }
}

#[async_trait]
impl<Client> ValidatorWorker for WorkerState<Client>
where
    Client: Storage + Clone + 'static,
{
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, Error> {
        // Obtain the sender's chain.
        let sender = proposal.request.chain_id.clone();
        let mut chain = self.storage.read_active_chain(&sender).await?;
        // Check authentication of the request.
        proposal.check(&chain.state.manager)?;
        // Check if the chain ready and if the request is well-formed.
        if chain
            .state
            .manager
            .check_request(chain.next_sequence_number, &proposal.request)?
            == Outcome::Skip
        {
            // If we just processed the same pending request, return the chain info
            // unchanged.
            return Ok(chain.make_chain_info(self.key_pair.as_ref()));
        }
        // Verify that the request is valid.
        chain.validate_operation(&proposal.request)?;
        // Create the vote and store it in the chain.
        chain
            .state
            .manager
            .create_vote(proposal, self.key_pair.as_ref());
        let info = chain.make_chain_info(self.key_pair.as_ref());
        self.storage.write_chain(chain).await?;
        Ok(info)
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(ChainInfoResponse, Vec<CrossShardRequest>), Error> {
        // Process the proposal.
        match &certificate.value {
            Value::Validated { request } => {
                // Confirm the validated request.
                let info = self
                    .process_validated_request(request.clone(), certificate)
                    .await?;
                Ok((info, Vec::new()))
            }
            Value::Confirmed { request } => {
                // Execute the confirmed request.
                self.process_confirmed_request(request.clone(), certificate)
                    .await
            }
        }
    }

    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, Error> {
        let chain = self.storage.read_chain_or_default(&query.chain_id).await?;
        let mut info = chain.make_chain_info(None).info;
        if query.query_committee {
            info.queried_committee = chain.state.committee;
        }
        if let Some(next_sequence_number) = query.check_next_sequence_number {
            ensure!(
                chain.next_sequence_number == next_sequence_number,
                Error::UnexpectedSequenceNumber
            );
        }
        if let Some(range) = query.query_sent_certificates_in_range {
            let keys = chain.confirmed_log[..]
                .iter()
                .skip(range.start.into())
                .cloned();
            let certs = match range.limit {
                None => self.storage.read_certificates(keys).await?,
                Some(count) => self.storage.read_certificates(keys.take(count)).await?,
            };
            info.queried_sent_certificates = certs;
        }
        if let Some(idx) = query.query_received_certificates_excluding_first_nth {
            let keys = chain.received_log[..].iter().skip(idx).cloned();
            let certs = self.storage.read_certificates(keys).await?;
            info.queried_received_certificates = certs;
        }
        let response = ChainInfoResponse::new(info, self.key_pair.as_ref());
        Ok(response)
    }

    async fn handle_cross_shard_request(
        &mut self,
        request: CrossShardRequest,
    ) -> Result<Vec<CrossShardRequest>, Error> {
        match request {
            CrossShardRequest::UpdateRecipient {
                committee,
                certificate,
            } => {
                let request = certificate
                    .value
                    .confirmed_request()
                    .ok_or(Error::InvalidCrossShardRequest)?;
                let sender = request.chain_id.clone();
                let hash = certificate.hash;
                self.update_recipient_chain(request.operation.clone(), committee, certificate)
                    .await?;
                // Reply with a cross-shard request.
                let cont = vec![CrossShardRequest::ConfirmUpdatedRecipient {
                    chain_id: sender,
                    hash,
                }];
                Ok(cont)
            }
            CrossShardRequest::ConfirmUpdatedRecipient { chain_id, hash } => {
                let mut chain = self.storage.read_active_chain(&chain_id).await?;
                if chain.keep_sending.remove(&hash) {
                    self.storage.write_chain(chain).await?;
                }
                Ok(Vec::new())
            }
        }
    }
}
