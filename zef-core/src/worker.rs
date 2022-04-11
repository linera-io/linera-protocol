// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::Outcome, base_types::*, committee::Committee, ensure, error::Error, messages::*,
    storage::StorageClient,
};
use async_trait::async_trait;

#[cfg(test)]
#[path = "unit_tests/worker_tests.rs"]
mod worker_tests;

/// Interface provided by each physical shard (aka "worker") of an authority or a local node.
/// * All commands return either the current account info or an error.
/// * Repeating commands produces no changes and returns no error.
/// * Some handlers may return cross-shard requests, that is, messages
///   to be communicated to other workers of the same authority.
#[async_trait]
pub trait AuthorityWorker {
    /// Initiate a new request.
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error>;

    /// Process a certificate, for instance to execute a confirmed request.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error>;

    /// Handle information queries for this account.
    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error>;

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

/// State of a worker in an authority or a local node.
pub struct WorkerState<StorageClient> {
    /// The signature key pair of the authority. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of accounts).
    key_pair: Option<KeyPair>,
    /// Access to local persistent storage.
    storage: StorageClient,
}

impl<Client> WorkerState<Client>
where
    Client: StorageClient + Clone + 'static,
{
    // NOTE: This only works for non-sharded workers!
    pub(crate) async fn fully_handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, crate::error::Error> {
        let (response, mut requests) = self.handle_certificate(certificate).await?;
        while let Some(request) = requests.pop() {
            requests.extend(self.handle_cross_shard_request(request).await?);
        }
        Ok(response)
    }

    /// (Trusted) Process a confirmed request issued from an account.
    async fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
        assert_eq!(
            &certificate.value.confirmed_request().unwrap().operation,
            &request.operation
        );
        // Obtain the sender's account.
        let sender = request.account_id.clone();
        // Check that the account is active and ready for this confirmation.
        let mut account = self.storage.read_active_account(&sender).await?;
        if account.next_sequence_number < request.sequence_number {
            return Err(Error::MissingEarlierConfirmations {
                current_sequence_number: account.next_sequence_number,
            });
        }
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        certificate
            .check(account.committee.as_ref().expect("account is active"))
            .map_err(|_| Error::InvalidCertificate)?;
        // Load pending cross-shard requests.
        let mut continuation = self
            .storage
            .read_certificates(account.keep_sending.iter().cloned())
            .await?
            .into_iter()
            .map(|certificate| CrossShardRequest::UpdateRecipient {
                committee: account
                    .committee
                    .as_ref()
                    .expect("Account is active")
                    .clone(),
                certificate,
            })
            .collect();
        if account.next_sequence_number > request.sequence_number {
            // Request was already confirmed.
            let info = account.make_account_info(self.key_pair.as_ref());
            return Ok((info, continuation));
        }
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Execute the sender's side of the operation.
        account.apply_operation_as_sender(&request.operation, certificate.hash)?;
        // Advance to next sequence number.
        account.next_sequence_number.try_add_assign_one()?;
        account.manager.reset();
        // Final touch on the sender's account.
        let info = account.make_account_info(self.key_pair.as_ref());
        // Schedule cross-shard request if any.
        let operation = &certificate.value.confirmed_request().unwrap().operation;
        if operation.recipient().is_some() {
            // Schedule a new cross-shard request to update recipient.
            account.keep_sending.insert(certificate.hash);
            continuation.push(CrossShardRequest::UpdateRecipient {
                committee: account
                    .committee
                    .as_ref()
                    .expect("account is active")
                    .clone(),
                certificate,
            });
        }
        // Persist account.
        self.storage.write_account(account.clone()).await?;
        Ok((info, continuation))
    }

    /// (Trusted) Process a validated request issued from a multi-owner account.
    async fn process_validated_request(
        &mut self,
        request: Request,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, Error> {
        assert_eq!(certificate.value.validated_request().unwrap(), &request);
        // Check that the account is active and ready for this confirmation.
        let mut account = self
            .storage
            .read_active_account(&request.account_id)
            .await?;
        // Verify the certificate. Returns a catch-all error to make client code more robust.
        certificate
            .check(account.committee.as_ref().expect("account is active"))
            .map_err(|_| Error::InvalidCertificate)?;
        if account
            .manager
            .check_validated_request(account.next_sequence_number, &request)?
            == Outcome::Skip
        {
            // If we just processed the same pending request, return the account info
            // unchanged.
            return Ok(account.make_account_info(self.key_pair.as_ref()));
        }
        account
            .manager
            .create_final_vote(request, certificate, self.key_pair.as_ref());
        let info = account.make_account_info(self.key_pair.as_ref());
        self.storage.write_account(account).await?;
        Ok(info)
    }

    /// (Trusted) Try to update the recipient account in a confirmed request.
    async fn update_recipient_account(
        &mut self,
        operation: Operation,
        committee: Committee,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            let request = certificate.value.confirmed_request().unwrap();
            assert_eq!(&request.operation, &operation);
            // Execute the recipient's side of the operation.
            let mut account = self.storage.read_account_or_default(recipient).await?;
            let need_update = account.apply_operation_as_recipient(
                &operation,
                committee,
                certificate.hash,
                request.account_id.clone(),
                request.sequence_number,
            )?;
            if need_update {
                self.storage.write_certificate(certificate).await?;
                self.storage.write_account(account).await?;
            }
        }
        // This concludes the confirmation of `certificate`.
        Ok(())
    }
}

#[async_trait]
impl<Client> AuthorityWorker for WorkerState<Client>
where
    Client: StorageClient + Clone + 'static,
{
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error> {
        // Obtain the sender's account.
        let sender = order.request.account_id.clone();
        let mut account = self.storage.read_active_account(&sender).await?;
        // Check authentication of the request.
        order.check(&account.manager)?;
        // Check if the account ready and if the request is well-formed.
        if account
            .manager
            .check_request(account.next_sequence_number, &order.request)?
            == Outcome::Skip
        {
            // If we just processed the same pending request, return the account info
            // unchanged.
            return Ok(account.make_account_info(self.key_pair.as_ref()));
        }
        // Verify that the request is valid.
        account.validate_operation(&order.request)?;
        // Create the vote and store it in the account.
        account.manager.create_vote(order, self.key_pair.as_ref());
        let info = account.make_account_info(self.key_pair.as_ref());
        self.storage.write_account(account).await?;
        Ok(info)
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
        // Process the order.
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

    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        let account = self
            .storage
            .read_account_or_default(&query.account_id)
            .await?;
        let mut info = account.make_account_info(None).info;
        if query.query_committee {
            info.queried_committee = account.committee;
        }
        if let Some(next_sequence_number) = query.check_next_sequence_number {
            ensure!(
                account.next_sequence_number == next_sequence_number,
                Error::UnexpectedSequenceNumber
            );
        }
        if let Some(range) = query.query_sent_certificates_in_range {
            let keys = account.confirmed_log[..]
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
            let keys = account.received_log[..].iter().skip(idx).cloned();
            let certs = self.storage.read_certificates(keys).await?;
            info.queried_received_certificates = certs;
        }
        let response = AccountInfoResponse::new(info, self.key_pair.as_ref());
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
                let sender = request.account_id.clone();
                let hash = certificate.hash;
                self.update_recipient_account(request.operation.clone(), committee, certificate)
                    .await?;
                // Reply with a cross-shard request.
                let cont = vec![CrossShardRequest::ConfirmUpdatedRecipient {
                    account_id: sender,
                    hash,
                }];
                Ok(cont)
            }
            CrossShardRequest::ConfirmUpdatedRecipient { account_id, hash } => {
                let mut account = self.storage.read_active_account(&account_id).await?;
                if account.keep_sending.remove(&hash) {
                    self.storage.write_account(account).await?;
                }
                Ok(Vec::new())
            }
        }
    }
}
