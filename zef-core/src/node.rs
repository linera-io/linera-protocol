// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountManager,
    authority::{Authority, WorkerState},
    base_types::*,
    error::Error,
    messages::*,
    storage::StorageClient,
};
use async_trait::async_trait;
use futures::{lock::Mutex, StreamExt};
use rand::prelude::SliceRandom;
use std::sync::Arc;

/// How to communicate with an authority or a local node.
#[async_trait]
pub trait AuthorityClient {
    /// Initiate a new transfer.
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error>;

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, Error>;

    /// Handle information queries for this account.
    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error>;
}

/// A local replica, typically used by clients.
pub struct LocalNode<S> {
    state: WorkerState<S>,
}

#[derive(Clone)]
pub struct LocalNodeClient<S>(Arc<Mutex<LocalNode<S>>>);

#[async_trait]
impl<S> AuthorityClient for LocalNodeClient<S>
where
    S: StorageClient + Clone + 'static,
{
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        node.state.handle_request_order(order).await
    }

    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, Error> {
        let node = self.0.clone();
        let mut node = node.lock().await;
        node.state.fully_handle_certificate(certificate).await
    }

    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        self.0
            .clone()
            .lock()
            .await
            .state
            .handle_account_info_query(query)
            .await
    }
}

impl<S> LocalNodeClient<S> {
    pub fn new(state: WorkerState<S>) -> Self {
        let node = LocalNode { state };
        Self(Arc::new(Mutex::new(node)))
    }
}

impl<S> LocalNodeClient<S>
where
    S: Clone,
{
    pub(crate) async fn storage_client(&self) -> S {
        let node = self.0.clone();
        let node = node.lock().await;
        node.state.storage_client().clone()
    }
}

impl<S> LocalNodeClient<S>
where
    S: StorageClient + Clone + 'static,
{
    async fn try_process_certificates(
        &mut self,
        account_id: &AccountId,
        certificates: Vec<Certificate>,
    ) -> Result<Option<SequenceNumber>, Error> {
        let mut next_sequence_number = None;
        for certificate in certificates {
            if let Value::Confirmed { request } = &certificate.value {
                if &request.account_id == account_id {
                    match self.handle_certificate(certificate).await {
                        Ok(response) => {
                            next_sequence_number = Some(response.info.next_sequence_number);
                            // Continue with the next certificate.
                            continue;
                        }
                        Err(
                            Error::InvalidCertificate | Error::MissingEarlierConfirmations { .. },
                        ) => {
                            // The certificate is not as expected. Give up.
                            return Ok(next_sequence_number);
                        }
                        Err(e) => {
                            // Something is wrong: a valid certificate with the
                            // right sequence number should not fail to execute.
                            return Err(e);
                        }
                    }
                }
            }
            // The certificate is not as expected. Give up.
            return Ok(next_sequence_number);
        }
        // Done with all certificates.
        Ok(next_sequence_number)
    }

    async fn current_next_sequence_number(
        &mut self,
        account_id: AccountId,
    ) -> Result<SequenceNumber, Error> {
        let query = AccountInfoQuery {
            account_id,
            check_next_sequence_number: None,
            query_committee: false,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let response = self.handle_account_info_query(query).await?;
        Ok(response.info.next_sequence_number)
    }

    pub async fn download_certificates<A>(
        &mut self,
        mut authorities: Vec<(AuthorityName, A)>,
        account_id: AccountId,
        target_next_sequence_number: SequenceNumber,
    ) -> Result<SequenceNumber, Error>
    where
        A: AuthorityClient + Send + Sync + 'static + Clone,
    {
        // Sequentially try each authority in random order.
        // TODO: We could also try a few of them in parallel.
        authorities.shuffle(&mut rand::thread_rng());
        for (name, client) in authorities {
            let current = self
                .current_next_sequence_number(account_id.clone())
                .await?;
            if target_next_sequence_number <= current {
                return Ok(current);
            }
            self.try_download_certificates_from(
                name,
                client,
                account_id.clone(),
                current,
                target_next_sequence_number,
            )
            .await?;
            // TODO: We could continue with the same authority if sufficient progress was made.
        }
        let current = self.current_next_sequence_number(account_id).await?;
        if target_next_sequence_number <= current {
            Ok(current)
        } else {
            Err(Error::ClientErrorWhileRequestingCertificate)
        }
    }

    pub async fn try_download_certificates_from<A>(
        &mut self,
        name: AuthorityName,
        mut client: A,
        account_id: AccountId,
        start: SequenceNumber,
        stop: SequenceNumber,
    ) -> Result<(), Error>
    where
        A: AuthorityClient + Send + Sync + 'static + Clone,
    {
        let range = SequenceNumberRange {
            start,
            limit: Some(usize::from(stop) - usize::from(start)),
        };
        let query = AccountInfoQuery {
            account_id: account_id.clone(),
            check_next_sequence_number: None,
            query_committee: false,
            query_sent_certificates_in_range: Some(range),
            query_received_certificates_excluding_first_nth: None,
        };
        if let Ok(response) = client.handle_account_info_query(query).await {
            if response.check(name).is_ok() {
                let AccountInfo {
                    queried_sent_certificates,
                    ..
                } = response.info;
                self.try_process_certificates(&account_id, queried_sent_certificates)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn synchronize_account_state<A>(
        &mut self,
        authorities: Vec<(AuthorityName, A)>,
        account_id: AccountId,
    ) -> Result<(SequenceNumber, RoundNumber), Error>
    where
        A: AuthorityClient + Send + Sync + 'static + Clone,
    {
        let infos: futures::stream::FuturesUnordered<_> = authorities
            .into_iter()
            .map(|(name, client)| {
                let mut node = self.clone();
                let id = account_id.clone();
                async move {
                    node.try_synchronize_account_state_from(name, client, id)
                        .await
                }
            })
            .collect();
        // TODO: This completely silents errors such as timeouts amd will be hard to debug.
        let values: Vec<_> = infos.filter_map(|x| async move { x.ok() }).collect().await;
        let maximum = values
            .into_iter()
            .max()
            .ok_or(Error::ClientErrorWhileRequestingCertificate)?;
        Ok(maximum)
    }

    pub async fn try_synchronize_account_state_from<A>(
        &mut self,
        name: AuthorityName,
        mut client: A,
        account_id: AccountId,
    ) -> Result<(SequenceNumber, RoundNumber), Error>
    where
        A: AuthorityClient + Send + Sync + 'static + Clone,
    {
        let start = self
            .current_next_sequence_number(account_id.clone())
            .await?;
        let range = SequenceNumberRange { start, limit: None };
        let query = AccountInfoQuery {
            account_id: account_id.clone(),
            check_next_sequence_number: None,
            query_committee: false,
            query_sent_certificates_in_range: Some(range),
            query_received_certificates_excluding_first_nth: None,
        };
        let info = match client.handle_account_info_query(query).await {
            Ok(response) if response.check(name).is_ok() => response.info,
            _ => {
                // Give up on this authority.
                return Ok((start, RoundNumber::default()));
            }
        };
        let next_sequence_number = match self
            .try_process_certificates(&account_id, info.queried_sent_certificates)
            .await?
        {
            Some(number) => number,
            None => {
                // Give up
                return Ok((start, RoundNumber::default()));
            }
        };
        let mut next_round = RoundNumber::default();
        if info.next_sequence_number != next_sequence_number {
            // Give up
            return Ok((next_sequence_number, next_round));
        }
        let manager = match info.manager {
            AccountManager::Multi(manager) => *manager,
            _ => {
                // Nothing to do.
                return Ok((next_sequence_number, next_round));
            }
        };
        if let Some(order) = manager.order {
            // Check the sequence number.
            if order.request.account_id != account_id
                || order.request.sequence_number != next_sequence_number
            {
                // Give up
                return Ok((next_sequence_number, next_round));
            }
            match self.handle_request_order(order).await {
                Ok(response) => {
                    next_round = response.info.manager.next_round();
                }
                Err(e) => {
                    log::warn!("Invalid request order: {}", e);
                }
            }
        }
        if let Some(cert) = manager.locked {
            if let Value::Validated { request } = &cert.value {
                // Check the sequence number.
                if request.account_id != account_id
                    || request.sequence_number != next_sequence_number
                {
                    // Give up
                    return Ok((next_sequence_number, next_round));
                }
                match self.handle_certificate(cert).await {
                    Ok(response) => {
                        next_round = response.info.manager.next_round();
                    }
                    Err(e) => {
                        log::warn!("Invalid certificate: {}", e);
                    }
                }
            }
        }
        Ok((next_sequence_number, next_round))
    }
}
