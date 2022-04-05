// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::{AccountManager, AccountState},
    authority::{Authority, Worker, WorkerState},
    base_types::*,
    committee::Committee,
    ensure as my_ensure,
    error::Error,
    messages::*,
    storage::StorageClient,
};
use async_trait::async_trait;
use failure::{bail, ensure};
use futures::{future, StreamExt};
use rand::seq::SliceRandom;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

/// How to communicate with an authority.
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

/// How to communicate with an account across all the authorities. As a rule,
/// operations are considered successful (and communication may stop) when they succeeded
/// in gathering a quorum of responses.
#[async_trait]
pub trait AccountClient {
    /// Send money to an account.
    async fn transfer_to_account(
        &mut self,
        amount: Amount,
        recipient: AccountId,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error>;

    /// Burn money.
    async fn burn(
        &mut self,
        amount: Amount,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error>;

    /// Process confirmed operation for which this account is a recipient.
    async fn receive_certificate(&mut self, certificate: Certificate)
        -> Result<(), failure::Error>;

    /// Rotate the key of the account.
    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate, failure::Error>;

    /// Transfer ownership of the account.
    async fn transfer_ownership(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error>;

    /// Add another owner to the account.
    async fn share_ownership(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error>;

    /// Open a new account with a derived UID.
    async fn open_account(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error>;

    /// Close the account (and lose everything in it!!)
    async fn close_account(&mut self) -> Result<Certificate, failure::Error>;

    /// Send money to an account.
    /// Do not check balance. (This may block the client)
    /// Do not confirm the transaction.
    async fn transfer_to_account_unsafe_unconfirmed(
        &mut self,
        amount: Amount,
        recipient: AccountId,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error>;

    /// Compute a safe (i.e. pessimistic) balance by synchronizing our "sent" certificates
    /// with authorities, and otherwise using local data on received transfers (i.e.
    /// certificates that were locally processed by `receive_from_account`).
    async fn synchronize_balance(&mut self) -> Result<Balance, failure::Error>;

    /// Attempt to resume an operation that previously failed.
    async fn retry_pending_request(&mut self) -> Result<Option<Certificate>, failure::Error>;

    /// Attempt to resume an operation that previously failed.
    async fn clear_pending_request(&mut self);

    /// Find the highest balance that is provably backed by at least one honest
    /// (sufficiently up-to-date) authority. This is a conservative approximation.
    async fn query_safe_balance(&mut self) -> Balance;
}

/// Reference implementation of the `AccountClient` trait using many instances of some
/// `AuthorityClient` implementation for communication, and a client to some (local)
/// storage.
pub struct AccountClientState<AuthorityClient, StorageClient> {
    /// The off-chain account id.
    account_id: AccountId,
    /// Current identity as an owner.
    identity: Option<AccountOwner>,
    /// The committee.
    committee: Committee,
    /// How to talk to this committee.
    authority_clients: Vec<(AuthorityName, AuthorityClient)>,
    /// Expected sequence number for the next certified request.
    /// This is also the number of certificates that we have created.
    next_sequence_number: SequenceNumber,
    /// Pending request.
    pending_request: Option<Request>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<AccountOwner, KeyPair>,

    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<AuthorityName, usize>,
    /// How much time to wait between attempts when we wait for a cross-shard update.
    cross_shard_delay: Duration,
    /// How many times we are willing to retry a request that depends on cross-shard updates.
    cross_shard_retries: usize,
    /// Manage the execution state and the local storage of the accounts that we are
    /// tracking.
    state: WorkerState<StorageClient>,
}

impl<A, S> AccountClientState<A, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: AccountId,
        key_pair: Option<KeyPair>,
        committee: Committee,
        authority_clients: Vec<(AuthorityName, A)>,
        storage_client: S,
        next_sequence_number: SequenceNumber,
        cross_shard_delay: Duration,
        cross_shard_retries: usize,
    ) -> Self {
        let mut known_key_pairs = BTreeMap::new();
        let identity = match key_pair {
            None => None,
            Some(kp) => {
                let id = kp.public();
                known_key_pairs.insert(kp.public(), kp);
                Some(id)
            }
        };
        let state = WorkerState::new(committee.clone(), None, storage_client);
        Self {
            account_id,
            identity,
            committee,
            authority_clients,
            next_sequence_number,
            pending_request: None,
            known_key_pairs,
            received_certificate_trackers: HashMap::new(),
            cross_shard_delay,
            cross_shard_retries,
            state,
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn identity(&self) -> Option<AccountOwner> {
        self.identity
    }

    pub fn key_pair(&self) -> Result<&KeyPair, failure::Error> {
        let id = self
            .identity
            .ok_or_else(|| failure::format_err!("No identity was setup"))?;
        self.known_key_pairs.get(&id).ok_or_else(|| {
            failure::format_err!("Cannot make request for an account that we don't own")
        })
    }

    pub fn next_sequence_number(&self) -> SequenceNumber {
        self.next_sequence_number
    }

    pub fn pending_request(&self) -> &Option<Request> {
        &self.pending_request
    }
}

impl<A, S> AccountClientState<A, S>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
    S: StorageClient + Clone + 'static,
{
    async fn count_sent_certificates(&mut self, account_id: AccountId) -> Result<usize, Error> {
        let query = AccountInfoQuery {
            account_id,
            check_next_sequence_number: None,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let response = self.state.handle_account_info_query(query).await?;
        Ok(response.info.next_sequence_number.into())
    }

    async fn manager(&mut self) -> AccountManager {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            check_next_sequence_number: None,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let response = self.state.handle_account_info_query(query).await.unwrap();
        response.info.manager
    }

    async fn has_multi_owners(&mut self) -> bool {
        let manager = self.manager().await;
        matches!(manager, AccountManager::Multi(_))
    }

    async fn next_round(&mut self) -> RoundNumber {
        let manager = self.manager().await;
        match manager {
            AccountManager::Multi(m) => {
                let round = m.round();
                round.try_add_one().unwrap_or(round)
            }
            _ => RoundNumber::default(),
        }
    }

    async fn balance(&mut self) -> Balance {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            check_next_sequence_number: None,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: Some(0),
        };
        let response = self.state.handle_account_info_query(query).await.unwrap();
        response.info.balance
    }
}

/// Used for `communicate_account_updates`
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum CommunicateAction {
    SubmitRequestForConfirmation(RequestOrder),
    SubmitRequestForValidation(RequestOrder),
    FinalizeRequest(Certificate),
    AdvanceToNextSequenceNumber(SequenceNumber),
}

impl<A, S> AccountClientState<A, S>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
    S: StorageClient + Clone + 'static,
{
    /// Try to find a (confirmation) certificate for the given account_id and sequence number.
    async fn fetch_and_process_certificate(
        &mut self,
        account_id: AccountId,
        sequence_number: SequenceNumber,
    ) -> Result<Certificate, Error> {
        let query = AccountInfoQuery {
            account_id: account_id.clone(),
            check_next_sequence_number: None,
            query_sent_certificates_in_range: Some(SequenceNumberRange {
                start: sequence_number,
                limit: Some(1),
            }),
            query_received_certificates_excluding_first_nth: None,
        };
        // Sequentially try each authority in random order.
        let mut authority_clients = self
            .authority_clients
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect::<Vec<_>>();
        authority_clients.shuffle(&mut rand::thread_rng());
        for (name, mut client) in authority_clients {
            if let Ok(response) = client.handle_account_info_query(query.clone()).await {
                if response.check(name).is_ok() {
                    let AccountInfo {
                        mut queried_sent_certificates,
                        ..
                    } = response.info;
                    if let Some(certificate) = queried_sent_certificates.pop() {
                        if let Value::Confirmed { request } = &certificate.value {
                            if request.account_id == account_id
                                && request.sequence_number == sequence_number
                            {
                                match self.process_certificate(certificate.clone()).await {
                                    Ok(()) => {
                                        // Success
                                        return Ok(certificate);
                                    }
                                    Err(Error::InvalidCertificate) => {
                                        // Try another authority.
                                    }
                                    Err(e) => {
                                        // Something is wrong: a valid certificate should not fail to execute.
                                        return Err(e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Err(Error::ClientErrorWhileRequestingCertificate)
    }

    async fn broadcast_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Vec<(AuthorityName, AccountInfo)> {
        let infos: futures::stream::FuturesUnordered<_> = self
            .authority_clients
            .iter_mut()
            .map(|(name, client)| {
                let fut = client.handle_account_info_query(query.clone());
                async move {
                    match fut.await {
                        Ok(response) => {
                            if response.check(*name).is_ok() {
                                Some((*name, response.info))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                }
            })
            .collect();
        infos.filter_map(|x| async move { x }).collect().await
    }

    /// Update our view of the account to include possible actions from another client.
    /// NOTE: This assumes network connectivity and a sufficient timeout value.
    async fn hard_synchronize_sent_certificates(&mut self) -> Result<(), Error> {
        assert_eq!(
            self.count_sent_certificates(self.account_id.clone())
                .await
                .unwrap(),
            usize::from(self.next_sequence_number),
        );
        let range = SequenceNumberRange {
            start: self.next_sequence_number,
            limit: None,
        };
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            check_next_sequence_number: None,
            query_sent_certificates_in_range: Some(range),
            query_received_certificates_excluding_first_nth: None,
        };
        let infos = self.broadcast_account_info_query(query).await;
        // First pass to update our local sequence number.
        'outer: for (_, info) in &infos {
            for certificate in &info.queried_sent_certificates {
                if let Value::Confirmed { request } = &certificate.value {
                    if request.account_id == self.account_id
                        && request.sequence_number == self.next_sequence_number
                    {
                        match self.process_certificate(certificate.clone()).await {
                            Ok(_) => (),
                            Err(Error::InvalidCertificate) => {
                                // Try another authority.
                                continue 'outer;
                            }
                            Err(e) => {
                                // Something is wrong: a valid certificate should not fail to execute.
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }
        // Second pass to update the current round number and locked_request.
        for (_, info) in infos {
            // Optional consistency checks.
            if info.next_sequence_number != self.next_sequence_number {
                continue;
            }
            let manager = match info.manager {
                AccountManager::Multi(manager) => *manager,
                _ => continue,
            };
            if let Some(order) = manager.order {
                // Check the sequence number.
                if order.request.account_id != self.account_id
                    || order.request.sequence_number != self.next_sequence_number
                {
                    continue;
                }
                if let Err(e) = self.state.handle_request_order(order).await {
                    log::warn!("Invalid request order: {}", e);
                }
            }
            if let Some(cert) = manager.locked {
                if let Value::Validated { request } = &cert.value {
                    // Check the sequence number.
                    if request.account_id != self.account_id
                        || request.sequence_number != self.next_sequence_number
                    {
                        continue;
                    }
                    if let Err(e) = self.state.handle_certificate(cert).await {
                        log::warn!("Invalid certificate: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Execute a sequence of actions in parallel for a quorum of authorities.
    async fn communicate_with_quorum<'a, V, F>(
        &'a mut self,
        execute: F,
    ) -> Result<Vec<V>, Option<Error>>
    where
        F: Fn(AuthorityName, &'a mut A) -> future::BoxFuture<'a, Result<V, Error>> + Clone,
    {
        let committee = &self.committee;
        let authority_clients = &mut self.authority_clients;
        let mut responses: futures::stream::FuturesUnordered<_> = authority_clients
            .iter_mut()
            .filter_map(|(name, client)| {
                let execute = execute.clone();
                if committee.weight(name) > 0 {
                    Some(async move { (*name, execute(*name, client).await) })
                } else {
                    // This should not happen but better prevent it because certificates
                    // are not allowed to include votes with weight 0.
                    None
                }
            })
            .collect();

        let mut values = Vec::new();
        let mut value_score = 0;
        let mut error_scores = HashMap::new();
        while let Some((name, result)) = responses.next().await {
            match result {
                Ok(value) => {
                    values.push(value);
                    value_score += committee.weight(&name);
                    if value_score >= committee.quorum_threshold() {
                        // Success!
                        return Ok(values);
                    }
                }
                Err(err) => {
                    let entry = error_scores.entry(err.clone()).or_insert(0);
                    *entry += committee.weight(&name);
                    if *entry >= committee.validity_threshold() {
                        // At least one honest node returned this error.
                        // No quorum can be reached, so return early.
                        return Err(Some(err));
                    }
                }
            }
        }

        // No specific error is available to report reliably.
        Err(None)
    }

    async fn send_certificate(
        name: AuthorityName,
        client: &mut A,
        delay: Duration,
        retries: usize,
        certificate: Certificate,
    ) -> Result<AccountInfo, Error> {
        let mut count = 0;
        loop {
            match client.handle_certificate(certificate.clone()).await {
                Ok(response) => {
                    response.check(name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveAccount(_)) if count < retries => {
                    // Retry
                    tokio::time::sleep(delay).await;
                    count += 1;
                    continue;
                }
                Err(e) => {
                    // Fail
                    return Err(e);
                }
            }
        }
    }

    async fn send_request_order(
        name: AuthorityName,
        client: &mut A,
        delay: Duration,
        retries: usize,
        order: RequestOrder,
    ) -> Result<AccountInfo, Error> {
        let mut count = 0;
        loop {
            match client.handle_request_order(order.clone()).await {
                Ok(response) => {
                    response.check(name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveAccount(_)) if count < retries => {
                    // Retry
                    tokio::time::sleep(delay).await;
                    count += 1;
                    continue;
                }
                Err(e) => {
                    // Fail
                    return Err(e);
                }
            }
        }
    }

    async fn send_account_information(
        storage_client: &mut S,
        authority_name: AuthorityName,
        authority_client: &mut A,
        mut account_id: AccountId,
        mut target_sequence_number: SequenceNumber,
        cross_shard_delay: Duration,
        cross_shard_retries: usize,
    ) -> Result<(), Error> {
        let mut jobs = Vec::new();
        loop {
            // Figure out which certificates this authority is missing.
            let query = AccountInfoQuery {
                account_id: account_id.clone(),
                check_next_sequence_number: None,
                query_sent_certificates_in_range: None,
                query_received_certificates_excluding_first_nth: None,
            };
            match authority_client.handle_account_info_query(query).await {
                Ok(response) => {
                    response.check(authority_name)?;
                    jobs.push((
                        account_id,
                        response.info.next_sequence_number,
                        target_sequence_number,
                        0,
                    ));
                    break;
                }
                Err(Error::InactiveAccount(id)) if id == account_id => match account_id.split() {
                    None => return Err(Error::InactiveAccount(id)),
                    Some((parent_id, number)) => {
                        jobs.push((
                            account_id,
                            SequenceNumber::from(0),
                            target_sequence_number,
                            cross_shard_retries,
                        ));
                        account_id = parent_id;
                        target_sequence_number = number.try_add_one()?;
                    }
                },
                Err(e) => return Err(e),
            }
        }
        for (account_id, initial_sequence_number, target_sequence_number, retries) in
            jobs.into_iter().rev()
        {
            // Obtain account state.
            let account = storage_client.read_account_or_default(&account_id).await?;
            // Send the requested certificates in order.
            for number in usize::from(initial_sequence_number)..usize::from(target_sequence_number)
            {
                let key = account
                    .confirmed_log
                    .get(number)
                    .expect("certificate should be known locally");
                let cert = storage_client.read_certificate(*key).await?;
                Self::send_certificate(
                    authority_name,
                    authority_client,
                    cross_shard_delay,
                    retries,
                    cert,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn send_account_information_as_a_receiver(
        storage_client: &mut S,
        authority_name: AuthorityName,
        authority_client: &mut A,
        account_id: AccountId,
        cross_shard_delay: Duration,
        cross_shard_retries: usize,
    ) -> Result<(), Error> {
        // Obtain account state.
        let account = storage_client.read_account_or_default(&account_id).await?;
        for (sender_id, sequence_number) in account.received_index.iter() {
            Self::send_account_information(
                storage_client,
                authority_name,
                authority_client,
                sender_id.clone(),
                sequence_number.try_add_one()?,
                cross_shard_delay,
                cross_shard_retries,
            )
            .await?;
        }
        Ok(())
    }

    async fn send_account_update(
        mut storage_client: S,
        account_id: AccountId,
        action: CommunicateAction,
        authority_name: AuthorityName,
        authority_client: &mut A,
        cross_shard_delay: Duration,
        cross_shard_retries: usize,
    ) -> Result<Option<Vote>, Error> {
        let target_sequence_number = match &action {
            CommunicateAction::SubmitRequestForValidation(order)
            | CommunicateAction::SubmitRequestForConfirmation(order) => {
                order.request.sequence_number
            }
            CommunicateAction::FinalizeRequest(certificate) => {
                certificate
                    .value
                    .validated_request()
                    .unwrap()
                    .sequence_number
            }
            CommunicateAction::AdvanceToNextSequenceNumber(seq) => *seq,
        };
        // Update the authority with missing information, if needed.
        Self::send_account_information(
            &mut storage_client,
            authority_name,
            authority_client,
            account_id.clone(),
            target_sequence_number,
            cross_shard_delay,
            cross_shard_retries,
        )
        .await?;
        // Send the request order (if any) and return a vote.
        match action {
            CommunicateAction::SubmitRequestForValidation(order)
            | CommunicateAction::SubmitRequestForConfirmation(order) => {
                let result = Self::send_request_order(
                    authority_name,
                    authority_client,
                    cross_shard_delay,
                    cross_shard_retries,
                    order.clone(),
                )
                .await;
                let info = match result {
                    Ok(info) => info,
                    Err(e) if AccountState::is_retriable_validation_error(&order.request, &e) => {
                        // Some received certificates may be missing for this authority
                        // (e.g. to make the balance sufficient) so we are going to
                        // synchronize them now.
                        Self::send_account_information_as_a_receiver(
                            &mut storage_client,
                            authority_name,
                            authority_client,
                            account_id,
                            cross_shard_delay,
                            cross_shard_retries,
                        )
                        .await?;
                        // Now retry the request.
                        Self::send_request_order(
                            authority_name,
                            authority_client,
                            cross_shard_delay,
                            cross_shard_retries,
                            order,
                        )
                        .await?
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(authority_name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingRequestOrder),
                }
            }
            CommunicateAction::FinalizeRequest(certificate) => {
                // The only cause for a retry is that the first certificate of a newly opened account.
                let cross_shard_retries = {
                    if target_sequence_number == SequenceNumber::default() {
                        cross_shard_retries
                    } else {
                        0
                    }
                };
                let info = Self::send_certificate(
                    authority_name,
                    authority_client,
                    cross_shard_delay,
                    cross_shard_retries,
                    certificate,
                )
                .await?;
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(authority_name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingRequestOrder),
                }
            }
            CommunicateAction::AdvanceToNextSequenceNumber(_) => (),
        }
        Ok(None)
    }

    /// Broadcast confirmation orders and optionally one more request order.
    /// The corresponding sequence numbers should be consecutive and increasing.
    async fn communicate_account_updates(
        &mut self,
        account_id: AccountId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>, failure::Error> {
        let storage_client = self.state.storage_client().clone();
        let cross_shard_delay = self.cross_shard_delay;
        let cross_shard_retries = self.cross_shard_retries;
        let result = self
            .communicate_with_quorum(|name, authority_client| {
                let action = action.clone();
                let storage_client = storage_client.clone();
                let account_id = account_id.clone();
                Box::pin(async move {
                    Self::send_account_update(
                        storage_client,
                        account_id,
                        action,
                        name,
                        authority_client,
                        cross_shard_delay,
                        cross_shard_retries,
                    )
                    .await
                })
            })
            .await;
        let votes = match result {
            Ok(votes) => votes,
            Err(Some(Error::InactiveAccount(id)))
                if id == account_id
                    && matches!(action, CommunicateAction::AdvanceToNextSequenceNumber(_)) =>
            {
                // The account is visibly not active (yet or any more) so there is no need
                // to synchronize sequence numbers.
                return Ok(None);
            }
            Err(Some(err)) => bail!(
                "Failed to communicate with a quorum of authorities: {}",
                err
            ),
            Err(None) => {
                bail!("Failed to communicate with a quorum of authorities (multiple errors)")
            }
        };
        let signatures: Vec<_> = votes
            .into_iter()
            .filter_map(|vote| match vote {
                Some(vote) => Some((vote.authority, vote.signature)),
                None => None,
            })
            .collect();
        match action {
            CommunicateAction::SubmitRequestForConfirmation(order) => {
                let value = Value::Confirmed {
                    request: order.request,
                };
                let certificate = Certificate::new(value, signatures);
                // Certificate is valid because
                // * `communicate_with_quorum` ensured a sufficient "weight" of
                // (non-error) answers were returned by authorities.
                // * each answer is a vote signed by the expected authority.
                Ok(Some(certificate))
            }
            CommunicateAction::SubmitRequestForValidation(order) => {
                let value = Value::Validated {
                    request: order.request,
                };
                let certificate = Certificate::new(value, signatures);
                Ok(Some(certificate))
            }
            CommunicateAction::FinalizeRequest(validity_certificate) => {
                let request = validity_certificate
                    .value
                    .validated_request()
                    .unwrap()
                    .clone();
                let certificate = Certificate::new(Value::Confirmed { request }, signatures);
                Ok(Some(certificate))
            }
            CommunicateAction::AdvanceToNextSequenceNumber(_) => Ok(None),
        }
    }

    /// Make sure we have all the certificates of this account with sequence number in the
    /// range 0..next_sequence_number. Due to the account creation protocol, we make sure
    /// that ancestor accounts are also synchronized.
    async fn synchronize_sent_certificates_for_account(
        &mut self,
        account_id: AccountId,
        next_sequence_number: SequenceNumber,
    ) -> Result<(), Error> {
        let last_id = match next_sequence_number.try_sub_one() {
            Ok(num) => account_id.make_child(num),
            Err(_) => account_id,
        };
        for ancestor_id in last_id.ancestors() {
            if let Some((account_id, sequence_number)) = ancestor_id.split() {
                let storage_number = self.count_sent_certificates(account_id.clone()).await?;
                for number in storage_number..=sequence_number.into() {
                    self.fetch_and_process_certificate(
                        account_id.clone(),
                        SequenceNumber::from(number as u64),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Make sure we have all our certificates with sequence number
    /// in the range 0..self.next_sequence_number
    async fn synchronize_sent_certificates(&mut self) -> Result<(), Error> {
        self.synchronize_sent_certificates_for_account(
            self.account_id.clone(),
            self.next_sequence_number,
        )
        .await?;
        if self.has_multi_owners().await {
            // We could be missing recent certificates created by other owners.
            self.hard_synchronize_sent_certificates().await?;
        }
        Ok(())
    }

    /// Attempt to download new received certificates.
    async fn synchronize_received_certificates(&mut self) -> Result<(), failure::Error> {
        let account_id = self.account_id.clone();
        let trackers = self.received_certificate_trackers.clone();
        let result = self
            .communicate_with_quorum(|name, client| {
                let account_id = &account_id;
                let tracker = *trackers.get(&name).unwrap_or(&0);
                Box::pin(async move {
                    // Retrieve new received certificates from this authority.
                    let query = AccountInfoQuery {
                        account_id: account_id.clone(),
                        check_next_sequence_number: None,
                        query_sent_certificates_in_range: None,
                        query_received_certificates_excluding_first_nth: Some(tracker),
                    };
                    let response = client.handle_account_info_query(query).await?;
                    // TODO: These quick verifications are not enough to discard (1) all
                    // invalid certificates or (2) spammy received certificates. (1): a
                    // dishonest authority could try to make us work by producing
                    // good-looking certificates with high sequence numbers. (2): Other
                    // users could send us a lot of uninteresting transactions.
                    response.check(name)?;
                    for certificate in &response.info.queried_received_certificates {
                        let request = certificate
                            .value
                            .confirmed_request()
                            .ok_or(Error::ClientErrorWhileRequestingCertificate)?;
                        let recipient = request
                            .operation
                            .recipient()
                            .ok_or(Error::ClientErrorWhileRequestingCertificate)?;
                        my_ensure!(
                            recipient == account_id,
                            Error::ClientErrorWhileRequestingCertificate
                        );
                    }
                    Ok((name, response.info))
                })
            })
            .await;
        let responses = match result {
            Ok(responses) => responses,
            Err(Some(Error::InactiveAccount(id))) if id == account_id => {
                // The account is visibly not active (yet or any more) so there is no need
                // to synchronize received certificates.
                return Ok(());
            }
            Err(Some(err)) => bail!(
                "Failed to communicate with a quorum of authorities: {}",
                err
            ),
            Err(None) => {
                bail!("Failed to communicate with a quorum of authorities (multiple errors)")
            }
        };
        'outer: for (name, response) in responses {
            // Process received certificates.
            for certificate in response.queried_received_certificates {
                if self.receive_certificate(certificate).await.is_err() {
                    // Do not update the authority's tracker in case of error.
                    // Move on to the next authority.
                    continue 'outer;
                }
            }
            // Update tracker.
            self.received_certificate_trackers
                .insert(name, response.count_received_certificates);
        }
        Ok(())
    }

    /// Send money.
    async fn transfer(
        &mut self,
        amount: Amount,
        recipient: Address,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error> {
        let balance = self.synchronize_balance().await?;
        ensure!(
            Balance::from(amount) <= balance,
            "Requested amount ({}) is not backed by sufficient funds ({})",
            amount,
            balance
        );
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::Transfer {
                recipient,
                amount,
                user_data,
            },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn process_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        // Start by having our worker handle the certificate.
        let (_, mut requests) = self.state.handle_certificate(certificate.clone()).await?;
        // If the certificate is from us, update the client-specific state as well.
        if let Some(request) = certificate.value.confirmed_request() {
            if request.account_id == self.account_id {
                self.next_sequence_number = request.sequence_number.try_add_one()?;
            }
            // Manage our identity
            match &request.operation {
                Operation::ChangeOwner { new_owner } => {
                    self.identity = Some(*new_owner);
                }
                Operation::ChangeMultipleOwners { new_owners } => {
                    if self.identity.is_none()
                        || !new_owners.contains(self.identity.as_ref().unwrap())
                    {
                        self.identity = None;
                        // Search for a new identity that works. TODO: We probably
                        // shouldn't just pick the first identity that works.
                        for owner in new_owners {
                            if let Some(value) = self.known_key_pairs.get(owner) {
                                self.identity = Some(value.public());
                                break;
                            }
                        }
                    }
                }
                Operation::CloseAccount => {
                    self.identity = None;
                }
                _ => (),
            }
        }
        // Finally, handle the relevant cross-shard requests.
        while let Some(request) = requests.pop() {
            requests.extend(self.state.handle_cross_shard_request(request).await?);
        }
        Ok(())
    }

    /// Execute (or retry) a regular request order. Update local balance.
    /// If `with_confirmation` is false, we stop short of executing the finalized request.
    async fn execute_request(
        &mut self,
        request: Request,
        with_confirmation: bool,
    ) -> Result<Certificate, failure::Error> {
        ensure!(
            matches!(&self.pending_request, None)
                || matches!(&self.pending_request, Some(r) if *r == request),
            "Client state has a different pending request",
        );
        ensure!(
            request.sequence_number == self.next_sequence_number,
            "Unexpected sequence number"
        );
        // Remember what we are trying to do
        self.pending_request = Some(request.clone());
        // Build the initial query.
        let key_pair = self.key_pair()?;
        let order = RequestOrder::new(request, key_pair);
        // Send the query.
        let final_certificate = {
            if self.has_multi_owners().await {
                // Need two-round trips.
                let certificate = self
                    .communicate_account_updates(
                        self.account_id.clone(),
                        CommunicateAction::SubmitRequestForValidation(order.clone()),
                    )
                    .await?
                    .expect("a certificate");
                assert_eq!(certificate.value.validated_request(), Some(&order.request));
                self.communicate_account_updates(
                    self.account_id.clone(),
                    CommunicateAction::FinalizeRequest(certificate),
                )
                .await?
                .expect("a certificate")
            } else {
                // Only one round-trip is needed
                self.communicate_account_updates(
                    self.account_id.clone(),
                    CommunicateAction::SubmitRequestForConfirmation(order.clone()),
                )
                .await?
                .expect("a certificate")
            }
        };
        // By now the request should be final.
        ensure!(
            final_certificate.value.confirmed_request() == Some(&order.request),
            "A different operation was executed in parallel (consider retrying the operation)"
        );
        self.process_certificate(final_certificate.clone()).await?;
        self.pending_request = None;
        // Communicate the new certificate now if needed.
        if with_confirmation {
            self.communicate_account_updates(
                self.account_id.clone(),
                CommunicateAction::AdvanceToNextSequenceNumber(self.next_sequence_number),
            )
            .await?;
        }
        Ok(final_certificate)
    }
}

#[async_trait]
impl<A, S> AccountClient for AccountClientState<A, S>
where
    A: AuthorityClient + Send + Sync + Clone + 'static,
    S: StorageClient + Clone + 'static,
{
    async fn query_safe_balance(&mut self) -> Balance {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            /// This is necessary to make sure that the response is conservative.
            check_next_sequence_number: Some(self.next_sequence_number),
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let infos = self.broadcast_account_info_query(query).await;
        self.committee.get_validity_lower_bound(
            infos
                .into_iter()
                .map(|(name, info)| (name, info.balance))
                .collect(),
        )
    }

    async fn transfer_to_account(
        &mut self,
        amount: Amount,
        recipient: AccountId,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error> {
        self.transfer(amount, Address::Account(recipient), user_data)
            .await
    }

    async fn burn(
        &mut self,
        amount: Amount,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error> {
        self.transfer(amount, Address::Burn, user_data).await
    }

    async fn synchronize_balance(&mut self) -> Result<Balance, failure::Error> {
        self.synchronize_received_certificates().await?;
        self.synchronize_sent_certificates().await?;
        Ok(self.balance().await)
    }

    async fn retry_pending_request(&mut self) -> Result<Option<Certificate>, failure::Error> {
        self.synchronize_received_certificates().await?;
        self.synchronize_sent_certificates().await?;
        match &self.pending_request {
            Some(request) => {
                // Finish executing the previous request.
                let mut request = request.clone();
                request.round = self.next_round().await;
                let certificate = self
                    .execute_request(request, /* with_confirmation */ true)
                    .await?;
                Ok(Some(certificate))
            }
            None => Ok(None),
        }
    }

    async fn clear_pending_request(&mut self) {
        self.pending_request = None;
    }

    async fn receive_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(), failure::Error> {
        let request = certificate
            .value
            .confirmed_request()
            .ok_or_else(|| failure::format_err!("Was expecting a confirmed account operation"))?
            .clone();
        let account_id = &request.account_id;
        ensure!(
            request.operation.recipient() == Some(&self.account_id),
            "Request should be received by us."
        );
        // Recover history from the network.
        self.synchronize_sent_certificates_for_account(account_id.clone(), request.sequence_number)
            .await?;
        // Process the received operation.
        self.process_certificate(certificate).await?;
        // Make sure a quorum of authorities are up-to-date for data availability.
        self.communicate_account_updates(
            account_id.clone(),
            CommunicateAction::AdvanceToNextSequenceNumber(request.sequence_number.try_add_one()?),
        )
        .await?;
        Ok(())
    }

    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let new_owner = key_pair.public();
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        self.known_key_pairs.insert(key_pair.public(), key_pair);
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn transfer_ownership(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn share_ownership(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let owner = self.identity.ok_or_else(|| {
            failure::format_err!("Cannot share ownership for an account that we don't own")
        })?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeMultipleOwners {
                new_owners: vec![owner, new_owner],
            },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn open_account(
        &mut self,
        new_owner: AccountOwner,
    ) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let new_id = self.account_id.make_child(self.next_sequence_number);
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::OpenAccount { new_id, new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn close_account(&mut self) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::CloseAccount,
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn transfer_to_account_unsafe_unconfirmed(
        &mut self,
        amount: Amount,
        recipient: AccountId,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error> {
        self.synchronize_sent_certificates().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::Transfer {
                recipient: Address::Account(recipient),
                amount,
                user_data,
            },
            sequence_number: self.next_sequence_number,
            round: self.next_round().await,
        };
        let new_certificate = self
            .execute_request(request, /* with_confirmation */ false)
            .await?;
        Ok(new_certificate)
    }
}
