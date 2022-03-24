// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountManager, base_types::*, committee::Committee, downloader::*,
    ensure as my_ensure, error::Error, messages::*,
};
use async_trait::async_trait;
use failure::{bail, ensure};
use futures::{future, StreamExt};
use rand::seq::SliceRandom;
use std::collections::{btree_map, BTreeMap, HashMap, HashSet};

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

    /// Find the highest balance that is backed by a quorum of authorities.
    /// NOTE: This is only safe in the synchronous model, assuming a sufficient timeout value.
    async fn query_strong_majority_balance(&mut self) -> Balance;
}

/// Reference implementation of the `AccountClient` trait using many instances of
/// some `AuthorityClient` implementation for communication.
pub struct AccountClientState<AuthorityClient> {
    /// The off-chain account id.
    account_id: AccountId,
    /// Current identity as an owner.
    identity: Option<AccountOwner>,
    /// The committee.
    committee: Committee,
    /// How to talk to this committee.
    authority_clients: HashMap<AuthorityName, AuthorityClient>,
    /// Expected sequence number for the next certified request.
    /// This is also the number of certificates that we have created.
    next_sequence_number: SequenceNumber,
    /// Pending request.
    pending_request: Option<Request>,
    /// Known key pairs from present and past identities.
    known_key_pairs: BTreeMap<AccountOwner, KeyPair>,

    // The remaining fields are used to minimize networking, and may not always be persisted locally.
    /// Confirmed requests that we have created ("sent") and already included in the state
    /// of this account client. Certificate at index `i` should have sequence number `i`.
    /// When no certificate is pending/missing, `sent_certificates` should be of size `next_sequence_number`.
    sent_certificates: Vec<Certificate>,
    /// Known received certificates, indexed by account_id and sequence number.
    received_certificates: BTreeMap<(AccountId, SequenceNumber), Certificate>,
    /// The known spendable balance (including a possible initial funding for testing
    /// purposes, excluding unknown sent or received certificates).
    balance: Balance,
    /// Our identity plus other owners.
    multi_owners: Option<HashSet<AccountOwner>>,
    /// Support synchronization of received certificates.
    received_certificate_trackers: HashMap<AuthorityName, usize>,
    /// Consensus: next available round
    next_round: RoundNumber,
    /// Consensus: locked request
    locked_request: Option<(RoundNumber, Request)>,
}

impl<A> AccountClientState<A> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: AccountId,
        key_pair: Option<KeyPair>,
        committee: Committee,
        authority_clients: HashMap<AuthorityName, A>,
        next_sequence_number: SequenceNumber,
        sent_certificates: Vec<Certificate>,
        received_certificates: Vec<Certificate>,
        balance: Balance,
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
        Self {
            account_id,
            identity,
            committee,
            authority_clients,
            next_sequence_number,
            pending_request: None,
            multi_owners: None,
            known_key_pairs,
            sent_certificates,
            received_certificates: received_certificates
                .into_iter()
                .filter_map(|cert| Some((cert.value.confirmed_key()?, cert)))
                .collect(),
            received_certificate_trackers: HashMap::new(),
            balance,
            next_round: RoundNumber::default(),
            locked_request: None,
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

    pub fn balance(&self) -> Balance {
        self.balance
    }

    pub fn pending_request(&self) -> &Option<Request> {
        &self.pending_request
    }

    pub fn sent_certificates(&self) -> &Vec<Certificate> {
        &self.sent_certificates
    }

    pub fn received_certificates(&self) -> impl Iterator<Item = &Certificate> {
        self.received_certificates.values()
    }
}

#[derive(Clone)]
struct CertificateRequester<A> {
    committee: Committee,
    authority_clients: Vec<A>,
    account_id: AccountId,
}

impl<A> CertificateRequester<A> {
    fn new(committee: Committee, authority_clients: Vec<A>, account_id: AccountId) -> Self {
        Self {
            committee,
            authority_clients,
            account_id,
        }
    }
}

#[async_trait]
impl<A> Requester for CertificateRequester<A>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
{
    type Key = SequenceNumber;
    type Value = Result<Certificate, Error>;

    /// Try to find a (confirmation) certificate for the given account_id and sequence number.
    async fn query(&mut self, sequence_number: SequenceNumber) -> Result<Certificate, Error> {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            query_sent_certificates_in_range: Some(SequenceNumberRange {
                start: sequence_number,
                limit: Some(1),
            }),
            query_received_certificates_excluding_first_nth: None,
        };
        // Sequentially try each authority in random order.
        self.authority_clients.shuffle(&mut rand::thread_rng());
        for client in self.authority_clients.iter_mut() {
            let result = client.handle_account_info_query(query.clone()).await;
            if let Ok(AccountInfoResponse {
                queried_sent_certificates,
                ..
            }) = &result
            {
                if let Some(certificate) = queried_sent_certificates.first() {
                    if certificate.check(&self.committee).is_ok() {
                        if let Value::Confirmed { request } = &certificate.value {
                            if request.account_id == self.account_id
                                && request.sequence_number == sequence_number
                            {
                                return Ok(certificate.clone());
                            }
                        }
                    }
                }
            }
        }
        Err(Error::ClientErrorWhileRequestingCertificate)
    }
}

/// Used for communicate_requests
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum CommunicateAction {
    SubmitRequest(RequestOrder),
    FinalizeRequest(Certificate),
    SynchronizeNextSequenceNumber(SequenceNumber),
}

impl<A> AccountClientState<A>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
{
    #[cfg(test)]
    async fn query_certificate(
        &mut self,
        account_id: AccountId,
        sequence_number: SequenceNumber,
    ) -> Result<Certificate, Error> {
        CertificateRequester::new(
            self.committee.clone(),
            self.authority_clients.values().cloned().collect(),
            account_id,
        )
        .query(sequence_number)
        .await
    }

    /// Find the highest sequence number that is known to a quorum of authorities.
    /// NOTE: This assumes network connectivity and a sufficient timeout value.
    async fn broadcast_account_info_query(
        &mut self,
        account_id: AccountId,
        next_sequence_number: Option<SequenceNumber>,
    ) -> Vec<(AuthorityName, AccountInfoResponse)> {
        let range = next_sequence_number.map(|start| SequenceNumberRange { start, limit: None });
        let query = AccountInfoQuery {
            account_id,
            query_sent_certificates_in_range: range,
            query_received_certificates_excluding_first_nth: None,
        };
        let infos: futures::stream::FuturesUnordered<_> = self
            .authority_clients
            .iter_mut()
            .map(|(name, client)| {
                let fut = client.handle_account_info_query(query.clone());
                async move {
                    match fut.await {
                        Ok(info) => Some((*name, info)),
                        _ => None,
                    }
                }
            })
            .collect();
        infos.filter_map(|x| async move { x }).collect().await
    }

    /// Find the highest sequence number that is known to a quorum of authorities.
    /// This is meant only for testing.
    #[cfg(test)]
    async fn get_strong_majority_sequence_number(
        &mut self,
        account_id: AccountId,
    ) -> SequenceNumber {
        let infos = self.broadcast_account_info_query(account_id, None).await;
        let numbers = infos
            .into_iter()
            .map(|(name, info)| (name, info.next_sequence_number))
            .collect();
        self.committee.get_strong_majority_lower_bound(numbers)
    }

    /// Update our view of the account to include possible actions from another client.
    /// NOTE: This assumes network connectivity and a sufficient timeout value.
    async fn synchronize_account_information(&mut self) -> Result<(), Error> {
        assert_eq!(
            self.sent_certificates.len(),
            usize::from(self.next_sequence_number),
            "download_missing_sent_certificates must be called first"
        );
        let infos = self
            .broadcast_account_info_query(self.account_id.clone(), Some(self.next_sequence_number))
            .await;
        // First pass to update our local sequence number.
        for (_, info) in &infos {
            for certificate in &info.queried_sent_certificates {
                if certificate.check(&self.committee).is_ok() {
                    if let Value::Confirmed { request } = &certificate.value {
                        if request.account_id == self.account_id
                            && request.sequence_number == self.next_sequence_number
                        {
                            self.add_sent_certificate(certificate.clone())?;
                        }
                    }
                }
            }
        }
        let owners = match &self.multi_owners {
            None => return Ok(()),
            Some(owners) => owners,
        };
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
                if order.value.request.sequence_number != self.next_sequence_number {
                    continue;
                }
                // Make sure the order comes from a legitimate owner.
                if owners.contains(&order.owner)
                    && order.signature.check(&order.value, order.owner).is_err()
                {
                    continue;
                }
                if let Some(round) = order.value.round {
                    // Update the round.
                    if round >= self.next_round {
                        self.next_round = round.try_add_one()?;
                    }
                }
            }
            if let Some(cert) = manager.locked {
                if cert.check(&self.committee).is_err() {
                    continue;
                }
                if let Value::Validated { round, request } = cert.value {
                    // Check the sequence number.
                    if request.sequence_number != self.next_sequence_number {
                        continue;
                    }
                    // Update the locked request.
                    match &mut self.locked_request {
                        lr @ None => {
                            *lr = Some((round, request));
                        }
                        Some((rnd, req)) if round > *rnd => {
                            *rnd = round;
                            *req = request;
                        }
                        _ => (),
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
            .map(|(name, client)| {
                let execute = execute.clone();
                async move { (*name, execute(*name, client).await) }
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

    /// Broadcast confirmation orders and optionally one more request order.
    /// The corresponding sequence numbers should be consecutive and increasing.
    async fn communicate_requests(
        &mut self,
        account_id: AccountId,
        known_certificates: Vec<Certificate>,
        action: CommunicateAction,
    ) -> Result<Vec<Certificate>, failure::Error> {
        let target_sequence_number = match &action {
            CommunicateAction::SubmitRequest(order) => order.value.request.sequence_number,
            CommunicateAction::FinalizeRequest(certificate) => {
                certificate
                    .value
                    .validated_request()
                    .unwrap()
                    .sequence_number
            }
            CommunicateAction::SynchronizeNextSequenceNumber(seq) => *seq,
        };
        let requester = CertificateRequester::new(
            self.committee.clone(),
            self.authority_clients.values().cloned().collect(),
            account_id.clone(),
        );
        let (task, mut handle) = Downloader::start(
            requester,
            known_certificates.into_iter().filter_map(|cert| {
                let request = cert.value.confirmed_request()?;
                if request.account_id == account_id {
                    Some((request.sequence_number, Ok(cert)))
                } else {
                    None
                }
            }),
        );
        let committee = self.committee.clone();
        let result = self
            .communicate_with_quorum(|name, client| {
                let mut handle = handle.clone();
                let action = action.clone();
                let committee = &committee;
                let account_id = account_id.clone();
                Box::pin(async move {
                    // Figure out which certificates this authority is missing.
                    let query = AccountInfoQuery {
                        account_id,
                        query_sent_certificates_in_range: None,
                        query_received_certificates_excluding_first_nth: None,
                    };
                    let response = client.handle_account_info_query(query).await?;
                    let current_sequence_number = response.next_sequence_number;
                    // Download each missing certificate in reverse order using the downloader.
                    let mut missing_certificates = Vec::new();
                    let mut number = target_sequence_number.try_sub_one();
                    while let Ok(value) = number {
                        if value < current_sequence_number {
                            break;
                        }
                        let certificate = handle
                            .query(value)
                            .await
                            .map_err(|_| Error::ClientErrorWhileRequestingCertificate)??;
                        missing_certificates.push(certificate);
                        number = value.try_sub_one();
                    }
                    // Send all missing confirmation orders.
                    missing_certificates.reverse();
                    for certificate in missing_certificates {
                        client.handle_certificate(certificate).await?;
                    }
                    // Send the request order (if any) and return a vote.
                    match action {
                        CommunicateAction::SubmitRequest(order) => {
                            let result = client.handle_request_order(order).await;
                            match result {
                                Ok(AccountInfoResponse { manager, .. }) => {
                                    match manager.pending() {
                                        Some(vote) => {
                                            my_ensure!(
                                                vote.authority == name,
                                                Error::ClientErrorWhileProcessingRequestOrder
                                            );
                                            vote.check(committee)?;
                                            return Ok(Some(vote.clone()));
                                        }
                                        None => {
                                            return Err(
                                                Error::ClientErrorWhileProcessingRequestOrder,
                                            )
                                        }
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        CommunicateAction::FinalizeRequest(certificate) => {
                            let result = client.handle_certificate(certificate).await;
                            match result {
                                Ok(AccountInfoResponse { manager, .. }) => {
                                    match manager.pending() {
                                        Some(vote) => {
                                            my_ensure!(
                                                vote.authority == name,
                                                Error::ClientErrorWhileProcessingRequestOrder
                                            );
                                            vote.check(committee)?;
                                            return Ok(Some(vote.clone()));
                                        }
                                        None => {
                                            return Err(
                                                Error::ClientErrorWhileProcessingRequestOrder,
                                            )
                                        }
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        CommunicateAction::SynchronizeNextSequenceNumber(_) => (),
                    }
                    Ok(None)
                })
            })
            .await;
        let votes = match result {
            Ok(votes) => votes,
            Err(Some(Error::InactiveAccount(id)))
                if id == account_id
                    && matches!(action, CommunicateAction::SynchronizeNextSequenceNumber(_)) =>
            {
                // The account is visibly not active (yet or any more) so there is no need
                // to synchronize sequence numbers.
                return Ok(Vec::new());
            }
            Err(Some(err)) => bail!(
                "Failed to communicate with a quorum of authorities: {}",
                err
            ),
            Err(None) => {
                bail!("Failed to communicate with a quorum of authorities (multiple errors)")
            }
        };
        // Terminate downloader task and retrieve the content of the cache.
        handle.stop().await?;
        let mut certificates: Vec<_> = task.await.unwrap().filter_map(Result::ok).collect();
        let signatures: Vec<_> = votes
            .into_iter()
            .filter_map(|vote| match vote {
                Some(vote) => Some((vote.authority, vote.signature)),
                None => None,
            })
            .collect();
        match action {
            CommunicateAction::SubmitRequest(order) => {
                let value = match order.value.round {
                    Some(round) => Value::Validated {
                        request: order.value.request,
                        round,
                    },
                    None => Value::Confirmed {
                        request: order.value.request,
                    },
                };
                let certificate = Certificate::new(value, signatures);
                // Certificate is valid because
                // * `communicate_with_quorum` ensured a sufficient "weight" of
                // (non-error) answers were returned by authorities.
                // * each answer is a vote signed by the expected authority.
                certificates.push(certificate);
            }
            CommunicateAction::FinalizeRequest(validity_certificate) => {
                let request = validity_certificate
                    .value
                    .validated_request()
                    .unwrap()
                    .clone();
                let certificate = Certificate::new(Value::Confirmed { request }, signatures);
                certificates.push(certificate);
            }
            CommunicateAction::SynchronizeNextSequenceNumber(_) => (),
        }
        Ok(certificates)
    }

    /// Make sure we have all our certificates with sequence number
    /// in the range 0..self.next_sequence_number
    async fn download_missing_sent_certificates(&mut self) -> Result<(), Error> {
        let mut requester = CertificateRequester::new(
            self.committee.clone(),
            self.authority_clients.values().cloned().collect(),
            self.account_id.clone(),
        );
        while self.sent_certificates.len() < self.next_sequence_number.into() {
            let certificate = requester
                .query(SequenceNumber::from(self.sent_certificates.len() as u64))
                .await?;
            self.add_sent_certificate(certificate)?;
        }
        Ok(())
    }

    /// Attempt to download new received certificates.
    async fn synchronize_received_certificates(&mut self) -> Result<(), failure::Error> {
        let account_id = self.account_id.clone();
        let trackers = self.received_certificate_trackers.clone();
        let committee = self.committee.clone();
        let result = self
            .communicate_with_quorum(|name, client| {
                let committee = &committee;
                let account_id = &account_id;
                let tracker = *trackers.get(&name).unwrap_or(&0);
                Box::pin(async move {
                    // Retrieve new received certificates from this authority.
                    let query = AccountInfoQuery {
                        account_id: account_id.clone(),
                        query_sent_certificates_in_range: None,
                        query_received_certificates_excluding_first_nth: Some(tracker),
                    };
                    let response = client.handle_account_info_query(query).await?;
                    for certificate in &response.queried_received_certificates {
                        certificate.check(committee)?;
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
                    Ok((name, response))
                })
            })
            .await;
        match result {
            Ok(responses) => {
                for (name, response) in responses {
                    // Process received certificates.
                    for certificate in response.queried_received_certificates {
                        self.receive_certificate(certificate).await.unwrap_or(());
                    }
                    // Update tracker.
                    self.received_certificate_trackers
                        .insert(name, response.count_received_certificates);
                }
            }
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
        Ok(())
    }

    /// Send money.
    async fn transfer(
        &mut self,
        amount: Amount,
        recipient: Address,
        user_data: UserData,
    ) -> Result<Certificate, failure::Error> {
        // Trying to overspend may block the account. To prevent this, we compare with
        // the balance as we know it.
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
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    fn update_sent_certificates(
        &mut self,
        sent_certificates: Vec<Certificate>,
    ) -> Result<(), Error> {
        let n = self.sent_certificates.len();
        for (i, certificate) in sent_certificates.into_iter().enumerate() {
            if i < n {
                assert_eq!(certificate.value, self.sent_certificates[i].value);
            } else {
                self.add_sent_certificate(certificate)?;
            }
        }
        Ok(())
    }

    fn add_sent_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        let request = certificate
            .value
            .confirmed_request()
            .expect("was expecting a confirmation certificate");
        assert_eq!(
            u64::from(request.sequence_number),
            self.sent_certificates.len() as u64
        );
        // Execute operation locally.
        match &request.operation {
            Operation::Transfer { amount, .. } => {
                self.balance.try_sub_assign((*amount).into())?;
            }
            Operation::ChangeOwner { new_owner } => {
                self.identity = Some(*new_owner);
                self.multi_owners = None;
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.multi_owners = Some(new_owners.iter().cloned().collect());
                if self.identity.is_none() || !new_owners.contains(self.identity.as_ref().unwrap())
                {
                    self.identity = None;
                    // Search for a new identity that works.
                    // TODO: We probably shouldn't choose the first identity that
                    // works like this.
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
            Operation::OpenAccount { .. } => (),
        }
        // Record certificate.
        self.sent_certificates.push(certificate);
        let next_sequence_number = SequenceNumber::from(self.sent_certificates.len() as u64);
        if self.next_sequence_number < next_sequence_number {
            self.next_sequence_number = next_sequence_number;
            self.next_round = RoundNumber::default();
            self.locked_request = None;
        }
        Ok(())
    }

    async fn make_request_order(
        &mut self,
        request: Request,
    ) -> Result<RequestOrder, failure::Error> {
        if self.multi_owners.is_none() {
            let key_pair = self.key_pair()?;
            return Ok(RequestOrder::new(request.into(), key_pair));
        }
        let request_value = RequestValue {
            request,
            limited_to: None,
            round: Some(self.next_round),
        };
        let key_pair = self.key_pair()?;
        Ok(RequestOrder::new(request_value, key_pair))
    }

    /// Execute (or retry) a regular request order. Update local balance.
    /// If `with_confirmation` is false, we stop short of executing the finalized request.
    async fn execute_request(
        &mut self,
        request: Request,
        with_confirmation: bool,
    ) -> Result<Certificate, failure::Error> {
        // Download (hypothetically) missing historical data.
        self.download_missing_sent_certificates().await?;
        if self.multi_owners.is_some() {
            // We could be missing recent certificates created by other owners.
            self.synchronize_account_information().await?;
        }
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
        let order = self.make_request_order(request).await?;
        // Send the query.
        if self.multi_owners.is_some() {
            // Need two-round trips.
            let mut certificates = self
                .communicate_requests(
                    self.account_id.clone(),
                    self.sent_certificates.clone(),
                    CommunicateAction::SubmitRequest(order.clone()),
                )
                .await?;
            let certificate = certificates
                .pop()
                .expect("last order should have been validated");
            assert_eq!(
                certificate.value.validated_request(),
                Some(&order.value.request)
            );
            self.update_sent_certificates(certificates)?;

            let certificates = self
                .communicate_requests(
                    self.account_id.clone(),
                    self.sent_certificates.clone(),
                    CommunicateAction::FinalizeRequest(certificate),
                )
                .await?;
            self.update_sent_certificates(certificates)?;
        } else {
            // Only one round-trip is needed
            let certificates = self
                .communicate_requests(
                    self.account_id.clone(),
                    self.sent_certificates.clone(),
                    CommunicateAction::SubmitRequest(order.clone()),
                )
                .await?;
            self.update_sent_certificates(certificates)?;
        };
        // By now the request should be final.
        ensure!(
            self.sent_certificates
                .last()
                .expect("last order should have been confirmed")
                .value
                .confirmed_request()
                == Some(&order.value.request),
            "A different operation was executed in parallel (consider retrying the operation)"
        );
        self.pending_request = None;
        // Confirm last request certificate if needed.
        if with_confirmation {
            self.communicate_requests(
                self.account_id.clone(),
                self.sent_certificates.clone(),
                CommunicateAction::SynchronizeNextSequenceNumber(self.next_sequence_number),
            )
            .await?;
        }
        Ok(self.sent_certificates.last().unwrap().clone())
    }
}

#[async_trait]
impl<A> AccountClient for AccountClientState<A>
where
    A: AuthorityClient + Send + Sync + Clone + 'static,
{
    async fn query_strong_majority_balance(&mut self) -> Balance {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let numbers: futures::stream::FuturesUnordered<_> = self
            .authority_clients
            .iter_mut()
            .map(|(name, client)| {
                let fut = client.handle_account_info_query(query.clone());
                async move {
                    match fut.await {
                        Ok(info) => Some((*name, info.balance)),
                        _ => None,
                    }
                }
            })
            .collect();
        self.committee.get_strong_majority_lower_bound(
            numbers.filter_map(|x| async move { x }).collect().await,
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
        if let Some(request) = &self.pending_request {
            // Finish executing the previous request.
            let request = request.clone();
            self.execute_request(request, /* with_confirmation */ false)
                .await?;
        }
        self.synchronize_received_certificates().await?;
        self.download_missing_sent_certificates().await?;
        Ok(self.balance)
    }

    async fn receive_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(), failure::Error> {
        let request = certificate
            .value
            .confirmed_request()
            .ok_or_else(|| failure::format_err!("Was expecting a confirmed account operation"))?;
        let account_id = &request.account_id;
        ensure!(
            request.operation.recipient() == Some(&self.account_id),
            "Request should be received by us."
        );
        self.communicate_requests(
            account_id.clone(),
            vec![certificate.clone()],
            CommunicateAction::SynchronizeNextSequenceNumber(
                request.sequence_number.try_add_one()?,
            ),
        )
        .await?;
        // Everything worked: update the local balance.
        if let btree_map::Entry::Vacant(entry) = self
            .received_certificates
            .entry(certificate.value.confirmed_key().unwrap())
        {
            if let Some(amount) = request.operation.received_amount() {
                self.balance.try_add_assign(amount.into())?;
            }
            entry.insert(certificate);
        }
        Ok(())
    }

    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate, failure::Error> {
        let new_owner = key_pair.public();
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
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
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
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
        let owner = self.identity.ok_or_else(|| {
            failure::format_err!("Cannot share ownership for an account that we don't own")
        })?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeMultipleOwners {
                new_owners: vec![owner, new_owner],
            },
            sequence_number: self.next_sequence_number,
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
        let new_id = self.account_id.make_child(self.next_sequence_number);
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::OpenAccount { new_id, new_owner },
            sequence_number: self.next_sequence_number,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn close_account(&mut self) -> Result<Certificate, failure::Error> {
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::CloseAccount,
            sequence_number: self.next_sequence_number,
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
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::Transfer {
                recipient: Address::Account(recipient),
                amount,
                user_data,
            },
            sequence_number: self.next_sequence_number,
        };
        let new_certificate = self
            .execute_request(request, /* with_confirmation */ false)
            .await?;
        Ok(new_certificate)
    }
}
