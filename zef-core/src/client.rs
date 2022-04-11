// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use zef_base::{
    account::AccountManager,
    base_types::*,
    committee::Committee,
    ensure as my_ensure,
    error::Error,
    messages::*,
};
use crate::{
    node::{AuthorityClient, LocalNodeClient},
    storage::StorageClient,
    updater::{communicate_with_quorum, AuthorityUpdater, CommunicateAction},
    worker::WorkerState,
};
use async_trait::async_trait;
use failure::{bail, ensure};
use futures::StreamExt;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

#[cfg(test)]
#[path = "unit_tests/client_tests.rs"]
mod client_tests;

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

    /// Retry the last pending request
    async fn retry_pending_request(&mut self) -> Result<Option<Certificate>, failure::Error>;

    /// Clear the information on any operation that previously failed.
    async fn clear_pending_request(&mut self);

    /// Find the highest balance that is provably backed by at least one honest
    /// (sufficiently up-to-date) authority. This is a conservative approximation.
    async fn query_safe_balance(&mut self) -> Result<Balance, Error>;
}

/// Reference implementation of the `AccountClient` trait using many instances of some
/// `AuthorityClient` implementation for communication, and a client to some (local)
/// storage.
pub struct AccountClientState<AuthorityClient, StorageClient> {
    /// The off-chain account id.
    account_id: AccountId,
    /// How to talk to this committee.
    authority_clients: Vec<(AuthorityName, AuthorityClient)>,
    /// Sequence number that we plan to use for the next request.
    /// We track this value outside local storage mainly for security reasons.
    next_sequence_number: SequenceNumber,
    /// Round number that we plan to use for the next request.
    next_round: RoundNumber,
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
    /// Local node to manage the execution state and the local storage of the accounts that we are
    /// tracking.
    node_client: LocalNodeClient<StorageClient>,
}

impl<A, S> AccountClientState<A, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: AccountId,
        known_key_pairs: Vec<KeyPair>,
        authority_clients: Vec<(AuthorityName, A)>,
        storage_client: S,
        next_sequence_number: SequenceNumber,
        cross_shard_delay: Duration,
        cross_shard_retries: usize,
    ) -> Self {
        let known_key_pairs = known_key_pairs
            .into_iter()
            .map(|kp| (kp.public(), kp))
            .collect();
        let state = WorkerState::new(None, storage_client);
        let node_client = LocalNodeClient::new(state);
        Self {
            account_id,
            authority_clients,
            next_sequence_number,
            next_round: RoundNumber::default(),
            pending_request: None,
            known_key_pairs,
            received_certificate_trackers: HashMap::new(),
            cross_shard_delay,
            cross_shard_retries,
            node_client,
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
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
    async fn account_info(&mut self) -> AccountInfo {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            check_next_sequence_number: None,
            query_committee: false,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: Some(0),
        };
        let response = self
            .node_client
            .handle_account_info_query(query)
            .await
            .unwrap();
        response.info
    }

    async fn balance(&mut self) -> Balance {
        self.account_info().await.balance
    }

    async fn has_multi_owners(&mut self) -> bool {
        let manager = self.account_info().await.manager;
        matches!(manager, AccountManager::Multi(_))
    }

    async fn committee(&mut self) -> Result<Committee, Error> {
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            check_next_sequence_number: None,
            query_committee: true,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let response = self
            .node_client
            .handle_account_info_query(query)
            .await
            .unwrap();
        response
            .info
            .queried_committee
            .ok_or_else(|| Error::InactiveAccount(self.account_id.clone()))
    }

    async fn identity(&mut self) -> Result<AccountOwner, failure::Error> {
        match self.account_info().await.manager {
            AccountManager::Single(m) => {
                if !self.known_key_pairs.contains_key(&m.owner) {
                    bail!(
                        "No key available to interact with single-owner account {}",
                        self.account_id
                    );
                }
                Ok(m.owner)
            }
            AccountManager::Multi(m) => {
                let mut identities = Vec::new();
                for owner in &m.owners {
                    if self.known_key_pairs.contains_key(owner) {
                        identities.push(*owner);
                    }
                }
                if identities.is_empty() {
                    bail!(
                        "Cannot find suitable identity to interact with multi-owner account {}",
                        self.account_id
                    );
                }
                if identities.len() >= 2 {
                    bail!(
                        "Found several possible identities to interact with multi-owner account {}",
                        self.account_id
                    );
                }
                Ok(identities.pop().unwrap())
            }
            AccountManager::None => Err(Error::InactiveAccount(self.account_id.clone()).into()),
        }
    }

    pub async fn key_pair(&mut self) -> Result<&KeyPair, failure::Error> {
        let id = self.identity().await?;
        Ok(self
            .known_key_pairs
            .get(&id)
            .expect("key should be known at this point"))
    }
}

impl<A, S> AccountClientState<A, S>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
    S: StorageClient + Clone + 'static,
{
    async fn broadcast_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Vec<(AuthorityName, AccountInfo)> {
        let infos: futures::stream::FuturesUnordered<_> = self
            .authority_clients
            .iter_mut()
            .map(|(name, client)| {
                let query = query.clone();
                async move {
                    match client.handle_account_info_query(query).await {
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

    /// Prepare the account for the next operation.
    /// * Verify that our local storage contains enough history compared to the
    ///   expected sequence number. Otherwise, download the missing history from the
    ///   network.
    /// * For multi-owner accounts, further synchronize the sequence number and the round
    ///   from the network.
    /// * Update `self.next_sequence_number` and `self.next_round`.
    async fn prepare_account(&mut self) -> Result<(), Error> {
        self.next_sequence_number = self
            .node_client
            .download_certificates(
                self.authority_clients.clone(),
                self.account_id.clone(),
                self.next_sequence_number,
            )
            .await?;
        if self.has_multi_owners().await {
            // We could be missing recent certificates created by other owners.
            let (next_sequence_number, next_round) = self
                .node_client
                .synchronize_account_state(self.authority_clients.clone(), self.account_id.clone())
                .await?;
            self.next_sequence_number = next_sequence_number;
            self.next_round = next_round;
        }
        Ok(())
    }

    /// Broadcast confirmation orders and optionally one more request order.
    /// The corresponding sequence numbers should be consecutive and increasing.
    async fn communicate_account_updates(
        &mut self,
        committee: &Committee,
        account_id: AccountId,
        action: CommunicateAction,
    ) -> Result<Option<Certificate>, failure::Error> {
        let storage_client = self.node_client.storage_client().await;
        let cross_shard_delay = self.cross_shard_delay;
        let cross_shard_retries = self.cross_shard_retries;
        let result = communicate_with_quorum(&self.authority_clients, committee, |name, client| {
            let mut updater = AuthorityUpdater {
                name,
                client,
                store: storage_client.clone(),
                delay: cross_shard_delay,
                retries: cross_shard_retries,
            };
            let action = action.clone();
            let account_id = account_id.clone();
            Box::pin(async move { updater.send_account_update(account_id, action).await })
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

    /// Attempt to download new received certificates.
    ///
    /// This is a best effort: it will only find certificates that have been confirmed
    /// amongst sufficiently many authorities of the current committee of the target
    /// account.
    ///
    /// However, this should be the case whenever a sender's account is still in use and
    /// is regularly upgraded to new committees.
    async fn find_received_certificates(&mut self) -> Result<(), failure::Error> {
        let account_id = self.account_id.clone();
        let committee = self.committee().await?;
        let trackers = self.received_certificate_trackers.clone();
        let result =
            communicate_with_quorum(&self.authority_clients, &committee, |name, mut client| {
                let account_id = &account_id;
                let tracker = *trackers.get(&name).unwrap_or(&0);
                Box::pin(async move {
                    // Retrieve new received certificates from this authority.
                    let query = AccountInfoQuery {
                        account_id: account_id.clone(),
                        check_next_sequence_number: None,
                        query_committee: false,
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
            round: self.next_round,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn process_certificate(&mut self, certificate: Certificate) -> Result<(), Error> {
        let info = self.node_client.handle_certificate(certificate).await?.info;
        if info.account_id == self.account_id
            && (info.next_sequence_number, info.manager.next_round())
                > (self.next_sequence_number, self.next_round)
        {
            self.next_sequence_number = info.next_sequence_number;
            self.next_round = info.manager.next_round();
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
        let key_pair = self.key_pair().await?;
        let order = RequestOrder::new(request, key_pair);
        // Send the query.
        let committee = self.committee().await?;
        let final_certificate = {
            if self.has_multi_owners().await {
                // Need two-round trips.
                let certificate = self
                    .communicate_account_updates(
                        &committee,
                        self.account_id.clone(),
                        CommunicateAction::SubmitRequestForValidation(order.clone()),
                    )
                    .await?
                    .expect("a certificate");
                assert_eq!(certificate.value.validated_request(), Some(&order.request));
                self.communicate_account_updates(
                    &committee,
                    self.account_id.clone(),
                    CommunicateAction::FinalizeRequest(certificate),
                )
                .await?
                .expect("a certificate")
            } else {
                // Only one round-trip is needed
                self.communicate_account_updates(
                    &committee,
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
                &committee,
                self.account_id.clone(),
                CommunicateAction::AdvanceToNextSequenceNumber(self.next_sequence_number),
            )
            .await?;
            if let Ok(new_committee) = self.committee().await {
                if new_committee != committee {
                    // If the configuration just changed, communicate to the new committee as well.
                    // (This is actually more important that updating the previous committee.)
                    self.communicate_account_updates(
                        &new_committee,
                        self.account_id.clone(),
                        CommunicateAction::AdvanceToNextSequenceNumber(self.next_sequence_number),
                    )
                    .await?;
                }
            }
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
    async fn query_safe_balance(&mut self) -> Result<Balance, Error> {
        let committee = self.committee().await?;
        let query = AccountInfoQuery {
            account_id: self.account_id.clone(),
            /// This is necessary to make sure that the response is conservative.
            check_next_sequence_number: Some(self.next_sequence_number),
            query_committee: false,
            query_sent_certificates_in_range: None,
            query_received_certificates_excluding_first_nth: None,
        };
        let infos = self.broadcast_account_info_query(query).await;
        let value = committee.get_validity_lower_bound(
            infos
                .into_iter()
                .map(|(name, info)| (name, info.balance))
                .collect(),
        );
        Ok(value)
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
        self.find_received_certificates().await?;
        self.prepare_account().await?;
        Ok(self.balance().await)
    }

    async fn retry_pending_request(&mut self) -> Result<Option<Certificate>, failure::Error> {
        self.find_received_certificates().await?;
        self.prepare_account().await?;
        match &self.pending_request {
            Some(request) => {
                // Finish executing the previous request.
                let mut request = request.clone();
                request.round = self.next_round;
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
        ensure!(
            request.operation.recipient() == Some(&self.account_id),
            "Request should be received by us."
        );
        // Recover history from the network.
        self.node_client
            .download_certificates(
                self.authority_clients.clone(),
                request.account_id.clone(),
                request.sequence_number,
            )
            .await?;
        // Process the received operation.
        self.process_certificate(certificate).await?;
        // Make sure a quorum of authorities (according to our committee) are up-to-date
        // for data availability.
        let committee = self.committee().await?;
        self.communicate_account_updates(
            &committee,
            request.account_id.clone(),
            CommunicateAction::AdvanceToNextSequenceNumber(request.sequence_number.try_add_one()?),
        )
        .await?;
        Ok(())
    }

    async fn rotate_key_pair(&mut self, key_pair: KeyPair) -> Result<Certificate, failure::Error> {
        self.prepare_account().await?;
        let new_owner = key_pair.public();
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round,
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
        self.prepare_account().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeOwner { new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round,
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
        self.prepare_account().await?;
        let owner = self.identity().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::ChangeMultipleOwners {
                new_owners: vec![owner, new_owner],
            },
            sequence_number: self.next_sequence_number,
            round: self.next_round,
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
        self.prepare_account().await?;
        let new_id = self.account_id.make_child(self.next_sequence_number);
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::OpenAccount { new_id, new_owner },
            sequence_number: self.next_sequence_number,
            round: self.next_round,
        };
        let certificate = self
            .execute_request(request, /* with_confirmation */ true)
            .await?;
        Ok(certificate)
    }

    async fn close_account(&mut self) -> Result<Certificate, failure::Error> {
        self.prepare_account().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::CloseAccount,
            sequence_number: self.next_sequence_number,
            round: self.next_round,
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
        self.prepare_account().await?;
        let request = Request {
            account_id: self.account_id.clone(),
            operation: Operation::Transfer {
                recipient: Address::Account(recipient),
                amount,
                user_data,
            },
            sequence_number: self.next_sequence_number,
            round: self.next_round,
        };
        let new_certificate = self
            .execute_request(request, /* with_confirmation */ false)
            .await?;
        Ok(new_certificate)
    }
}
