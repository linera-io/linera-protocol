// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::node::AuthorityClient;
use futures::{future, StreamExt};
use std::{collections::HashMap, time::Duration};
use zef_base::{
    account::AccountState, base_types::*, committee::Committee, error::Error, messages::*,
};
use zef_storage::StorageClient;

/// Used for `communicate_account_updates`
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum CommunicateAction {
    SubmitRequestForConfirmation(RequestOrder),
    SubmitRequestForValidation(RequestOrder),
    FinalizeRequest(Certificate),
    AdvanceToNextSequenceNumber(SequenceNumber),
}

pub struct AuthorityUpdater<A, S> {
    pub name: AuthorityName,
    pub client: A,
    pub store: S,
    pub delay: Duration,
    pub retries: usize,
}

/// Execute a sequence of actions in parallel for all authorities.
/// Try to stop early when a quorum is reached.
pub async fn communicate_with_quorum<'a, A, V, F>(
    authority_clients: &'a [(AuthorityName, A)],
    committee: &Committee,
    execute: F,
) -> Result<Vec<V>, Option<Error>>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
    F: Fn(AuthorityName, A) -> future::BoxFuture<'a, Result<V, Error>> + Clone,
{
    let mut responses: futures::stream::FuturesUnordered<_> = authority_clients
        .iter()
        .filter_map(|(name, client)| {
            let client = client.clone();
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

impl<A, S> AuthorityUpdater<A, S>
where
    A: AuthorityClient + Send + Sync + 'static + Clone,
    S: StorageClient + Clone + 'static,
{
    pub async fn send_certificate(
        &mut self,
        certificate: Certificate,
        retryable: bool,
    ) -> Result<AccountInfo, Error> {
        let mut count = 0;
        loop {
            match self.client.handle_certificate(certificate.clone()).await {
                Ok(response) => {
                    response.check(self.name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveAccount(_)) if retryable && count < self.retries => {
                    // Retry
                    tokio::time::sleep(self.delay).await;
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

    pub async fn send_request_order(&mut self, order: RequestOrder) -> Result<AccountInfo, Error> {
        let mut count = 0;
        loop {
            match self.client.handle_request_order(order.clone()).await {
                Ok(response) => {
                    response.check(self.name)?;
                    // Succeed
                    return Ok(response.info);
                }
                Err(Error::InactiveAccount(_)) if count < self.retries => {
                    // Retry
                    tokio::time::sleep(self.delay).await;
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

    pub async fn send_account_information(
        &mut self,
        mut account_id: AccountId,
        mut target_sequence_number: SequenceNumber,
    ) -> Result<(), Error> {
        let mut jobs = Vec::new();
        loop {
            // Figure out which certificates this authority is missing.
            let query = AccountInfoQuery {
                account_id: account_id.clone(),
                check_next_sequence_number: None,
                query_committee: false,
                query_sent_certificates_in_range: None,
                query_received_certificates_excluding_first_nth: None,
            };
            match self.client.handle_account_info_query(query).await {
                Ok(response) if response.info.manager.is_active() => {
                    response.check(self.name)?;
                    jobs.push((
                        account_id,
                        response.info.next_sequence_number,
                        target_sequence_number,
                        false,
                    ));
                    break;
                }
                Ok(response) => {
                    response.check(self.name)?;
                    match account_id.split() {
                        None => return Err(Error::InactiveAccount(account_id)),
                        Some((parent_id, number)) => {
                            jobs.push((
                                account_id,
                                SequenceNumber::from(0),
                                target_sequence_number,
                                true,
                            ));
                            account_id = parent_id;
                            target_sequence_number = number.try_add_one()?;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        for (account_id, initial_sequence_number, target_sequence_number, retryable) in
            jobs.into_iter().rev()
        {
            // Obtain account state.
            let account = self.store.read_account_or_default(&account_id).await?;
            // Send the requested certificates in order.
            for number in usize::from(initial_sequence_number)..usize::from(target_sequence_number)
            {
                let key = account
                    .confirmed_log
                    .get(number)
                    .expect("certificate should be known locally");
                let cert = self.store.read_certificate(*key).await?;
                self.send_certificate(cert, retryable).await?;
            }
        }
        Ok(())
    }

    pub async fn send_account_information_as_a_receiver(
        &mut self,
        account_id: AccountId,
    ) -> Result<(), Error> {
        // Obtain account state.
        let account = self.store.read_account_or_default(&account_id).await?;
        for (sender_id, sequence_number) in account.received_index.iter() {
            self.send_account_information(sender_id.clone(), sequence_number.try_add_one()?)
                .await?;
        }
        Ok(())
    }

    pub async fn send_account_update(
        &mut self,
        account_id: AccountId,
        action: CommunicateAction,
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
        self.send_account_information(account_id.clone(), target_sequence_number)
            .await?;
        // Send the request order (if any) and return a vote.
        match action {
            CommunicateAction::SubmitRequestForValidation(order)
            | CommunicateAction::SubmitRequestForConfirmation(order) => {
                let result = self.send_request_order(order.clone()).await;
                let info = match result {
                    Ok(info) => info,
                    Err(e) if AccountState::is_retriable_validation_error(&order.request, &e) => {
                        // Some received certificates may be missing for this authority
                        // (e.g. to make the balance sufficient) so we are going to
                        // synchronize them now.
                        self.send_account_information_as_a_receiver(account_id)
                            .await?;
                        // Now retry the request.
                        self.send_request_order(order).await?
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(self.name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingRequestOrder),
                }
            }
            CommunicateAction::FinalizeRequest(certificate) => {
                // The only cause for a retry is that the first certificate of a newly opened account.
                let retryable = target_sequence_number == SequenceNumber::from(0);
                let info = self.send_certificate(certificate, retryable).await?;
                match info.manager.pending() {
                    Some(vote) => {
                        vote.check(self.name)?;
                        return Ok(Some(vote.clone()));
                    }
                    None => return Err(Error::ClientErrorWhileProcessingRequestOrder),
                }
            }
            CommunicateAction::AdvanceToNextSequenceNumber(_) => (),
        }
        Ok(None)
    }
}
