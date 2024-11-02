// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Handle requests from the synchronous execution thread of user applications.

use std::fmt::{self, Debug, Formatter};
#[cfg(with_metrics)]
use std::sync::LazyLock;

use futures::channel::mpsc;
#[cfg(with_metrics)]
use linera_base::prometheus_util::{self, MeasureLatency as _};
use linera_base::{
    data_types::{Amount, ApplicationPermissions, BlobContent, Timestamp},
    http,
    identifiers::{Account, BlobId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_views::{batch::Batch, context::Context, views::View};
use oneshot::Sender;
#[cfg(with_metrics)]
use prometheus::HistogramVec;
use reqwest::{header::HeaderMap, Client};

use crate::{
    system::{OpenChainConfig, Recipient},
    util::RespondExt,
    ExecutionError, ExecutionRuntimeContext, ExecutionStateView, RawExecutionOutcome,
    RawOutgoingMessage, SystemExecutionError, SystemMessage, UserApplicationDescription,
    UserApplicationId, UserContractCode, UserServiceCode,
};

#[cfg(with_metrics)]
/// Histogram of the latency to load a contract bytecode.
static LOAD_CONTRACT_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "load_contract_latency",
        "Load contract latency",
        &[],
        Some(vec![
            0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0,
            100.0, 250.0,
        ]),
    )
    .expect("Histogram creation should not fail")
});

#[cfg(with_metrics)]
/// Histogram of the latency to load a service bytecode.
static LOAD_SERVICE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "load_service_latency",
        "Load service latency",
        &[],
        Some(vec![
            0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0,
            100.0, 250.0,
        ]),
    )
    .expect("Histogram creation should not fail")
});

pub(crate) type ExecutionStateSender = mpsc::UnboundedSender<ExecutionRequest>;

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    // TODO(#1416): Support concurrent I/O.
    pub(crate) async fn handle_request(
        &mut self,
        request: ExecutionRequest,
    ) -> Result<(), ExecutionError> {
        use ExecutionRequest::*;
        match request {
            LoadContract { id, callback } => {
                #[cfg(with_metrics)]
                let _latency = LOAD_CONTRACT_LATENCY.measure_latency();
                let description = self.system.registry.describe_application(id).await?;
                let code = self
                    .context()
                    .extra()
                    .get_user_contract(&description)
                    .await?;
                callback.respond((code, description));
            }

            LoadService { id, callback } => {
                #[cfg(with_metrics)]
                let _latency = LOAD_SERVICE_LATENCY.measure_latency();
                let description = self.system.registry.describe_application(id).await?;
                let code = self
                    .context()
                    .extra()
                    .get_user_service(&description)
                    .await?;
                callback.respond((code, description));
            }

            ChainBalance { callback } => {
                let balance = *self.system.balance.get();
                callback.respond(balance);
            }

            OwnerBalance { owner, callback } => {
                let balance = self.system.balances.get(&owner).await?.unwrap_or_default();
                callback.respond(balance);
            }

            OwnerBalances { callback } => {
                let mut balances = Vec::new();
                self.system
                    .balances
                    .for_each_index_value(|owner, balance| {
                        balances.push((owner, balance));
                        Ok(())
                    })
                    .await?;
                callback.respond(balances);
            }

            BalanceOwners { callback } => {
                let owners = self.system.balances.indices().await?;
                callback.respond(owners);
            }

            Transfer {
                source,
                destination,
                amount,
                signer,
                callback,
            } => {
                let mut execution_outcome = RawExecutionOutcome::default();
                let message = self
                    .system
                    .transfer(signer, source, Recipient::Account(destination), amount)
                    .await?;

                if let Some(message) = message {
                    execution_outcome.messages.push(message);
                }
                callback.respond(execution_outcome);
            }

            Claim {
                source,
                destination,
                amount,
                signer,
                callback,
            } => {
                let owner = source.owner.ok_or(ExecutionError::OwnerIsNone)?;
                let mut execution_outcome = RawExecutionOutcome::default();
                let message = self
                    .system
                    .claim(
                        signer,
                        owner,
                        source.chain_id,
                        Recipient::Account(destination),
                        amount,
                    )
                    .await?;

                execution_outcome.messages.push(message);
                callback.respond(execution_outcome);
            }

            SystemTimestamp { callback } => {
                let timestamp = *self.system.timestamp.get();
                callback.respond(timestamp);
            }

            ChainOwnership { callback } => {
                let ownership = self.system.ownership.get().clone();
                callback.respond(ownership);
            }

            ContainsKey { id, key, callback } => {
                let view = self.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.contains_key(&key).await?,
                    None => false,
                };
                callback.respond(result);
            }

            ContainsKeys { id, keys, callback } => {
                let view = self.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.contains_keys(keys).await?,
                    None => vec![false; keys.len()],
                };
                callback.respond(result);
            }

            ReadMultiValuesBytes { id, keys, callback } => {
                let view = self.users.try_load_entry(&id).await?;
                let values = match view {
                    Some(view) => view.multi_get(keys).await?,
                    None => vec![None; keys.len()],
                };
                callback.respond(values);
            }

            ReadValueBytes { id, key, callback } => {
                let view = self.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.get(&key).await?,
                    None => None,
                };
                callback.respond(result);
            }

            FindKeysByPrefix {
                id,
                key_prefix,
                callback,
            } => {
                let view = self.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.find_keys_by_prefix(&key_prefix).await?,
                    None => Vec::new(),
                };
                callback.respond(result);
            }

            FindKeyValuesByPrefix {
                id,
                key_prefix,
                callback,
            } => {
                let view = self.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.find_key_values_by_prefix(&key_prefix).await?,
                    None => Vec::new(),
                };
                callback.respond(result);
            }

            WriteBatch {
                id,
                batch,
                callback,
            } => {
                let mut view = self.users.try_load_entry_mut(&id).await?;
                view.write_batch(batch).await?;
                callback.respond(());
            }

            OpenChain {
                ownership,
                balance,
                next_message_id,
                application_permissions,
                callback,
            } => {
                let inactive_err = || SystemExecutionError::InactiveChain;
                let config = OpenChainConfig {
                    ownership,
                    admin_id: self.system.admin_id.get().ok_or_else(inactive_err)?,
                    epoch: self.system.epoch.get().ok_or_else(inactive_err)?,
                    committees: self.system.committees.get().clone(),
                    balance,
                    application_permissions,
                };
                let messages = self.system.open_chain(config, next_message_id)?;
                callback.respond(messages)
            }

            CloseChain {
                application_id,
                callback,
            } => {
                let app_permissions = self.system.application_permissions.get();
                if !app_permissions.can_close_chain(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    let chain_id = self.context().extra().chain_id();
                    self.system.close_chain(chain_id).await?;
                    callback.respond(Ok(()));
                }
            }

            FetchUrl { url, callback } => {
                let bytes = reqwest::get(url).await?.bytes().await?.to_vec();
                callback.respond(bytes);
            }

            HttpRequest { request, callback } => {
                let headers = request
                    .headers
                    .into_iter()
                    .map(|(name, value)| Ok((name.parse()?, value.try_into()?)))
                    .collect::<Result<HeaderMap, ExecutionError>>()?;

                let response = Client::new()
                    .request(request.method.into(), request.url)
                    .body(request.body)
                    .headers(headers)
                    .send()
                    .await?;
                callback.respond(http::Response::from_reqwest(response).await?);
            }

            ReadBlobContent { blob_id, callback } => {
                let blob = self
                    .system
                    .read_blob_content(blob_id)
                    .await
                    .map_err(|_| SystemExecutionError::BlobNotFoundOnRead(blob_id))?;
                callback.respond(blob);
            }

            AssertBlobExists { blob_id, callback } => {
                self.system.assert_blob_exists(blob_id).await?;
                callback.respond(())
            }
        }

        Ok(())
    }
}

/// Requests to the execution state.
pub enum ExecutionRequest {
    LoadContract {
        id: UserApplicationId,
        callback: Sender<(UserContractCode, UserApplicationDescription)>,
    },

    LoadService {
        id: UserApplicationId,
        callback: Sender<(UserServiceCode, UserApplicationDescription)>,
    },

    ChainBalance {
        callback: Sender<Amount>,
    },

    OwnerBalance {
        owner: Owner,
        callback: Sender<Amount>,
    },

    OwnerBalances {
        callback: Sender<Vec<(Owner, Amount)>>,
    },

    BalanceOwners {
        callback: Sender<Vec<Owner>>,
    },

    Transfer {
        source: Option<Owner>,
        destination: Account,
        amount: Amount,
        signer: Option<Owner>,
        callback: Sender<RawExecutionOutcome<SystemMessage, Amount>>,
    },

    Claim {
        source: Account,
        destination: Account,
        amount: Amount,
        signer: Option<Owner>,
        callback: Sender<RawExecutionOutcome<SystemMessage, Amount>>,
    },

    SystemTimestamp {
        callback: Sender<Timestamp>,
    },

    ChainOwnership {
        callback: Sender<ChainOwnership>,
    },

    ReadValueBytes {
        id: UserApplicationId,
        key: Vec<u8>,
        callback: Sender<Option<Vec<u8>>>,
    },

    ContainsKey {
        id: UserApplicationId,
        key: Vec<u8>,
        callback: Sender<bool>,
    },

    ContainsKeys {
        id: UserApplicationId,
        keys: Vec<Vec<u8>>,
        callback: Sender<Vec<bool>>,
    },

    ReadMultiValuesBytes {
        id: UserApplicationId,
        keys: Vec<Vec<u8>>,
        callback: Sender<Vec<Option<Vec<u8>>>>,
    },

    FindKeysByPrefix {
        id: UserApplicationId,
        key_prefix: Vec<u8>,
        callback: Sender<Vec<Vec<u8>>>,
    },

    FindKeyValuesByPrefix {
        id: UserApplicationId,
        key_prefix: Vec<u8>,
        callback: Sender<Vec<(Vec<u8>, Vec<u8>)>>,
    },

    WriteBatch {
        id: UserApplicationId,
        batch: Batch,
        callback: Sender<()>,
    },

    OpenChain {
        ownership: ChainOwnership,
        balance: Amount,
        next_message_id: MessageId,
        application_permissions: ApplicationPermissions,
        callback: Sender<[RawOutgoingMessage<SystemMessage, Amount>; 2]>,
    },

    CloseChain {
        application_id: UserApplicationId,
        callback: oneshot::Sender<Result<(), ExecutionError>>,
    },

    FetchUrl {
        url: String,
        callback: Sender<Vec<u8>>,
    },

    HttpRequest {
        request: http::Request,
        callback: oneshot::Sender<http::Response>,
    },

    ReadBlobContent {
        blob_id: BlobId,
        callback: Sender<BlobContent>,
    },

    AssertBlobExists {
        blob_id: BlobId,
        callback: Sender<()>,
    },
}

impl Debug for ExecutionRequest {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            ExecutionRequest::LoadContract { id, .. } => formatter
                .debug_struct("ExecutionRequest::LoadContract")
                .field("id", id)
                .finish_non_exhaustive(),

            ExecutionRequest::LoadService { id, .. } => formatter
                .debug_struct("ExecutionRequest::LoadService")
                .field("id", id)
                .finish_non_exhaustive(),

            ExecutionRequest::ChainBalance { .. } => formatter
                .debug_struct("ExecutionRequest::ChainBalance")
                .finish_non_exhaustive(),

            ExecutionRequest::OwnerBalance { owner, .. } => formatter
                .debug_struct("ExecutionRequest::OwnerBalance")
                .field("owner", owner)
                .finish_non_exhaustive(),

            ExecutionRequest::OwnerBalances { .. } => formatter
                .debug_struct("ExecutionRequest::OwnerBalances")
                .finish_non_exhaustive(),

            ExecutionRequest::BalanceOwners { .. } => formatter
                .debug_struct("ExecutionRequest::BalanceOwners")
                .finish_non_exhaustive(),

            ExecutionRequest::Transfer {
                source,
                destination,
                amount,
                signer,
                ..
            } => formatter
                .debug_struct("ExecutionRequest::Transfer")
                .field("source", source)
                .field("destination", destination)
                .field("amount", amount)
                .field("signer", signer)
                .finish_non_exhaustive(),

            ExecutionRequest::Claim {
                source,
                destination,
                amount,
                signer,
                ..
            } => formatter
                .debug_struct("ExecutionRequest::Claim")
                .field("source", source)
                .field("destination", destination)
                .field("amount", amount)
                .field("signer", signer)
                .finish_non_exhaustive(),

            ExecutionRequest::SystemTimestamp { .. } => formatter
                .debug_struct("ExecutionRequest::SystemTimestamp")
                .finish_non_exhaustive(),

            ExecutionRequest::ChainOwnership { .. } => formatter
                .debug_struct("ExecutionRequest::ChainOwnership")
                .finish_non_exhaustive(),

            ExecutionRequest::ReadValueBytes { id, key, .. } => formatter
                .debug_struct("ExecutionRequest::ReadValueBytes")
                .field("id", id)
                .field("key", key)
                .finish_non_exhaustive(),

            ExecutionRequest::ContainsKey { id, key, .. } => formatter
                .debug_struct("ExecutionRequest::ContainsKey")
                .field("id", id)
                .field("key", key)
                .finish_non_exhaustive(),

            ExecutionRequest::ContainsKeys { id, keys, .. } => formatter
                .debug_struct("ExecutionRequest::ContainsKeys")
                .field("id", id)
                .field("keys", keys)
                .finish_non_exhaustive(),

            ExecutionRequest::ReadMultiValuesBytes { id, keys, .. } => formatter
                .debug_struct("ExecutionRequest::ReadMultiValuesBytes")
                .field("id", id)
                .field("keys", keys)
                .finish_non_exhaustive(),

            ExecutionRequest::FindKeysByPrefix { id, key_prefix, .. } => formatter
                .debug_struct("ExecutionRequest::FindKeysByPrefix")
                .field("id", id)
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),

            ExecutionRequest::FindKeyValuesByPrefix { id, key_prefix, .. } => formatter
                .debug_struct("ExecutionRequest::FindKeyValuesByPrefix")
                .field("id", id)
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),

            ExecutionRequest::WriteBatch { id, batch, .. } => formatter
                .debug_struct("ExecutionRequest::WriteBatch")
                .field("id", id)
                .field("batch", batch)
                .finish_non_exhaustive(),

            ExecutionRequest::OpenChain { balance, .. } => formatter
                .debug_struct("ExecutionRequest::OpenChain")
                .field("balance", balance)
                .finish_non_exhaustive(),

            ExecutionRequest::CloseChain { application_id, .. } => formatter
                .debug_struct("ExecutionRequest::CloseChain")
                .field("application_id", application_id)
                .finish_non_exhaustive(),

            ExecutionRequest::FetchUrl { url, .. } => formatter
                .debug_struct("ExecutionRequest::FetchUrl")
                .field("url", url)
                .finish_non_exhaustive(),

            ExecutionRequest::HttpRequest { request, .. } => formatter
                .debug_struct("ExecutionRequest::HttpRequest")
                .field("request", request)
                .finish_non_exhaustive(),

            ExecutionRequest::ReadBlobContent { blob_id, .. } => formatter
                .debug_struct("ExecutionRequest::ReadBlob")
                .field("blob_id", blob_id)
                .finish_non_exhaustive(),

            ExecutionRequest::AssertBlobExists { blob_id, .. } => formatter
                .debug_struct("ExecutionRequest::AssertBlobExists")
                .field("blob_id", blob_id)
                .finish_non_exhaustive(),
        }
    }
}
