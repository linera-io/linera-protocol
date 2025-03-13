// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Handle requests from the synchronous execution thread of user applications.

#[cfg(with_metrics)]
use std::sync::LazyLock;
#[cfg(not(web))]
use std::time::Duration;

use custom_debug_derive::Debug;
use futures::{channel::mpsc, StreamExt as _};
#[cfg(with_metrics)]
use linera_base::prometheus_util::{
    exponential_bucket_latencies, register_histogram_vec, MeasureLatency as _,
};
use linera_base::{
    data_types::{Amount, ApplicationPermissions, BlobContent, BlockHeight, Timestamp},
    ensure, hex_debug, hex_vec_debug, http,
    identifiers::{Account, AccountOwner, BlobId, ChainId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_views::{batch::Batch, context::Context, views::View};
use oneshot::Sender;
#[cfg(with_metrics)]
use prometheus::HistogramVec;
use reqwest::{header::HeaderMap, Client, Url};

use crate::{
    system::{CreateApplicationResult, OpenChainConfig, Recipient},
    util::RespondExt,
    ExecutionError, ExecutionRuntimeContext, ExecutionStateView, ModuleId, RawExecutionOutcome,
    RawOutgoingMessage, SystemMessage, TransactionTracker, UserApplicationDescription,
    UserApplicationId, UserContractCode, UserServiceCode,
};

#[cfg(with_metrics)]
/// Histogram of the latency to load a contract bytecode.
static LOAD_CONTRACT_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "load_contract_latency",
        "Load contract latency",
        &[],
        exponential_bucket_latencies(250.0),
    )
});

#[cfg(with_metrics)]
/// Histogram of the latency to load a service bytecode.
static LOAD_SERVICE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "load_service_latency",
        "Load service latency",
        &[],
        exponential_bucket_latencies(250.0),
    )
});

pub(crate) type ExecutionStateSender = mpsc::UnboundedSender<ExecutionRequest>;

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    pub(crate) async fn load_contract(
        &mut self,
        id: UserApplicationId,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<(UserContractCode, UserApplicationDescription), ExecutionError> {
        #[cfg(with_metrics)]
        let _latency = LOAD_CONTRACT_LATENCY.measure_latency();
        let blob_id = id.description_blob_id();
        let description = match txn_tracker.created_blobs().get(&blob_id) {
            Some(description) => {
                let blob = description.clone();
                bcs::from_bytes(blob.bytes())?
            }
            None => {
                self.system
                    .describe_application(id, Some(txn_tracker), None)
                    .await?
            }
        };
        let code = self
            .context()
            .extra()
            .get_user_contract(&description)
            .await?;
        Ok((code, description))
    }

    pub(crate) async fn load_service(
        &mut self,
        id: UserApplicationId,
        txn_tracker: Option<&mut TransactionTracker>,
    ) -> Result<(UserServiceCode, UserApplicationDescription), ExecutionError> {
        #[cfg(with_metrics)]
        let _latency = LOAD_SERVICE_LATENCY.measure_latency();
        let blob_id = id.description_blob_id();
        let description = match txn_tracker
            .as_ref()
            .and_then(|tracker| tracker.created_blobs().get(&blob_id))
        {
            Some(description) => {
                let blob = description.clone();
                bcs::from_bytes(blob.bytes())?
            }
            None => {
                self.system
                    .describe_application(id, txn_tracker, None)
                    .await?
            }
        };
        let code = self
            .context()
            .extra()
            .get_user_service(&description)
            .await?;
        Ok((code, description))
    }

    // TODO(#1416): Support concurrent I/O.
    pub(crate) async fn handle_request(
        &mut self,
        request: ExecutionRequest,
    ) -> Result<(), ExecutionError> {
        use ExecutionRequest::*;
        match request {
            #[cfg(not(web))]
            LoadContract {
                id,
                callback,
                mut txn_tracker,
            } => {
                let (code, description) = self.load_contract(id, &mut txn_tracker).await?;
                callback.respond((code, description, txn_tracker))
            }
            #[cfg(not(web))]
            LoadService {
                id,
                callback,
                mut txn_tracker,
            } => {
                let (code, description) = self.load_service(id, Some(&mut txn_tracker)).await?;
                callback.respond((code, description, txn_tracker))
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
                let balances = self.system.balances.index_values().await?;
                callback.respond(balances.into_iter().collect());
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
                application_id,
                callback,
            } => {
                let mut execution_outcome = RawExecutionOutcome::default();
                let message = self
                    .system
                    .transfer(
                        signer,
                        Some(application_id),
                        source,
                        Recipient::Account(destination),
                        amount,
                    )
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
                application_id,
                callback,
            } => {
                let owner = source.owner.ok_or(ExecutionError::OwnerIsNone)?;
                let mut execution_outcome = RawExecutionOutcome::default();
                let message = self
                    .system
                    .claim(
                        signer,
                        Some(application_id),
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
                let inactive_err = || ExecutionError::InactiveChain;
                let config = OpenChainConfig {
                    ownership,
                    admin_id: self.system.admin_id.get().ok_or_else(inactive_err)?,
                    epoch: self.system.epoch.get().ok_or_else(inactive_err)?,
                    committees: self.system.committees.get().clone(),
                    balance,
                    application_permissions,
                };
                let messages = self.system.open_chain(config, next_message_id).await?;
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

            ChangeApplicationPermissions {
                application_id,
                application_permissions,
                callback,
            } => {
                let app_permissions = self.system.application_permissions.get();
                if !app_permissions.can_change_application_permissions(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.system
                        .application_permissions
                        .set(application_permissions);
                    callback.respond(Ok(()));
                }
            }

            CreateApplication {
                chain_id,
                block_height,
                module_id,
                parameters,
                required_application_ids,
                callback,
                txn_tracker,
            } => {
                let create_application_result = self
                    .system
                    .create_application(
                        chain_id,
                        block_height,
                        module_id,
                        parameters,
                        required_application_ids,
                        txn_tracker,
                        None,
                    )
                    .await?;
                callback.respond(Ok(create_application_result));
            }

            PerformHttpRequest { request, callback } => {
                let headers = request
                    .headers
                    .into_iter()
                    .map(|http::Header { name, value }| Ok((name.parse()?, value.try_into()?)))
                    .collect::<Result<HeaderMap, ExecutionError>>()?;

                let url = Url::parse(&request.url)?;
                let host = url
                    .host_str()
                    .ok_or_else(|| ExecutionError::UnauthorizedHttpRequest(url.clone()))?;

                let (_epoch, committee) = self
                    .system
                    .current_committee()
                    .ok_or_else(|| ExecutionError::UnauthorizedHttpRequest(url.clone()))?;
                let allowed_hosts = &committee.policy().http_request_allow_list;

                ensure!(
                    allowed_hosts.contains(host),
                    ExecutionError::UnauthorizedHttpRequest(url)
                );

                #[cfg_attr(web, allow(unused_mut))]
                let mut request = Client::new()
                    .request(request.method.into(), url)
                    .body(request.body)
                    .headers(headers);
                #[cfg(not(web))]
                {
                    request = request.timeout(Duration::from_millis(
                        committee.policy().http_request_timeout_ms,
                    ));
                }

                let response = request.send().await?;

                let response_size_limit = committee.policy().maximum_http_response_bytes;

                callback.respond(
                    self.receive_http_response(response, response_size_limit)
                        .await?,
                );
            }

            ReadBlobContent { blob_id, callback } => {
                let blob = self.system.read_blob_content(blob_id).await?;
                let is_new = self
                    .system
                    .blob_used(None, None, blob_id, blob.bytes().len())
                    .await?;
                callback.respond((blob, is_new))
            }

            AssertBlobExists { blob_id, callback } => {
                self.system.assert_blob_exists(blob_id).await?;
                callback.respond(self.system.blob_used(None, None, blob_id, 0).await?)
            }

            GetApplicationPermissions { callback } => {
                let app_permissions = self.system.application_permissions.get();
                callback.respond(app_permissions.clone());
            }
        }

        Ok(())
    }
}

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Receives an HTTP response, returning the prepared [`http::Response`] instance.
    ///
    /// Ensures that the response does not exceed the provided `size_limit`.
    async fn receive_http_response(
        &mut self,
        response: reqwest::Response,
        size_limit: u64,
    ) -> Result<http::Response, ExecutionError> {
        let status = response.status().as_u16();
        let maybe_content_length = response.content_length();

        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| http::Header::new(name.to_string(), value.as_bytes()))
            .collect::<Vec<_>>();

        let total_header_size = headers
            .iter()
            .map(|header| (header.name.as_bytes().len() + header.value.len()) as u64)
            .sum();

        let mut remaining_bytes = size_limit.checked_sub(total_header_size).ok_or(
            ExecutionError::HttpResponseSizeLimitExceeded {
                limit: size_limit,
                size: total_header_size,
            },
        )?;

        if let Some(content_length) = maybe_content_length {
            if content_length > remaining_bytes {
                return Err(ExecutionError::HttpResponseSizeLimitExceeded {
                    limit: size_limit,
                    size: content_length + total_header_size,
                });
            }
        }

        let mut body = Vec::with_capacity(maybe_content_length.unwrap_or(0) as usize);
        let mut body_stream = response.bytes_stream();

        while let Some(bytes) = body_stream.next().await.transpose()? {
            remaining_bytes = remaining_bytes.checked_sub(bytes.len() as u64).ok_or(
                ExecutionError::HttpResponseSizeLimitExceeded {
                    limit: size_limit,
                    size: bytes.len() as u64 + (size_limit - remaining_bytes),
                },
            )?;

            body.extend(&bytes);
        }

        Ok(http::Response {
            status,
            headers,
            body,
        })
    }
}

/// Requests to the execution state.
#[derive(Debug)]
pub enum ExecutionRequest {
    #[cfg(not(web))]
    LoadContract {
        id: UserApplicationId,
        #[debug(skip)]
        callback: Sender<(
            UserContractCode,
            UserApplicationDescription,
            TransactionTracker,
        )>,
        #[debug(skip)]
        txn_tracker: TransactionTracker,
    },

    #[cfg(not(web))]
    LoadService {
        id: UserApplicationId,
        #[debug(skip)]
        callback: Sender<(
            UserServiceCode,
            UserApplicationDescription,
            TransactionTracker,
        )>,
        #[debug(skip)]
        txn_tracker: TransactionTracker,
    },

    ChainBalance {
        #[debug(skip)]
        callback: Sender<Amount>,
    },

    OwnerBalance {
        owner: AccountOwner,
        #[debug(skip)]
        callback: Sender<Amount>,
    },

    OwnerBalances {
        #[debug(skip)]
        callback: Sender<Vec<(AccountOwner, Amount)>>,
    },

    BalanceOwners {
        #[debug(skip)]
        callback: Sender<Vec<AccountOwner>>,
    },

    Transfer {
        #[debug(skip_if = Option::is_none)]
        source: Option<AccountOwner>,
        destination: Account,
        amount: Amount,
        #[debug(skip_if = Option::is_none)]
        signer: Option<Owner>,
        application_id: UserApplicationId,
        #[debug(skip)]
        callback: Sender<RawExecutionOutcome<SystemMessage>>,
    },

    Claim {
        source: Account,
        destination: Account,
        amount: Amount,
        #[debug(skip_if = Option::is_none)]
        signer: Option<Owner>,
        application_id: UserApplicationId,
        #[debug(skip)]
        callback: Sender<RawExecutionOutcome<SystemMessage>>,
    },

    SystemTimestamp {
        #[debug(skip)]
        callback: Sender<Timestamp>,
    },

    ChainOwnership {
        #[debug(skip)]
        callback: Sender<ChainOwnership>,
    },

    ReadValueBytes {
        id: UserApplicationId,
        #[debug(with = hex_debug)]
        key: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Option<Vec<u8>>>,
    },

    ContainsKey {
        id: UserApplicationId,
        key: Vec<u8>,
        #[debug(skip)]
        callback: Sender<bool>,
    },

    ContainsKeys {
        id: UserApplicationId,
        #[debug(with = hex_vec_debug)]
        keys: Vec<Vec<u8>>,
        callback: Sender<Vec<bool>>,
    },

    ReadMultiValuesBytes {
        id: UserApplicationId,
        #[debug(with = hex_vec_debug)]
        keys: Vec<Vec<u8>>,
        #[debug(skip)]
        callback: Sender<Vec<Option<Vec<u8>>>>,
    },

    FindKeysByPrefix {
        id: UserApplicationId,
        #[debug(with = hex_debug)]
        key_prefix: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Vec<Vec<u8>>>,
    },

    FindKeyValuesByPrefix {
        id: UserApplicationId,
        #[debug(with = hex_debug)]
        key_prefix: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Vec<(Vec<u8>, Vec<u8>)>>,
    },

    WriteBatch {
        id: UserApplicationId,
        batch: Batch,
        #[debug(skip)]
        callback: Sender<()>,
    },

    OpenChain {
        ownership: ChainOwnership,
        #[debug(skip_if = Amount::is_zero)]
        balance: Amount,
        next_message_id: MessageId,
        application_permissions: ApplicationPermissions,
        #[debug(skip)]
        callback: Sender<RawOutgoingMessage<SystemMessage, Amount>>,
    },

    CloseChain {
        application_id: UserApplicationId,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    ChangeApplicationPermissions {
        application_id: UserApplicationId,
        application_permissions: ApplicationPermissions,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    CreateApplication {
        chain_id: ChainId,
        block_height: BlockHeight,
        module_id: ModuleId,
        parameters: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
        #[debug(skip)]
        txn_tracker: TransactionTracker,
        #[debug(skip)]
        callback: Sender<Result<CreateApplicationResult, ExecutionError>>,
    },

    PerformHttpRequest {
        request: http::Request,
        #[debug(skip)]
        callback: Sender<http::Response>,
    },

    ReadBlobContent {
        blob_id: BlobId,
        #[debug(skip)]
        callback: Sender<(BlobContent, bool)>,
    },

    AssertBlobExists {
        blob_id: BlobId,
        #[debug(skip)]
        callback: Sender<bool>,
    },

    GetApplicationPermissions {
        #[debug(skip)]
        callback: Sender<ApplicationPermissions>,
    },
}
