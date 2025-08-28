// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Handle requests from the synchronous execution thread of user applications.

use custom_debug_derive::Debug;
use futures::{channel::mpsc, StreamExt as _};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlobContent, BlockHeight, OracleResponse,
        Timestamp,
    },
    ensure, hex_debug, hex_vec_debug, http,
    identifiers::{Account, AccountOwner, BlobId, BlobType, ChainId, EventId, StreamId},
    ownership::ChainOwnership,
};
use linera_views::{batch::Batch, context::Context, views::View};
use oneshot::Sender;
use reqwest::{header::HeaderMap, Client, Url};

use crate::{
    system::{CreateApplicationResult, OpenChainConfig},
    util::RespondExt,
    ApplicationDescription, ApplicationId, ExecutionError, ExecutionRuntimeContext,
    ExecutionStateView, ModuleId, ResourceController, TransactionTracker, UserContractCode,
    UserServiceCode,
};

/// Actor for handling requests to the execution state.
pub struct ExecutionStateActor<'a, C> {
    /// The execution state view being operated on.
    state: &'a mut ExecutionStateView<C>,
    /// The transaction tracker.
    txn_tracker: &'a mut TransactionTracker,
}

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// Histogram of the latency to load a contract bytecode.
    pub static LOAD_CONTRACT_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "load_contract_latency",
            "Load contract latency",
            &[],
            exponential_bucket_latencies(250.0),
        )
    });

    /// Histogram of the latency to load a service bytecode.
    pub static LOAD_SERVICE_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "load_service_latency",
            "Load service latency",
            &[],
            exponential_bucket_latencies(250.0),
        )
    });
}

pub(crate) type ExecutionStateSender = mpsc::UnboundedSender<ExecutionRequest>;

impl<'a, C> ExecutionStateActor<'a, C>
where
    C: Context + Clone + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Creates a new execution state actor.
    pub(crate) fn new(
        state: &'a mut ExecutionStateView<C>,
        txn_tracker: &'a mut TransactionTracker,
    ) -> Self {
        Self { state, txn_tracker }
    }

    pub(crate) async fn load_contract(
        &mut self,
        id: ApplicationId,
    ) -> Result<(UserContractCode, ApplicationDescription), ExecutionError> {
        #[cfg(with_metrics)]
        let _latency = metrics::LOAD_CONTRACT_LATENCY.measure_latency();
        let blob_id = id.description_blob_id();
        let description = match self.txn_tracker.get_blob_content(&blob_id) {
            Some(blob) => bcs::from_bytes(blob.bytes())?,
            None => {
                self.state
                    .system
                    .describe_application(id, self.txn_tracker)
                    .await?
            }
        };
        let code = self
            .state
            .context()
            .extra()
            .get_user_contract(&description, self.txn_tracker)
            .await?;
        Ok((code, description))
    }

    pub(crate) async fn load_service(
        &mut self,
        id: ApplicationId,
    ) -> Result<(UserServiceCode, ApplicationDescription), ExecutionError> {
        #[cfg(with_metrics)]
        let _latency = metrics::LOAD_SERVICE_LATENCY.measure_latency();
        let blob_id = id.description_blob_id();
        let description = match self.txn_tracker.get_blob_content(&blob_id) {
            Some(blob) => bcs::from_bytes(blob.bytes())?,
            None => {
                self.state
                    .system
                    .describe_application(id, self.txn_tracker)
                    .await?
            }
        };
        let code = self
            .state
            .context()
            .extra()
            .get_user_service(&description, self.txn_tracker)
            .await?;
        Ok((code, description))
    }

    // TODO(#1416): Support concurrent I/O.
    pub(crate) async fn handle_request(
        &mut self,
        request: ExecutionRequest,
        resource_controller: &mut ResourceController<Option<AccountOwner>>,
    ) -> Result<(), ExecutionError> {
        use ExecutionRequest::*;
        match request {
            #[cfg(not(web))]
            LoadContract { id, callback } => {
                let (code, description) = self.load_contract(id).await?;
                callback.respond((code, description))
            }
            #[cfg(not(web))]
            LoadService { id, callback } => {
                let (code, description) = self.load_service(id).await?;
                callback.respond((code, description))
            }

            ChainBalance { callback } => {
                let balance = *self.state.system.balance.get();
                callback.respond(balance);
            }

            OwnerBalance { owner, callback } => {
                let balance = self
                    .state
                    .system
                    .balances
                    .get(&owner)
                    .await?
                    .unwrap_or_default();
                callback.respond(balance);
            }

            OwnerBalances { callback } => {
                let balances = self.state.system.balances.index_values().await?;
                callback.respond(balances.into_iter().collect());
            }

            BalanceOwners { callback } => {
                let owners = self.state.system.balances.indices().await?;
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
                let maybe_message = self
                    .state
                    .system
                    .transfer(signer, Some(application_id), source, destination, amount)
                    .await?;
                self.txn_tracker.add_outgoing_messages(maybe_message);
                callback.respond(());
            }

            Claim {
                source,
                destination,
                amount,
                signer,
                application_id,
                callback,
            } => {
                let maybe_message = self
                    .state
                    .system
                    .claim(
                        signer,
                        Some(application_id),
                        source.owner,
                        source.chain_id,
                        destination,
                        amount,
                    )
                    .await?;
                self.txn_tracker.add_outgoing_messages(maybe_message);
                callback.respond(());
            }

            SystemTimestamp { callback } => {
                let timestamp = *self.state.system.timestamp.get();
                callback.respond(timestamp);
            }

            ChainOwnership { callback } => {
                let ownership = self.state.system.ownership.get().clone();
                callback.respond(ownership);
            }

            ContainsKey { id, key, callback } => {
                let view = self.state.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.contains_key(&key).await?,
                    None => false,
                };
                callback.respond(result);
            }

            ContainsKeys { id, keys, callback } => {
                let view = self.state.users.try_load_entry(&id).await?;
                let result = match view {
                    Some(view) => view.contains_keys(keys).await?,
                    None => vec![false; keys.len()],
                };
                callback.respond(result);
            }

            ReadMultiValuesBytes { id, keys, callback } => {
                let view = self.state.users.try_load_entry(&id).await?;
                let values = match view {
                    Some(view) => view.multi_get(keys).await?,
                    None => vec![None; keys.len()],
                };
                callback.respond(values);
            }

            ReadValueBytes { id, key, callback } => {
                let view = self.state.users.try_load_entry(&id).await?;
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
                let view = self.state.users.try_load_entry(&id).await?;
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
                let view = self.state.users.try_load_entry(&id).await?;
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
                let mut view = self.state.users.try_load_entry_mut(&id).await?;
                view.write_batch(batch).await?;
                callback.respond(());
            }

            OpenChain {
                ownership,
                balance,
                parent_id,
                block_height,
                application_permissions,
                timestamp,
                callback,
            } => {
                let config = OpenChainConfig {
                    ownership,
                    balance,
                    application_permissions,
                };
                let chain_id = self
                    .state
                    .system
                    .open_chain(config, parent_id, block_height, timestamp, self.txn_tracker)
                    .await?;
                callback.respond(chain_id);
            }

            CloseChain {
                application_id,
                callback,
            } => {
                let app_permissions = self.state.system.application_permissions.get();
                if !app_permissions.can_close_chain(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.state.system.close_chain().await?;
                    callback.respond(Ok(()));
                }
            }

            ChangeApplicationPermissions {
                application_id,
                application_permissions,
                callback,
            } => {
                let app_permissions = self.state.system.application_permissions.get();
                if !app_permissions.can_change_application_permissions(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.state
                        .system
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
            } => {
                let create_application_result = self
                    .state
                    .system
                    .create_application(
                        chain_id,
                        block_height,
                        module_id,
                        parameters,
                        required_application_ids,
                        self.txn_tracker,
                    )
                    .await?;
                callback.respond(Ok(create_application_result));
            }

            PerformHttpRequest {
                request,
                http_responses_are_oracle_responses,
                callback,
            } => {
                let response = if let Some(response) =
                    self.txn_tracker.next_replayed_oracle_response()?
                {
                    match response {
                        OracleResponse::Http(response) => response,
                        _ => return Err(ExecutionError::OracleResponseMismatch),
                    }
                } else {
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
                        .state
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
                        request = request.timeout(linera_base::time::Duration::from_millis(
                            committee.policy().http_request_timeout_ms,
                        ));
                    }

                    let response = request.send().await?;

                    let mut response_size_limit = committee.policy().maximum_http_response_bytes;

                    if http_responses_are_oracle_responses {
                        response_size_limit = response_size_limit
                            .min(committee.policy().maximum_oracle_response_bytes);
                    }

                    self.receive_http_response(response, response_size_limit)
                        .await?
                };

                // Record the oracle response
                self.txn_tracker
                    .add_oracle_response(OracleResponse::Http(response.clone()));

                callback.respond(response);
            }

            ReadBlobContent { blob_id, callback } => {
                let content = if let Some(content) = self.txn_tracker.get_blob_content(&blob_id) {
                    content.clone()
                } else {
                    let content = self.state.system.read_blob_content(blob_id).await?;
                    if blob_id.blob_type == BlobType::Data {
                        resource_controller
                            .with_state(&mut self.state.system)
                            .await?
                            .track_blob_read(content.bytes().len() as u64)?;
                    }
                    content
                };
                let is_new = self
                    .state
                    .system
                    .blob_used(self.txn_tracker, blob_id)
                    .await?;
                if is_new {
                    self.txn_tracker
                        .replay_oracle_response(OracleResponse::Blob(blob_id))?;
                }
                callback.respond(content)
            }

            AssertBlobExists { blob_id, callback } => {
                self.state.system.assert_blob_exists(blob_id).await?;
                // Treating this as reading a size-0 blob for fee purposes.
                if blob_id.blob_type == BlobType::Data {
                    resource_controller
                        .with_state(&mut self.state.system)
                        .await?
                        .track_blob_read(0)?;
                }
                let is_new = self
                    .state
                    .system
                    .blob_used(self.txn_tracker, blob_id)
                    .await?;
                if is_new {
                    self.txn_tracker
                        .replay_oracle_response(OracleResponse::Blob(blob_id))?;
                }
                callback.respond(());
            }

            Emit {
                stream_id,
                value,
                callback,
            } => {
                let count = self
                    .state
                    .stream_event_counts
                    .get_mut_or_default(&stream_id)
                    .await?;
                let index = *count;
                *count = count.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                self.txn_tracker.add_event(stream_id, index, value);
                callback.respond(index)
            }

            ReadEvent { event_id, callback } => {
                let event = match self.txn_tracker.next_replayed_oracle_response()? {
                    None => {
                        let event = self
                            .state
                            .context()
                            .extra()
                            .get_event(event_id.clone())
                            .await?;
                        event.ok_or(ExecutionError::EventsNotFound(vec![event_id.clone()]))?
                    }
                    Some(OracleResponse::Event(recorded_event_id, event))
                        if recorded_event_id == event_id =>
                    {
                        event
                    }
                    Some(_) => return Err(ExecutionError::OracleResponseMismatch),
                };
                self.txn_tracker
                    .add_oracle_response(OracleResponse::Event(event_id, event.clone()));
                callback.respond(event);
            }

            SubscribeToEvents {
                chain_id,
                stream_id,
                subscriber_app_id,
                callback,
            } => {
                let subscriptions = self
                    .state
                    .system
                    .event_subscriptions
                    .get_mut_or_default(&(chain_id, stream_id.clone()))
                    .await?;
                let next_index = if subscriptions.applications.insert(subscriber_app_id) {
                    subscriptions.next_index
                } else {
                    0
                };
                self.txn_tracker.add_stream_to_process(
                    subscriber_app_id,
                    chain_id,
                    stream_id,
                    0,
                    next_index,
                );
                callback.respond(());
            }

            UnsubscribeFromEvents {
                chain_id,
                stream_id,
                subscriber_app_id,
                callback,
            } => {
                let key = (chain_id, stream_id.clone());
                let subscriptions = self
                    .state
                    .system
                    .event_subscriptions
                    .get_mut_or_default(&key)
                    .await?;
                subscriptions.applications.remove(&subscriber_app_id);
                if subscriptions.applications.is_empty() {
                    self.state.system.event_subscriptions.remove(&key)?;
                }
                if let crate::GenericApplicationId::User(app_id) = stream_id.application_id {
                    self.txn_tracker
                        .remove_stream_to_process(app_id, chain_id, stream_id);
                }
                callback.respond(());
            }

            GetApplicationPermissions { callback } => {
                let app_permissions = self.state.system.application_permissions.get();
                callback.respond(app_permissions.clone());
            }

            QueryServiceOracle { callback } => {
                let response = match self.txn_tracker.next_replayed_oracle_response()? {
                    Some(OracleResponse::Service(bytes)) => Some(bytes),
                    Some(_) => return Err(ExecutionError::OracleResponseMismatch),
                    None => None,
                };
                callback.respond(response);
            }

            QueryService { response, callback } => {
                self.txn_tracker
                    .add_oracle_response(OracleResponse::Service(response));
                callback.respond(());
            }

            AddOutgoingMessage { message, callback } => {
                self.txn_tracker.add_outgoing_message(message);
                callback.respond(());
            }

            SetLocalTime {
                local_time,
                callback,
            } => {
                self.txn_tracker.set_local_time(local_time);
                callback.respond(());
            }

            GetLocalTime { callback } => {
                let local_time = self.txn_tracker.local_time();
                callback.respond(local_time);
            }

            GetCreatedBlobs { callback } => {
                let blobs = self.txn_tracker.created_blobs().clone();
                callback.respond(blobs);
            }

            AssertBefore {
                timestamp,
                callback,
            } => {
                let result = if !self
                    .txn_tracker
                    .replay_oracle_response(OracleResponse::Assert)?
                {
                    // There are no recorded oracle responses, so we check the local time.
                    let local_time = self.txn_tracker.local_time();
                    if local_time >= timestamp {
                        Err(ExecutionError::AssertBefore {
                            timestamp,
                            local_time,
                        })
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                };
                callback.respond(result);
            }

            AddCreatedBlob { blob, callback } => {
                self.txn_tracker.add_created_blob(blob);
                callback.respond(());
            }

            ValidationRound { round, callback } => {
                let result_round =
                    if let Some(response) = self.txn_tracker.next_replayed_oracle_response()? {
                        match response {
                            OracleResponse::Round(round) => round,
                            _ => return Err(ExecutionError::OracleResponseMismatch),
                        }
                    } else {
                        round
                    };
                self.txn_tracker
                    .add_oracle_response(OracleResponse::Round(result_round));
                callback.respond(result_round);
            }

            AddOracleResponse { response, callback } => {
                self.txn_tracker.add_oracle_response(response);
                callback.respond(());
            }
        }

        Ok(())
    }

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
            .map(|header| (header.name.len() + header.value.len()) as u64)
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
        id: ApplicationId,
        #[debug(skip)]
        callback: Sender<(UserContractCode, ApplicationDescription)>,
    },

    #[cfg(not(web))]
    LoadService {
        id: ApplicationId,
        #[debug(skip)]
        callback: Sender<(UserServiceCode, ApplicationDescription)>,
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
        source: AccountOwner,
        destination: Account,
        amount: Amount,
        #[debug(skip_if = Option::is_none)]
        signer: Option<AccountOwner>,
        application_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<()>,
    },

    Claim {
        source: Account,
        destination: Account,
        amount: Amount,
        #[debug(skip_if = Option::is_none)]
        signer: Option<AccountOwner>,
        application_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<()>,
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
        id: ApplicationId,
        #[debug(with = hex_debug)]
        key: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Option<Vec<u8>>>,
    },

    ContainsKey {
        id: ApplicationId,
        key: Vec<u8>,
        #[debug(skip)]
        callback: Sender<bool>,
    },

    ContainsKeys {
        id: ApplicationId,
        #[debug(with = hex_vec_debug)]
        keys: Vec<Vec<u8>>,
        callback: Sender<Vec<bool>>,
    },

    ReadMultiValuesBytes {
        id: ApplicationId,
        #[debug(with = hex_vec_debug)]
        keys: Vec<Vec<u8>>,
        #[debug(skip)]
        callback: Sender<Vec<Option<Vec<u8>>>>,
    },

    FindKeysByPrefix {
        id: ApplicationId,
        #[debug(with = hex_debug)]
        key_prefix: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Vec<Vec<u8>>>,
    },

    FindKeyValuesByPrefix {
        id: ApplicationId,
        #[debug(with = hex_debug)]
        key_prefix: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Vec<(Vec<u8>, Vec<u8>)>>,
    },

    WriteBatch {
        id: ApplicationId,
        batch: Batch,
        #[debug(skip)]
        callback: Sender<()>,
    },

    OpenChain {
        ownership: ChainOwnership,
        #[debug(skip_if = Amount::is_zero)]
        balance: Amount,
        parent_id: ChainId,
        block_height: BlockHeight,
        application_permissions: ApplicationPermissions,
        timestamp: Timestamp,
        #[debug(skip)]
        callback: Sender<ChainId>,
    },

    CloseChain {
        application_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    ChangeApplicationPermissions {
        application_id: ApplicationId,
        application_permissions: ApplicationPermissions,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    CreateApplication {
        chain_id: ChainId,
        block_height: BlockHeight,
        module_id: ModuleId,
        parameters: Vec<u8>,
        required_application_ids: Vec<ApplicationId>,
        #[debug(skip)]
        callback: Sender<Result<CreateApplicationResult, ExecutionError>>,
    },

    PerformHttpRequest {
        request: http::Request,
        http_responses_are_oracle_responses: bool,
        #[debug(skip)]
        callback: Sender<http::Response>,
    },

    ReadBlobContent {
        blob_id: BlobId,
        #[debug(skip)]
        callback: Sender<BlobContent>,
    },

    AssertBlobExists {
        blob_id: BlobId,
        #[debug(skip)]
        callback: Sender<()>,
    },

    Emit {
        stream_id: StreamId,
        #[debug(with = hex_debug)]
        value: Vec<u8>,
        #[debug(skip)]
        callback: Sender<u32>,
    },

    ReadEvent {
        event_id: EventId,
        callback: oneshot::Sender<Vec<u8>>,
    },

    SubscribeToEvents {
        chain_id: ChainId,
        stream_id: StreamId,
        subscriber_app_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<()>,
    },

    UnsubscribeFromEvents {
        chain_id: ChainId,
        stream_id: StreamId,
        subscriber_app_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<()>,
    },

    GetApplicationPermissions {
        #[debug(skip)]
        callback: Sender<ApplicationPermissions>,
    },

    QueryServiceOracle {
        #[debug(skip)]
        callback: Sender<Option<Vec<u8>>>,
    },

    QueryService {
        #[debug(with = hex_debug)]
        response: Vec<u8>,
        #[debug(skip)]
        callback: Sender<()>,
    },

    AddOutgoingMessage {
        message: crate::OutgoingMessage,
        #[debug(skip)]
        callback: Sender<()>,
    },

    SetLocalTime {
        local_time: Timestamp,
        #[debug(skip)]
        callback: Sender<()>,
    },

    GetLocalTime {
        #[debug(skip)]
        callback: Sender<Timestamp>,
    },

    GetCreatedBlobs {
        #[debug(skip)]
        callback: Sender<std::collections::BTreeMap<BlobId, BlobContent>>,
    },

    AssertBefore {
        timestamp: Timestamp,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    AddCreatedBlob {
        blob: crate::Blob,
        #[debug(skip)]
        callback: Sender<()>,
    },

    ValidationRound {
        round: Option<u32>,
        #[debug(skip)]
        callback: Sender<Option<u32>>,
    },

    AddOracleResponse {
        response: OracleResponse,
        #[debug(skip)]
        callback: Sender<()>,
    },
}
