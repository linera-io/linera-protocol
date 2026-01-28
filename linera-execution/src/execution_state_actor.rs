// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Handle requests from the synchronous execution thread of user applications.

use std::collections::{BTreeMap, BTreeSet};

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
    time::Instant,
};
use linera_views::{batch::Batch, context::Context, views::View};
use oneshot::Sender;
use reqwest::{header::HeaderMap, Client, Url};

use crate::{
    execution::UserAction,
    runtime::ContractSyncRuntime,
    system::{CreateApplicationResult, OpenChainConfig},
    util::{OracleResponseExt as _, RespondExt as _},
    ApplicationDescription, ApplicationId, ExecutionError, ExecutionRuntimeContext,
    ExecutionStateView, JsVec, Message, MessageContext, MessageKind, ModuleId, Operation,
    OperationContext, OutgoingMessage, ProcessStreamsContext, QueryContext, QueryOutcome,
    ResourceController, SystemMessage, TransactionTracker, UserContractCode, UserServiceCode,
};

/// Actor for handling requests to the execution state.
pub struct ExecutionStateActor<'a, C> {
    state: &'a mut ExecutionStateView<C>,
    txn_tracker: &'a mut TransactionTracker,
    resource_controller: &'a mut ResourceController<Option<AccountOwner>>,
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
    C: Context + Clone + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Creates a new execution state actor.
    pub fn new(
        state: &'a mut ExecutionStateView<C>,
        txn_tracker: &'a mut TransactionTracker,
        resource_controller: &'a mut ResourceController<Option<AccountOwner>>,
    ) -> Self {
        Self {
            state,
            txn_tracker,
            resource_controller,
        }
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
                callback.respond(self.state.system.balances.index_values().await?);
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

            ApplicationPermissions { callback } => {
                let permissions = self.state.system.application_permissions.get().clone();
                callback.respond(permissions);
            }

            ReadApplicationDescription {
                application_id,
                callback,
            } => {
                let blob_id = application_id.description_blob_id();
                let description = match self.txn_tracker.get_blob_content(&blob_id) {
                    Some(blob) => bcs::from_bytes(blob.bytes())?,
                    None => {
                        let blob_content = self.state.system.read_blob_content(blob_id).await?;
                        self.state
                            .system
                            .blob_used(self.txn_tracker, blob_id)
                            .await?;
                        bcs::from_bytes(blob_content.bytes())?
                    }
                };
                callback.respond(description);
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
                    Some(view) => view.contains_keys(&keys).await?,
                    None => vec![false; keys.len()],
                };
                callback.respond(result);
            }

            ReadMultiValuesBytes { id, keys, callback } => {
                let view = self.state.users.try_load_entry(&id).await?;
                let values = match view {
                    Some(view) => view.multi_get(&keys).await?,
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
                if !app_permissions.can_manage_chain(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.state.system.close_chain();
                    callback.respond(Ok(()));
                }
            }

            ChangeOwnership {
                application_id,
                ownership,
                callback,
            } => {
                let app_permissions = self.state.system.application_permissions.get();
                if !app_permissions.can_manage_chain(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.state.system.ownership.set(ownership);
                    callback.respond(Ok(()));
                }
            }

            ChangeApplicationPermissions {
                application_id,
                application_permissions,
                callback,
            } => {
                let app_permissions = self.state.system.application_permissions.get();
                if !app_permissions.can_manage_chain(&application_id) {
                    callback.respond(Err(ExecutionError::UnauthorizedApplication(application_id)));
                } else {
                    self.state
                        .system
                        .application_permissions
                        .set(application_permissions);
                    callback.respond(Ok(()));
                }
            }

            PeekApplicationIndex { callback } => {
                let index = self.txn_tracker.peek_application_index();
                callback.respond(index)
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
                callback.respond(create_application_result);
            }

            PerformHttpRequest {
                request,
                http_responses_are_oracle_responses,
                callback,
            } => {
                let system = &mut self.state.system;
                let response = self
                    .txn_tracker
                    .oracle(|| async {
                        let headers = request
                            .headers
                            .into_iter()
                            .map(|http::Header { name, value }| {
                                Ok((name.parse()?, value.try_into()?))
                            })
                            .collect::<Result<HeaderMap, ExecutionError>>()?;

                        let url = Url::parse(&request.url)?;
                        let host = url
                            .host_str()
                            .ok_or_else(|| ExecutionError::UnauthorizedHttpRequest(url.clone()))?;

                        let (_epoch, committee) = system
                            .current_committee()
                            .ok_or_else(|| ExecutionError::UnauthorizedHttpRequest(url.clone()))?;
                        let allowed_hosts = &committee.policy().http_request_allow_list;

                        ensure!(
                            allowed_hosts.contains(host),
                            ExecutionError::UnauthorizedHttpRequest(url)
                        );

                        let request = Client::new()
                            .request(request.method.into(), url)
                            .body(request.body)
                            .headers(headers);
                        #[cfg(not(web))]
                        let request = request.timeout(linera_base::time::Duration::from_millis(
                            committee.policy().http_request_timeout_ms,
                        ));

                        let response = request.send().await?;

                        let mut response_size_limit =
                            committee.policy().maximum_http_response_bytes;

                        if http_responses_are_oracle_responses {
                            response_size_limit = response_size_limit
                                .min(committee.policy().maximum_oracle_response_bytes);
                        }
                        Ok(OracleResponse::Http(
                            Self::receive_http_response(response, response_size_limit).await?,
                        ))
                    })
                    .await?
                    .to_http_response()?;
                callback.respond(response);
            }

            ReadBlobContent { blob_id, callback } => {
                let content = if let Some(content) = self.txn_tracker.get_blob_content(&blob_id) {
                    content.clone()
                } else {
                    let content = self.state.system.read_blob_content(blob_id).await?;
                    if blob_id.blob_type == BlobType::Data {
                        self.resource_controller
                            .with_state(&mut self.state.system)
                            .await?
                            .track_blob_read(content.bytes().len() as u64)?;
                    }
                    self.state
                        .system
                        .blob_used(self.txn_tracker, blob_id)
                        .await?;
                    content
                };
                callback.respond(content)
            }

            AssertBlobExists { blob_id, callback } => {
                self.state.system.assert_blob_exists(blob_id).await?;
                // Treating this as reading a size-0 blob for fee purposes.
                if blob_id.blob_type == BlobType::Data {
                    self.resource_controller
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
                    .system
                    .stream_event_counts
                    .get_mut_or_default(&stream_id)
                    .await?;
                let index = *count;
                *count = count.checked_add(1).ok_or(ArithmeticError::Overflow)?;
                self.resource_controller
                    .with_state(&mut self.state.system)
                    .await?
                    .track_event_published(&value)?;
                self.txn_tracker.add_event(stream_id, index, value);
                callback.respond(index)
            }

            ReadEvent { event_id, callback } => {
                let context = self.state.context();
                let extra = context.extra();
                let event = self
                    .txn_tracker
                    .oracle(|| async {
                        let event = extra
                            .get_event(event_id.clone())
                            .await?
                            .ok_or(ExecutionError::EventsNotFound(vec![event_id.clone()]))?;
                        Ok(OracleResponse::Event(event_id.clone(), event))
                    })
                    .await?
                    .to_event(&event_id)?;
                self.resource_controller
                    .with_state(&mut self.state.system)
                    .await?
                    .track_event_read(event.len() as u64)?;
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

            QueryServiceOracle {
                deadline,
                application_id,
                next_block_height,
                query,
                callback,
            } => {
                let state = &mut self.state;
                let local_time = self.txn_tracker.local_time();
                let created_blobs = self.txn_tracker.created_blobs().clone();
                let bytes = self
                    .txn_tracker
                    .oracle(|| async {
                        let context = QueryContext {
                            chain_id: state.context().extra().chain_id(),
                            next_block_height,
                            local_time,
                        };
                        let QueryOutcome {
                            response,
                            operations,
                        } = Box::pin(state.query_user_application_with_deadline(
                            application_id,
                            context,
                            query,
                            deadline,
                            created_blobs,
                        ))
                        .await?;
                        ensure!(
                            operations.is_empty(),
                            ExecutionError::ServiceOracleQueryOperations(operations)
                        );
                        Ok(OracleResponse::Service(response))
                    })
                    .await?
                    .to_service_response()?;
                callback.respond(bytes);
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
                let validation_round = self
                    .txn_tracker
                    .oracle(|| async { Ok(OracleResponse::Round(round)) })
                    .await?
                    .to_round()?;
                callback.respond(validation_round);
            }

            TotalStorageSize {
                application,
                callback,
            } => {
                let view = self.state.users.try_load_entry(&application).await?;
                let result = match view {
                    Some(view) => {
                        let total_size = view.total_size();
                        (total_size.key, total_size.value)
                    }
                    None => (0, 0),
                };
                callback.respond(result);
            }

            AllowApplicationLogs { callback } => {
                let allow = self
                    .state
                    .context()
                    .extra()
                    .execution_runtime_config()
                    .allow_application_logs;
                callback.respond(allow);
            }

            #[cfg(web)]
            Log { message, level } => {
                // Output directly to browser console with clean formatting
                let formatted: js_sys::JsString = format!("[CONTRACT {level}] {message}").into();
                match level {
                    tracing::log::Level::Trace | tracing::log::Level::Debug => {
                        web_sys::console::debug_1(&formatted)
                    }
                    tracing::log::Level::Info => web_sys::console::log_1(&formatted),
                    tracing::log::Level::Warn => web_sys::console::warn_1(&formatted),
                    tracing::log::Level::Error => web_sys::console::error_1(&formatted),
                }
            }
        }

        Ok(())
    }

    /// Calls `process_streams` for all applications that are subscribed to streams with new
    /// events or that have new subscriptions.
    async fn process_subscriptions(
        &mut self,
        context: ProcessStreamsContext,
    ) -> Result<(), ExecutionError> {
        // Keep track of which streams we have already processed. This is to guard against
        // applications unsubscribing and subscribing in the process_streams call itself.
        let mut processed = BTreeSet::new();
        loop {
            let to_process = self
                .txn_tracker
                .take_streams_to_process()
                .into_iter()
                .filter_map(|(app_id, updates)| {
                    let updates = updates
                        .into_iter()
                        .filter_map(|update| {
                            if !processed.insert((
                                app_id,
                                update.chain_id,
                                update.stream_id.clone(),
                            )) {
                                return None;
                            }
                            Some(update)
                        })
                        .collect::<Vec<_>>();
                    if updates.is_empty() {
                        return None;
                    }
                    Some((app_id, updates))
                })
                .collect::<BTreeMap<_, _>>();
            if to_process.is_empty() {
                return Ok(());
            }
            for (app_id, updates) in to_process {
                self.run_user_action(
                    app_id,
                    UserAction::ProcessStreams(context, updates),
                    None,
                    None,
                )
                .await?;
            }
        }
    }

    pub(crate) async fn run_user_action(
        &mut self,
        application_id: ApplicationId,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
    ) -> Result<(), ExecutionError> {
        self.run_user_action_with_runtime(application_id, action, refund_grant_to, grant)
            .await
    }

    // TODO(#5034): unify with `contract_and_dependencies`
    pub(crate) async fn service_and_dependencies(
        &mut self,
        application: ApplicationId,
    ) -> Result<(Vec<UserServiceCode>, Vec<ApplicationDescription>), ExecutionError> {
        // cyclic futures are illegal so we need to either box the frames or keep our own
        // stack
        let mut stack = vec![application];
        let mut codes = vec![];
        let mut descriptions = vec![];

        while let Some(id) = stack.pop() {
            let (code, description) = self.load_service(id).await?;
            stack.extend(description.required_application_ids.iter().rev().copied());
            codes.push(code);
            descriptions.push(description);
        }

        codes.reverse();
        descriptions.reverse();

        Ok((codes, descriptions))
    }

    // TODO(#5034): unify with `service_and_dependencies`
    async fn contract_and_dependencies(
        &mut self,
        application: ApplicationId,
    ) -> Result<(Vec<UserContractCode>, Vec<ApplicationDescription>), ExecutionError> {
        // cyclic futures are illegal so we need to either box the frames or keep our own
        // stack
        let mut stack = vec![application];
        let mut codes = vec![];
        let mut descriptions = vec![];

        while let Some(id) = stack.pop() {
            let (code, description) = self.load_contract(id).await?;
            stack.extend(description.required_application_ids.iter().rev().copied());
            codes.push(code);
            descriptions.push(description);
        }

        codes.reverse();
        descriptions.reverse();

        Ok((codes, descriptions))
    }

    async fn run_user_action_with_runtime(
        &mut self,
        application_id: ApplicationId,
        action: UserAction,
        refund_grant_to: Option<Account>,
        grant: Option<&mut Amount>,
    ) -> Result<(), ExecutionError> {
        let chain_id = self.state.context().extra().chain_id();
        let mut cloned_grant = grant.as_ref().map(|x| **x);
        let initial_balance = self
            .resource_controller
            .with_state_and_grant(&mut self.state.system, cloned_grant.as_mut())
            .await?
            .balance()?;
        let controller = ResourceController::new(
            self.resource_controller.policy().clone(),
            self.resource_controller.tracker,
            initial_balance,
        );
        let (execution_state_sender, mut execution_state_receiver) =
            futures::channel::mpsc::unbounded();

        let (codes, descriptions): (Vec<_>, Vec<_>) =
            self.contract_and_dependencies(application_id).await?;

        let allow_application_logs = self
            .state
            .context()
            .extra()
            .execution_runtime_config()
            .allow_application_logs;

        let contract_runtime_task = self
            .state
            .context()
            .extra()
            .thread_pool()
            .run_send(JsVec(codes), move |codes| async move {
                let runtime = ContractSyncRuntime::new(
                    execution_state_sender,
                    chain_id,
                    refund_grant_to,
                    controller,
                    &action,
                    allow_application_logs,
                );

                for (code, description) in codes.0.into_iter().zip(descriptions) {
                    runtime.preload_contract(
                        ApplicationId::from(&description),
                        code,
                        description,
                    )?;
                }

                runtime.run_action(application_id, chain_id, action)
            })
            .await;

        while let Some(request) = execution_state_receiver.next().await {
            self.handle_request(request).await?;
        }

        let (result, controller) = contract_runtime_task.await??;

        self.txn_tracker.add_operation_result(result);

        self.resource_controller
            .with_state_and_grant(&mut self.state.system, grant)
            .await?
            .merge_balance(initial_balance, controller.balance()?)?;
        self.resource_controller.tracker = controller.tracker;

        Ok(())
    }

    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: Operation,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.state.context().extra().chain_id());
        match operation {
            Operation::System(op) => {
                let new_application = self
                    .state
                    .system
                    .execute_operation(context, *op, self.txn_tracker, self.resource_controller)
                    .await?;
                if let Some((application_id, argument)) = new_application {
                    let user_action = UserAction::Instantiate(context, argument);
                    self.run_user_action(
                        application_id,
                        user_action,
                        context.refund_grant_to(),
                        None,
                    )
                    .await?;
                }
            }
            Operation::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    UserAction::Operation(context, bytes),
                    context.refund_grant_to(),
                    None,
                )
                .await?;
            }
        }
        self.process_subscriptions(context.into()).await?;
        Ok(())
    }

    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: Message,
        grant: Option<&mut Amount>,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.state.context().extra().chain_id());
        match message {
            Message::System(message) => {
                let outcome = self.state.system.execute_message(context, message).await?;
                self.txn_tracker.add_outgoing_messages(outcome);
            }
            Message::User {
                application_id,
                bytes,
            } => {
                self.run_user_action(
                    application_id,
                    UserAction::Message(context, bytes),
                    context.refund_grant_to,
                    grant,
                )
                .await?;
            }
        }
        self.process_subscriptions(context.into()).await?;
        Ok(())
    }

    pub fn bounce_message(
        &mut self,
        context: MessageContext,
        grant: Amount,
        message: Message,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.state.context().extra().chain_id());
        self.txn_tracker.add_outgoing_message(OutgoingMessage {
            destination: context.origin,
            authenticated_owner: context.authenticated_owner,
            refund_grant_to: context.refund_grant_to.filter(|_| !grant.is_zero()),
            grant,
            kind: MessageKind::Bouncing,
            message,
        });
        Ok(())
    }

    pub fn send_refund(
        &mut self,
        context: MessageContext,
        amount: Amount,
    ) -> Result<(), ExecutionError> {
        assert_eq!(context.chain_id, self.state.context().extra().chain_id());
        if amount.is_zero() {
            return Ok(());
        }
        let Some(account) = context.refund_grant_to else {
            return Err(ExecutionError::InternalError(
                "Messages with grants should have a non-empty `refund_grant_to`",
            ));
        };
        let message = SystemMessage::Credit {
            amount,
            source: context.authenticated_owner.unwrap_or(AccountOwner::CHAIN),
            target: account.owner,
        };
        self.txn_tracker.add_outgoing_message(
            OutgoingMessage::new(account.chain_id, message).with_kind(MessageKind::Tracked),
        );
        Ok(())
    }

    /// Receives an HTTP response, returning the prepared [`http::Response`] instance.
    ///
    /// Ensures that the response does not exceed the provided `size_limit`.
    async fn receive_http_response(
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

    ApplicationPermissions {
        #[debug(skip)]
        callback: Sender<ApplicationPermissions>,
    },

    ReadApplicationDescription {
        application_id: ApplicationId,
        #[debug(skip)]
        callback: Sender<ApplicationDescription>,
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

    ChangeOwnership {
        application_id: ApplicationId,
        ownership: ChainOwnership,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    ChangeApplicationPermissions {
        application_id: ApplicationId,
        application_permissions: ApplicationPermissions,
        #[debug(skip)]
        callback: Sender<Result<(), ExecutionError>>,
    },

    PeekApplicationIndex {
        #[debug(skip)]
        callback: Sender<u32>,
    },

    CreateApplication {
        chain_id: ChainId,
        block_height: BlockHeight,
        module_id: ModuleId,
        parameters: Vec<u8>,
        required_application_ids: Vec<ApplicationId>,
        #[debug(skip)]
        callback: Sender<CreateApplicationResult>,
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
        deadline: Option<Instant>,
        application_id: ApplicationId,
        next_block_height: BlockHeight,
        query: Vec<u8>,
        #[debug(skip)]
        callback: Sender<Vec<u8>>,
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

    TotalStorageSize {
        application: ApplicationId,
        #[debug(skip)]
        callback: Sender<(u32, u32)>,
    },

    AllowApplicationLogs {
        #[debug(skip)]
        callback: Sender<bool>,
    },

    /// Log message from contract execution (fire-and-forget, no callback needed).
    #[cfg(web)]
    Log {
        message: String,
        level: tracing::log::Level,
    },
}
