// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use colored::Colorize;
use futures::{lock::OwnedMutexGuard, Future};
use linera_base::{
    crypto::KeyPair,
    data_types::{BlockHeight, HashedBlob, Timestamp},
    identifiers::{Account, BlobId, BytecodeId, ChainId},
    ownership::ChainOwnership,
};
use linera_chain::data_types::Certificate;
use linera_core::{
    client::{ArcChainClient, ChainClient, Client},
    data_types::ClientOutcome,
    node::CrossChainMessageDelivery,
    JoinSetExt as _,
};
use linera_execution::Bytecode;
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_service::{
    chain_listener,
    config::WalletState,
    node_service::wait_for_next_round,
    wallet::{UserChain, Wallet},
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use tokio::task::JoinSet;
use tracing::{debug, info};
#[cfg(feature = "benchmark")]
use {
    futures::{stream, StreamExt as _},
    linera_base::{
        crypto::PublicKey,
        data_types::Amount,
        identifiers::{AccountOwner, ApplicationId, Owner},
    },
    linera_chain::data_types::{Block, BlockProposal, SignatureAggregator, Vote},
    linera_core::{data_types::ChainInfoQuery, local_node::LocalNodeClient, worker::WorkerState},
    linera_execution::{
        committee::Epoch,
        system::{OpenChainConfig, Recipient, SystemOperation, UserData, OPEN_CHAIN_MESSAGE_INDEX},
        Operation,
    },
    linera_rpc::{
        config::NetworkProtocol, grpc::GrpcClient, mass_client::MassClient,
        simple::SimpleMassClient, RpcMessage,
    },
    linera_sdk::abis::fungible,
    std::{
        collections::{HashMap, HashSet},
        iter,
    },
    tracing::{error, trace},
};

use crate::{client_options::ChainOwnershipConfig, ClientOptions};

pub struct ClientContext<Storage> {
    pub(crate) wallet_state: WalletState,
    pub(crate) client: Arc<Client<NodeProvider, Storage>>,
    pub(crate) send_timeout: Duration,
    pub(crate) recv_timeout: Duration,
    pub(crate) notification_retry_delay: Duration,
    pub(crate) notification_retries: u32,
    chain_listeners: JoinSet<()>,
}

#[async_trait]
impl<S> chain_listener::ClientContext for ClientContext<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<<S as Storage>::ContextError>,
{
    type ValidatorNodeProvider = NodeProvider;
    type Storage = S;

    fn wallet(&self) -> &Wallet {
        self.wallet_state.inner()
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<NodeProvider, S> {
        self.make_chain_client(chain_id)
    }

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        self.update_wallet_for_new_chain(chain_id, key_pair, timestamp);
        self.save_wallet();
    }

    async fn update_wallet<'a>(&'a mut self, client: &'a mut ChainClient<NodeProvider, S>) {
        self.update_and_save_wallet(client).await;
    }
}

impl<S> ClientContext<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Returns the [`Wallet`] as an immutable reference.
    fn wallet(&self) -> &Wallet {
        self.wallet_state.inner()
    }

    /// Returns the [`Wallet`] as a mutable reference.
    pub fn wallet_mut(&mut self) -> &mut Wallet {
        self.wallet_state.inner_mut()
    }

    pub fn new(storage: S, options: &ClientOptions, wallet_state: WalletState) -> Self {
        let node_options = NodeOptions {
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            notification_retry_delay: options.notification_retry_delay,
            notification_retries: options.notification_retries,
        };
        let node_provider = NodeProvider::new(node_options);
        let delivery = CrossChainMessageDelivery::new(options.wait_for_outgoing_messages);
        let client = Arc::new(
            Client::new(
                node_provider,
                storage,
                options.max_pending_messages,
                delivery,
            )
            .with_message_policy(options.message_policy),
        );
        ClientContext {
            client,
            wallet_state,
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            notification_retry_delay: options.notification_retry_delay,
            notification_retries: options.notification_retries,
            chain_listeners: JoinSet::new(),
        }
    }

    /// Retrieve the default account. Current this is the common account of the default
    /// chain.
    pub fn default_account(&self) -> Account {
        Account::chain(self.default_chain())
    }

    /// Retrieve the default chain.
    pub fn default_chain(&self) -> ChainId {
        self.wallet()
            .default_chain()
            .expect("No chain specified in wallet with no default chain")
    }

    fn make_chain_client(&self, chain_id: ChainId) -> ChainClient<NodeProvider, S> {
        let chain = self
            .wallet()
            .get(chain_id)
            .unwrap_or_else(|| panic!("Unknown chain: {}", chain_id));
        let known_key_pairs = chain
            .key_pair
            .as_ref()
            .map(|kp| kp.copy())
            .into_iter()
            .collect();
        self.client.build(
            chain_id,
            known_key_pairs,
            self.wallet().genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_block.clone(),
            chain.pending_blobs.clone(),
        )
    }

    pub fn make_node_provider(&self) -> NodeProvider {
        NodeProvider::new(self.make_node_options())
    }

    fn make_node_options(&self) -> NodeOptions {
        NodeOptions {
            send_timeout: self.send_timeout,
            recv_timeout: self.recv_timeout,
            notification_retry_delay: self.notification_retry_delay,
            notification_retries: self.notification_retries,
        }
    }

    pub fn save_wallet(&mut self) {
        self.wallet_state.refresh_prng_seed();
        self.wallet_state
            .write()
            .expect("Unable to write user chains");
        info!("Saved user chain states");
    }

    async fn update_wallet_from_client(&mut self, state: &mut ChainClient<NodeProvider, S>) {
        self.wallet_mut().update_from_state(state).await
    }

    pub async fn update_and_save_wallet(&mut self, state: &mut ChainClient<NodeProvider, S>) {
        self.update_wallet_from_client(state).await;
        self.save_wallet()
    }

    /// Remembers the new private key (if any) in the wallet.
    pub fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        if self.wallet().get(chain_id).is_none() {
            self.wallet_mut().insert(UserChain {
                chain_id,
                key_pair: key_pair.as_ref().map(|kp| kp.copy()),
                block_hash: None,
                timestamp,
                next_block_height: BlockHeight::ZERO,
                pending_block: None,
                pending_blobs: BTreeMap::new(),
            });
        }
    }

    pub async fn process_inbox(
        &mut self,
        chain_client: &ArcChainClient<NodeProvider, S>,
    ) -> anyhow::Result<Vec<Certificate>> {
        let mut certificates = Vec::new();
        // Try processing the inbox optimistically without waiting for validator notifications.
        let (new_certificates, maybe_timeout) = {
            let mut guard = chain_client.0.lock().await;
            guard.synchronize_from_validators().await?;
            let result = guard.process_inbox().await;
            self.update_wallet_from_client(&mut *guard).await;
            if result.is_err() {
                self.save_wallet();
            }
            result?
        };
        certificates.extend(new_certificates);
        if maybe_timeout.is_none() {
            self.save_wallet();
            return Ok(certificates);
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = chain_client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            let (new_certificates, maybe_timeout) = {
                let mut guard = chain_client.0.lock().await;
                let result = guard.process_inbox().await;
                self.update_wallet_from_client(&mut *guard).await;
                if result.is_err() {
                    self.save_wallet();
                }
                result?
            };
            certificates.extend(new_certificates);
            match maybe_timeout {
                None => {
                    self.save_wallet();
                    return Ok(certificates);
                }
                Some(timestamp) => wait_for_next_round(&mut notification_stream, timestamp).await,
            }
        }
    }

    pub async fn publish_bytecode(
        &mut self,
        chain_client: &ArcChainClient<NodeProvider, S>,
        contract: PathBuf,
        service: PathBuf,
    ) -> anyhow::Result<BytecodeId> {
        info!("Loading bytecode files");
        let contract_bytecode = Bytecode::load_from_file(&contract).await.context(format!(
            "failed to load contract bytecode from {:?}",
            &contract
        ))?;
        let service_bytecode = Bytecode::load_from_file(&service).await.context(format!(
            "failed to load service bytecode from {:?}",
            &service
        ))?;

        info!("Publishing bytecode");
        let (bytecode_id, _) = self
            .apply_client_command(chain_client, |mut chain_client| {
                let contract_bytecode = contract_bytecode.clone();
                let service_bytecode = service_bytecode.clone();
                async move {
                    chain_client
                        .publish_bytecode(contract_bytecode, service_bytecode)
                        .await
                        .context("Failed to publish bytecode")
                }
            })
            .await?;

        info!("{}", "Bytecode published successfully!".green().bold());

        info!("Synchronizing client and processing inbox");
        chain_client
            .lock()
            .await
            .synchronize_from_validators()
            .await?;
        self.process_inbox(chain_client).await?;
        Ok(bytecode_id)
    }

    pub async fn publish_blob(
        &mut self,
        chain_client: &ArcChainClient<NodeProvider, S>,
        blob_path: PathBuf,
    ) -> anyhow::Result<BlobId> {
        info!("Loading blob file");
        let blob = HashedBlob::load_from_file(&blob_path)
            .await
            .context(format!("failed to load blob from {:?}", &blob_path))?;
        let blob_id = blob.id();

        info!("Publishing blob");
        self.apply_client_command(chain_client, |mut chain_client| {
            let blob = blob.clone();
            async move {
                chain_client
                    .publish_blob(blob)
                    .await
                    .context("Failed to publish blob")
            }
        })
        .await?;

        info!("{}", "Blob published successfully!".green().bold());
        Ok(blob_id)
    }

    /// Applies the given function to the chain client.
    ///
    /// Updates the wallet regardless of the outcome. As long as the function returns a round
    /// timeout, it will wait and retry.
    pub async fn apply_client_command<E, F, Fut, T>(
        &mut self,
        client: &ArcChainClient<NodeProvider, S>,
        mut f: F,
    ) -> anyhow::Result<T>
    where
        F: FnMut(OwnedMutexGuard<ChainClient<NodeProvider, S>>) -> Fut,
        Fut: Future<Output = Result<ClientOutcome<T>, E>>,
        anyhow::Error: From<E>,
    {
        // Try applying f optimistically without validator notifications. Return if committed.
        let result = f(client.0.clone().lock_owned().await).await;
        self.update_and_save_wallet(&mut *client.lock().await).await;
        if let ClientOutcome::Committed(t) = result? {
            return Ok(t);
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            // Try applying f. Return if committed.
            let result = f(client.0.clone().lock_owned().await).await;
            self.update_and_save_wallet(&mut *client.lock().await).await;
            let timeout = match result? {
                ClientOutcome::Committed(t) => return Ok(t),
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            // Otherwise wait and try again in the next round.
            wait_for_next_round(&mut notification_stream, timeout).await;
        }
    }

    pub async fn change_ownership(
        &mut self,
        chain_id: Option<ChainId>,
        ownership_config: ChainOwnershipConfig,
    ) -> anyhow::Result<()> {
        let chain_id = chain_id.unwrap_or_else(|| self.default_chain());
        let chain_client = self.make_chain_client(chain_id).into_arc();
        info!("Changing ownership for chain {}", chain_id);
        let time_start = Instant::now();
        let ownership = ChainOwnership::try_from(ownership_config)?;

        let certificate = self
            .apply_client_command(&chain_client, |mut chain_client| {
                let ownership = ownership.clone();
                async move {
                    chain_client
                        .change_ownership(ownership)
                        .await
                        .context("Failed to change ownership")
                }
            })
            .await?;
        let time_total = time_start.elapsed();
        info!("Operation confirmed after {} ms", time_total.as_millis());
        debug!("{:?}", certificate);
        Ok(())
    }
}

#[cfg(feature = "benchmark")]
impl<S> ClientContext<S>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    pub async fn process_inboxes_and_force_validator_updates(&mut self) {
        for chain_id in self.wallet().own_chain_ids() {
            let chain_client = self.make_chain_client(chain_id).into_arc();
            self.process_inbox(&chain_client).await.unwrap();
            chain_client.lock().await.update_validators().await.unwrap();
        }
    }

    /// Creates chains if necessary, and returns a map of exactly `num_chains` chain IDs
    /// with key pairs.
    pub async fn make_benchmark_chains(
        &mut self,
        num_chains: usize,
        balance: Amount,
    ) -> anyhow::Result<HashMap<ChainId, KeyPair>> {
        let mut key_pairs = HashMap::new();
        for chain_id in self.wallet().own_chain_ids() {
            if key_pairs.len() == num_chains {
                break;
            }
            let Some(key_pair) = self
                .wallet()
                .get(chain_id)
                .and_then(|chain| chain.key_pair.as_ref().map(|kp| kp.copy()))
            else {
                continue;
            };
            let mut chain_client = self.make_chain_client(chain_id);
            let ownership = chain_client.chain_info().await?.manager.ownership;
            if !ownership.owners.is_empty() || ownership.super_owners.len() != 1 {
                continue;
            }
            key_pairs.insert(chain_id, key_pair);
        }

        let default_chain_id = self
            .wallet()
            .default_chain()
            .context("should have default chain")?;
        let mut chain_client = self.make_chain_client(default_chain_id);
        while key_pairs.len() < num_chains {
            let key_pair = self.wallet_state.generate_key_pair();
            let public_key = key_pair.public();
            let (epoch, committees) = chain_client.epoch_and_committees(default_chain_id).await?;
            let epoch = epoch.context("missing epoch on the default chain")?;
            // Put at most 1000 OpenChain operations in each block.
            let num_new_chains = (num_chains - key_pairs.len()).min(1000);
            let config = OpenChainConfig {
                ownership: ChainOwnership::single(public_key),
                committees,
                admin_id: self.wallet().genesis_admin_chain(),
                epoch,
                balance,
                application_permissions: Default::default(),
            };
            let operations = iter::repeat(Operation::System(SystemOperation::OpenChain(config)))
                .take(num_new_chains)
                .collect();
            let certificate = chain_client
                .execute_with_messages(operations)
                .await?
                .expect("should execute block with OpenChain operations");
            let executed_block = certificate
                .value()
                .executed_block()
                .context("certificate should be confirmed block")?;
            let timestamp = executed_block.block.timestamp;
            for i in 0..num_new_chains {
                let message_id = executed_block
                    .message_id_for_operation(i, OPEN_CHAIN_MESSAGE_INDEX)
                    .context("failed to create new chain")?;
                let chain_id = ChainId::child(message_id);
                key_pairs.insert(chain_id, key_pair.copy());
                self.update_wallet_for_new_chain(chain_id, Some(key_pair.copy()), timestamp);
            }
        }

        for chain_id in key_pairs.keys() {
            let mut child_client = self.make_chain_client(*chain_id);
            child_client.process_inbox().await?;
            self.wallet_mut().update_from_state(&mut child_client).await;
        }
        Ok(key_pairs)
    }

    /// Creates chains if necessary, and returns a map of exactly `num_chains` chain IDs
    /// with key pairs.
    pub async fn supply_fungible_tokens(
        &mut self,
        key_pairs: &HashMap<ChainId, KeyPair>,
        application_id: ApplicationId,
        max_in_flight: usize,
    ) -> anyhow::Result<()> {
        let default_chain_id = self
            .wallet()
            .default_chain()
            .context("should have default chain")?;
        let default_key = self
            .wallet()
            .get(default_chain_id)
            .unwrap()
            .key_pair
            .as_ref()
            .unwrap()
            .public();
        let amount = Amount::from(1_000_000);
        let operations: Vec<_> = key_pairs
            .iter()
            .map(|(chain_id, key_pair)| {
                Self::fungible_transfer(
                    application_id,
                    *chain_id,
                    default_key,
                    key_pair.public(),
                    amount,
                )
            })
            .collect();
        let mut chain_client = self.make_chain_client(default_chain_id);
        // Put at most 1000 fungible token operations in each block.
        for operations in operations.chunks(1000) {
            chain_client
                .execute_with_messages(operations.to_vec())
                .await?
                .expect("should execute block with OpenChain operations");
        }
        self.update_wallet_from_client(&mut chain_client).await;
        // Make sure all chains have registered the application now.
        let futures = key_pairs
            .keys()
            .map(|&chain_id| {
                let mut chain_client = self.make_chain_client(chain_id);
                async move {
                    for i in 0..5 {
                        tokio::time::sleep(Duration::from_secs(i)).await;
                        chain_client.process_inbox().await?;
                        let chain_state = chain_client.chain_state_view().await?;
                        if chain_state
                            .execution_state
                            .system
                            .registry
                            .known_applications
                            .contains_key(&application_id)
                            .await?
                        {
                            return Ok(chain_client);
                        }
                    }
                    anyhow::bail!("Could not instantiate application on chain {:?}", chain_id);
                }
            })
            .collect::<Vec<_>>();
        // We have to collect the futures to avoid a higher-ranked lifetime error:
        // https://github.com/rust-lang/rust/issues/102211#issuecomment-1673201352
        let results = stream::iter(futures)
            .buffer_unordered(max_in_flight)
            .collect::<Vec<_>>()
            .await;
        for result in results {
            let mut client = result?;
            self.update_wallet_from_client(&mut client).await;
        }
        Ok(())
    }

    /// Makes one block proposal per chain, up to `num_chains` blocks.
    pub fn make_benchmark_block_proposals(
        &mut self,
        key_pairs: &HashMap<ChainId, KeyPair>,
        transactions_per_block: usize,
        fungible_application_id: Option<ApplicationId>,
    ) -> Vec<RpcMessage> {
        let mut proposals = Vec::new();
        let mut next_recipient = self.wallet_mut().last_chain().unwrap().chain_id;
        let amount = Amount::from(1);
        for (&chain_id, key_pair) in key_pairs {
            let public_key = key_pair.public();
            let operation = match fungible_application_id {
                Some(application_id) => Self::fungible_transfer(
                    application_id,
                    next_recipient,
                    public_key,
                    public_key,
                    amount,
                ),
                None => Operation::System(SystemOperation::Transfer {
                    owner: None,
                    recipient: Recipient::chain(next_recipient),
                    amount,
                    user_data: UserData::default(),
                }),
            };
            let operations = iter::repeat(operation)
                .take(transactions_per_block)
                .collect();
            let chain = self.wallet().get(chain_id).expect("should have chain");
            let block = Block {
                epoch: Epoch::ZERO,
                chain_id,
                incoming_messages: Vec::new(),
                operations,
                previous_block_hash: chain.block_hash,
                height: chain.next_block_height,
                authenticated_signer: Some(Owner::from(public_key)),
                timestamp: chain.timestamp.max(Timestamp::now()),
            };
            trace!("Preparing block proposal: {:?}", block);
            let proposal = BlockProposal::new_initial(
                linera_base::data_types::Round::Fast,
                block.clone(),
                key_pair,
                vec![],
                vec![],
            );
            proposals.push(proposal.into());
            next_recipient = chain.chain_id;
        }
        proposals
    }

    /// Tries to aggregate votes into certificates.
    pub fn make_benchmark_certificates_from_votes(&self, votes: Vec<Vote>) -> Vec<Certificate> {
        let committee = self.wallet().genesis_config().create_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for vote in votes {
            // We aggregate votes indexed by sender.
            let chain_id = vote.value().chain_id();
            if done_senders.contains(&chain_id) {
                continue;
            }
            trace!(
                "Processing vote on {:?}'s block by {:?}",
                chain_id,
                vote.validator,
            );
            let aggregator = aggregators.entry(chain_id).or_insert_with(|| {
                SignatureAggregator::new(
                    vote.value,
                    linera_base::data_types::Round::Fast,
                    &committee,
                )
            });
            match aggregator.append(vote.validator, vote.signature) {
                Ok(Some(certificate)) => {
                    trace!("Found certificate: {:?}", certificate);
                    certificates.push(certificate);
                    done_senders.insert(chain_id);
                }
                Ok(None) => {
                    trace!("Added one vote");
                }
                Err(error) => {
                    error!("Failed to aggregate vote: {}", error);
                }
            }
        }
        certificates
    }

    /// Broadcasts a bulk of blocks to each validator.
    pub async fn mass_broadcast(
        &self,
        phase: &'static str,
        max_in_flight: usize,
        proposals: Vec<RpcMessage>,
    ) -> Vec<RpcMessage> {
        let time_start = Instant::now();
        info!("Broadcasting {} {}", proposals.len(), phase);
        let mut join_set = JoinSet::new();
        let mut handles = Vec::new();
        for mut client in self.make_validator_mass_clients() {
            let proposals = proposals.clone();
            let handle = join_set.spawn_task(async move {
                debug!("Sending {} requests", proposals.len());
                let responses = client
                    .send(proposals, max_in_flight)
                    .await
                    .unwrap_or_default();
                debug!("Done sending requests");
                responses
            });
            handles.push(handle);
        }
        let responses = futures::future::join_all(handles)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<RpcMessage>>();
        let time_elapsed = time_start.elapsed();
        info!(
            "Received {} responses in {} ms.",
            responses.len(),
            time_elapsed.as_millis()
        );
        info!(
            "Estimated server throughput: {} {} per sec",
            (proposals.len() as u128) * 1_000_000 / time_elapsed.as_micros(),
            phase
        );
        responses
    }

    fn make_validator_mass_clients(&self) -> Vec<Box<dyn MassClient + Send>> {
        let mut validator_clients = Vec::new();
        for config in &self.wallet().genesis_config().committee.validators {
            let client: Box<dyn MassClient + Send> = match config.network.protocol {
                NetworkProtocol::Simple(protocol) => {
                    let network = config.network.clone_with_protocol(protocol);
                    Box::new(SimpleMassClient::new(
                        network,
                        self.send_timeout,
                        self.recv_timeout,
                    ))
                }
                NetworkProtocol::Grpc { .. } => Box::new(
                    GrpcClient::new(config.network.clone(), self.make_node_options()).unwrap(),
                ),
            };

            validator_clients.push(client);
        }
        validator_clients
    }

    pub async fn update_wallet_from_certificates(&mut self, certificates: Vec<Certificate>) {
        // First instantiate a local node on top of storage.
        let worker = WorkerState::new(
            "Temporary client node".to_string(),
            None,
            self.client.storage_client().clone(),
        )
        .with_allow_inactive_chains(true)
        .with_allow_messages_from_deprecated_epochs(true);
        let node = LocalNodeClient::new(worker);
        // Second replay the certificates locally.
        for certificate in certificates {
            // No required certificates from other chains: This is only used with benchmark.
            node.handle_certificate(certificate, vec![], vec![], &mut vec![])
                .await
                .unwrap();
        }
        // Last update the wallet.
        for chain in self.wallet_mut().chains_mut() {
            let query = ChainInfoQuery::new(chain.chain_id);
            let info = node.handle_chain_info_query(query).await.unwrap().info;
            // We don't have private keys but that's ok.
            chain.block_hash = info.block_hash;
            chain.next_block_height = info.next_block_height;
        }
    }

    /// Creates a fungible token transfer operation.
    fn fungible_transfer(
        application_id: ApplicationId,
        chain_id: ChainId,
        sender: PublicKey,
        receiver: PublicKey,
        amount: Amount,
    ) -> Operation {
        let target_account = fungible::Account {
            chain_id,
            owner: AccountOwner::User(Owner::from(receiver)),
        };
        let bytes = bcs::to_bytes(&fungible::Operation::Transfer {
            owner: AccountOwner::User(Owner::from(sender)),
            amount,
            target_account,
        })
        .expect("should serialize fungible token operation");
        Operation::User {
            application_id,
            bytes,
        }
    }
}
