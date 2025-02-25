// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::num::NonZeroUsize;
use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use futures::Future;
use linera_base::{
    crypto::{AccountSecretKey, CryptoHash},
    data_types::{BlockHeight, Timestamp},
    identifiers::{Account, ChainId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{
    client::{BlanketMessagePolicy, ChainClient, Client, MessagePolicy, PendingProposal},
    data_types::ClientOutcome,
    join_set_ext::JoinSet,
    node::CrossChainMessageDelivery,
    JoinSetExt,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_storage::Storage;
use thiserror_context::Context;
use tracing::{debug, info};
#[cfg(feature = "benchmark")]
use {
    crate::benchmark::Benchmark,
    linera_base::{data_types::Amount, identifiers::ApplicationId},
    linera_execution::{
        committee::{Committee, Epoch},
        system::{OpenChainConfig, SystemOperation, OPEN_CHAIN_MESSAGE_INDEX},
        Operation,
    },
    std::{collections::HashMap, iter},
    tokio::task,
};
#[cfg(feature = "fs")]
use {
    linera_base::{
        data_types::{BlobContent, Bytecode},
        identifiers::BytecodeId,
    },
    linera_core::client::create_bytecode_blobs,
    std::{fs, path::PathBuf},
};

#[cfg(web)]
use crate::persistent::{LocalPersist as Persist, LocalPersistExt as _};
#[cfg(not(web))]
use crate::persistent::{Persist, PersistExt as _};
use crate::{
    chain_listener,
    client_options::{ChainOwnershipConfig, ClientOptions},
    config::WalletState,
    error, util,
    wallet::{UserChain, Wallet},
    Error,
};

pub struct ClientContext<Storage, W>
where
    Storage: linera_storage::Storage,
{
    pub wallet: WalletState<W>,
    pub client: Arc<Client<NodeProvider, Storage>>,
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
    pub chain_listeners: JoinSet,
    pub blanket_message_policy: BlanketMessagePolicy,
    pub restrict_chain_ids_to: Option<HashSet<ChainId>>,
}

#[cfg_attr(not(web), async_trait)]
#[cfg_attr(web, async_trait(?Send))]
impl<S, W> chain_listener::ClientContext for ClientContext<S, W>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet> + 'static,
{
    type ValidatorNodeProvider = NodeProvider;
    type Storage = S;

    fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    fn make_chain_client(&self, chain_id: ChainId) -> Result<ChainClient<NodeProvider, S>, Error> {
        self.make_chain_client(chain_id)
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<AccountSecretKey>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain(chain_id, key_pair, timestamp)
            .await?;
        self.save_wallet().await
    }

    async fn update_wallet(&mut self, client: &ChainClient<NodeProvider, S>) -> Result<(), Error> {
        self.update_and_save_wallet(client).await
    }
}

impl<S, W> ClientContext<S, W>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
{
    /// Returns a reference to the wallet.
    pub fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    /// Returns the [`WalletState`] as a mutable reference.
    pub fn wallet_mut(&mut self) -> &mut WalletState<W> {
        &mut self.wallet
    }

    pub async fn mutate_wallet<R: Send>(
        &mut self,
        mutation: impl FnOnce(&mut Wallet) -> R + Send,
    ) -> Result<R, Error> {
        self.wallet
            .mutate(mutation)
            .await
            .map_err(|e| error::Inner::Persistence(Box::new(e)).into())
    }

    pub fn new(storage: S, options: ClientOptions, wallet: W) -> Self {
        let node_options = NodeOptions {
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
        };
        let node_provider = NodeProvider::new(node_options);
        let delivery = CrossChainMessageDelivery::new(options.wait_for_outgoing_messages);
        let chain_ids = wallet.chain_ids();
        let name = match chain_ids.len() {
            0 => "Client node".to_string(),
            1 => format!("Client node for {:.8}", chain_ids[0]),
            n => format!("Client node for {:.8} and {} others", chain_ids[0], n - 1),
        };
        let client = Client::new(
            node_provider,
            storage,
            options.max_pending_message_bundles,
            delivery,
            options.long_lived_services,
            chain_ids,
            name,
            options.max_loaded_chains,
            options.grace_period,
            options.blob_download_timeout,
        );

        ClientContext {
            client: Arc::new(client),
            wallet: WalletState::new(wallet),
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
            chain_listeners: JoinSet::default(),
            blanket_message_policy: options.blanket_message_policy,
            restrict_chain_ids_to: options.restrict_chain_ids_to,
        }
    }

    #[cfg(with_testing)]
    pub fn new_test_client_context(storage: S, wallet: W) -> Self {
        use linera_core::DEFAULT_GRACE_PERIOD;

        let send_recv_timeout = Duration::from_millis(4000);
        let retry_delay = Duration::from_millis(1000);
        let max_retries = 10;

        let node_options = NodeOptions {
            send_timeout: send_recv_timeout,
            recv_timeout: send_recv_timeout,
            retry_delay,
            max_retries,
        };
        let node_provider = NodeProvider::new(node_options);
        let delivery = CrossChainMessageDelivery::new(true);
        let chain_ids = wallet.chain_ids();
        let name = match chain_ids.len() {
            0 => "Client node".to_string(),
            1 => format!("Client node for {:.8}", chain_ids[0]),
            n => format!("Client node for {:.8} and {} others", chain_ids[0], n - 1),
        };
        let client = Client::new(
            node_provider,
            storage,
            10,
            delivery,
            false,
            chain_ids,
            name,
            NonZeroUsize::new(20).expect("Chain worker limit should not be zero"),
            DEFAULT_GRACE_PERIOD,
            Duration::from_secs(1),
        );

        ClientContext {
            client: Arc::new(client),
            wallet: WalletState::new(wallet),
            send_timeout: send_recv_timeout,
            recv_timeout: send_recv_timeout,
            retry_delay,
            max_retries,
            chain_listeners: JoinSet::default(),
            blanket_message_policy: BlanketMessagePolicy::Accept,
            restrict_chain_ids_to: None,
        }
    }

    /// Retrieve the default account. Current this is the common account of the default
    /// chain.
    pub fn default_account(&self) -> Account {
        Account::chain(self.default_chain())
    }

    /// Retrieve the default chain.
    pub fn default_chain(&self) -> ChainId {
        self.wallet
            .default_chain()
            .expect("No chain specified in wallet with no default chain")
    }

    fn make_chain_client(&self, chain_id: ChainId) -> Result<ChainClient<NodeProvider, S>, Error> {
        let chain = self
            .wallet
            .get(chain_id)
            .ok_or_else(|| error::Inner::NonexistentChain(chain_id))?;
        let known_key_pairs = chain
            .key_pair
            .as_ref()
            .map(|kp| kp.copy())
            .into_iter()
            .collect();
        Ok(self.make_chain_client_internal(
            chain_id,
            known_key_pairs,
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_proposal.clone(),
        ))
    }

    fn make_chain_client_internal(
        &self,
        chain_id: ChainId,
        known_key_pairs: Vec<AccountSecretKey>,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_proposal: Option<PendingProposal>,
    ) -> ChainClient<NodeProvider, S> {
        let mut chain_client = self.client.create_chain_client(
            chain_id,
            known_key_pairs,
            self.wallet.genesis_admin_chain(),
            block_hash,
            timestamp,
            next_block_height,
            pending_proposal,
        );
        chain_client.options_mut().message_policy = MessagePolicy::new(
            self.blanket_message_policy,
            self.restrict_chain_ids_to.clone(),
        );
        chain_client
    }

    pub fn make_node_provider(&self) -> NodeProvider {
        NodeProvider::new(self.make_node_options())
    }

    fn make_node_options(&self) -> NodeOptions {
        NodeOptions {
            send_timeout: self.send_timeout,
            recv_timeout: self.recv_timeout,
            retry_delay: self.retry_delay,
            max_retries: self.max_retries,
        }
    }

    pub async fn save_wallet(&mut self) -> Result<(), Error> {
        self.wallet
            .persist()
            .await
            .map_err(|e| error::Inner::Persistence(Box::new(e)).into())
    }

    async fn update_wallet_from_client(
        &mut self,
        client: &ChainClient<NodeProvider, S>,
    ) -> Result<(), Error> {
        self.wallet.as_mut().update_from_state(client).await;
        self.save_wallet().await?;
        Ok(())
    }

    pub async fn update_and_save_wallet(
        &mut self,
        client: &ChainClient<NodeProvider, S>,
    ) -> Result<(), Error> {
        self.update_wallet_from_client(client).await?;
        self.save_wallet().await
    }

    /// Remembers the new chain and private key (if any) in the wallet.
    pub async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<AccountSecretKey>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain_internal(chain_id, key_pair, timestamp)
            .await
    }

    async fn update_wallet_for_new_chain_internal(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<AccountSecretKey>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        if self.wallet.get(chain_id).is_none() {
            self.mutate_wallet(|w| {
                w.insert(UserChain {
                    chain_id,
                    key_pair: key_pair.as_ref().map(|kp| kp.copy()),
                    block_hash: None,
                    timestamp,
                    next_block_height: BlockHeight::ZERO,
                    pending_proposal: None,
                })
            })
            .await?;
        }

        Ok(())
    }

    pub async fn process_inbox(
        &mut self,
        chain_client: &ChainClient<NodeProvider, S>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, Error> {
        let mut certificates = Vec::new();
        // Try processing the inbox optimistically without waiting for validator notifications.
        let (new_certificates, maybe_timeout) = {
            chain_client.synchronize_from_validators().await?;
            let result = chain_client.process_inbox_without_prepare().await;
            self.update_wallet_from_client(chain_client).await?;
            if result.is_err() {
                self.save_wallet().await?;
            }
            result?
        };
        certificates.extend(new_certificates);
        if maybe_timeout.is_none() {
            self.save_wallet().await?;
            return Ok(certificates);
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = chain_client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            let (new_certificates, maybe_timeout) = {
                let result = chain_client.process_inbox().await;
                self.update_wallet_from_client(chain_client).await?;
                if result.is_err() {
                    self.save_wallet().await?;
                }
                result?
            };
            certificates.extend(new_certificates);
            if let Some(timestamp) = maybe_timeout {
                util::wait_for_next_round(&mut notification_stream, timestamp).await
            } else {
                self.save_wallet().await?;
                return Ok(certificates);
            }
        }
    }

    /// Applies the given function to the chain client.
    ///
    /// Updates the wallet regardless of the outcome. As long as the function returns a round
    /// timeout, it will wait and retry.
    pub async fn apply_client_command<E, F, Fut, T>(
        &mut self,
        client: &ChainClient<NodeProvider, S>,
        mut f: F,
    ) -> Result<T, Error>
    where
        F: FnMut(&ChainClient<NodeProvider, S>) -> Fut,
        Fut: Future<Output = Result<ClientOutcome<T>, E>>,
        Error: From<E>,
    {
        client.prepare_chain().await?;
        // Try applying f optimistically without validator notifications. Return if committed.
        let result = f(client).await;
        self.update_and_save_wallet(client).await?;
        if let ClientOutcome::Committed(t) = result? {
            return Ok(t);
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            // Try applying f. Return if committed.
            client.prepare_chain().await?;
            let result = f(client).await;
            self.update_and_save_wallet(client).await?;
            let timeout = match result? {
                ClientOutcome::Committed(t) => return Ok(t),
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            // Otherwise wait and try again in the next round.
            util::wait_for_next_round(&mut notification_stream, timeout).await;
        }
    }

    pub async fn change_ownership(
        &mut self,
        chain_id: Option<ChainId>,
        ownership_config: ChainOwnershipConfig,
    ) -> Result<(), Error> {
        let chain_id = chain_id.unwrap_or_else(|| self.default_chain());
        let chain_client = self.make_chain_client(chain_id)?;
        info!("Changing ownership for chain {}", chain_id);
        let time_start = Instant::now();
        let ownership = ChainOwnership::try_from(ownership_config)?;

        let certificate = self
            .apply_client_command(&chain_client, |chain_client| {
                let ownership = ownership.clone();
                let chain_client = chain_client.clone();
                async move {
                    chain_client
                        .change_ownership(ownership)
                        .await
                        .map_err(Error::from)
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

#[cfg(feature = "fs")]
impl<S, W> ClientContext<S, W>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
{
    pub async fn publish_bytecode(
        &mut self,
        chain_client: &ChainClient<NodeProvider, S>,
        contract: PathBuf,
        service: PathBuf,
    ) -> Result<BytecodeId, Error> {
        info!("Loading bytecode files");
        let contract_bytecode = Bytecode::load_from_file(&contract)
            .await
            .with_context(|| format!("failed to load contract bytecode from {:?}", &contract))?;
        let service_bytecode = Bytecode::load_from_file(&service)
            .await
            .with_context(|| format!("failed to load service bytecode from {:?}", &service))?;

        info!("Publishing bytecode");
        let (contract_blob, service_blob, bytecode_id) =
            create_bytecode_blobs(contract_bytecode, service_bytecode).await;
        let (bytecode_id, _) = self
            .apply_client_command(chain_client, |chain_client| {
                let contract_blob = contract_blob.clone();
                let service_blob = service_blob.clone();
                let chain_client = chain_client.clone();
                async move {
                    chain_client
                        .publish_bytecode_blobs(contract_blob, service_blob, bytecode_id)
                        .await
                        .context("Failed to publish bytecode")
                }
            })
            .await?;

        info!("{}", "Bytecode published successfully!");

        info!("Synchronizing client and processing inbox");
        self.process_inbox(chain_client).await?;
        Ok(bytecode_id)
    }

    pub async fn publish_data_blob(
        &mut self,
        chain_client: &ChainClient<NodeProvider, S>,
        blob_path: PathBuf,
    ) -> Result<CryptoHash, Error> {
        info!("Loading data blob file");
        let blob_bytes = fs::read(&blob_path).context(format!(
            "failed to load data blob bytes from {:?}",
            &blob_path
        ))?;

        info!("Publishing data blob");
        self.apply_client_command(chain_client, |chain_client| {
            let blob_bytes = blob_bytes.clone();
            let chain_client = chain_client.clone();
            async move {
                chain_client
                    .publish_data_blob(blob_bytes)
                    .await
                    .context("Failed to publish data blob")
            }
        })
        .await?;

        info!("{}", "Data blob published successfully!");
        Ok(CryptoHash::new(&BlobContent::new_data(blob_bytes)))
    }

    // TODO(#2490): Consider removing or renaming this.
    pub async fn read_data_blob(
        &mut self,
        chain_client: &ChainClient<NodeProvider, S>,
        hash: CryptoHash,
    ) -> Result<(), Error> {
        info!("Verifying data blob");
        self.apply_client_command(chain_client, |chain_client| {
            let chain_client = chain_client.clone();
            async move {
                chain_client
                    .read_data_blob(hash)
                    .await
                    .context("Failed to verify data blob")
            }
        })
        .await?;

        info!("{}", "Data blob verified successfully!");
        Ok(())
    }
}

#[cfg(feature = "benchmark")]
impl<S, W> ClientContext<S, W>
where
    S: Storage + Clone + Send + Sync + 'static,
    W: Persist<Target = Wallet>,
{
    pub async fn prepare_for_benchmark(
        &mut self,
        num_chains: usize,
        transactions_per_block: usize,
        tokens_per_chain: Amount,
        fungible_application_id: Option<ApplicationId>,
    ) -> Result<
        (
            HashMap<ChainId, ChainClient<NodeProvider, S>>,
            Epoch,
            Vec<(ChainId, Vec<Operation>, AccountSecretKey)>,
            Committee,
        ),
        Error,
    > {
        let start = Instant::now();
        // Below all block proposals are supposed to succeed without retries, we
        // must make sure that all incoming payments have been accepted on-chain
        // and that no validator is missing user certificates.
        self.process_inboxes_and_force_validator_updates().await;
        info!(
            "Processed inboxes and forced validator updates in {} ms",
            start.elapsed().as_millis()
        );

        let start = Instant::now();
        let (key_pairs, chain_clients) = self
            .make_benchmark_chains(num_chains, tokens_per_chain)
            .await?;
        info!(
            "Got {} chains in {} ms",
            key_pairs.len(),
            start.elapsed().as_millis()
        );

        if let Some(id) = fungible_application_id {
            let start = Instant::now();
            self.supply_fungible_tokens(&key_pairs, id, &chain_clients)
                .await?;
            info!(
                "Supplied fungible tokens in {} ms",
                start.elapsed().as_millis()
            );
        }

        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let default_chain_client = self.make_chain_client(default_chain_id)?;
        let (epoch, mut committees) = default_chain_client
            .epoch_and_committees(default_chain_id)
            .await?;
        let epoch = epoch.expect("default chain should have an epoch");
        let committee = committees
            .remove(&epoch)
            .expect("current epoch should have a committee");
        let blocks_infos = Benchmark::<S>::make_benchmark_block_info(
            key_pairs,
            transactions_per_block,
            fungible_application_id,
        );

        Ok((chain_clients, epoch, blocks_infos, committee))
    }

    async fn process_inboxes_and_force_validator_updates(&mut self) {
        let chain_clients = self
            .wallet
            .owned_chain_ids()
            .iter()
            .map(|chain_id| {
                self.make_chain_client(*chain_id)
                    .expect("chains in the wallet must exist")
            })
            .collect::<Vec<_>>();

        let mut join_set = task::JoinSet::new();
        for chain_client in chain_clients {
            join_set.spawn(async move {
                Self::process_inbox_without_updating_wallet(&chain_client)
                    .await
                    .expect("Processing inbox should not fail!");
                chain_client
            });
        }

        let chain_clients = join_set.join_all().await;
        for chain_client in &chain_clients {
            self.update_wallet_from_client(chain_client).await.unwrap();
        }
    }

    async fn process_inbox_without_updating_wallet(
        chain_client: &ChainClient<NodeProvider, S>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, Error> {
        // Try processing the inbox optimistically without waiting for validator notifications.
        chain_client.synchronize_from_validators().await?;
        let (certificates, maybe_timeout) = chain_client.process_inbox_without_prepare().await?;
        assert!(
            maybe_timeout.is_none(),
            "Should not timeout within benchmark!"
        );

        Ok(certificates)
    }

    /// Creates chains, and returns a map of exactly `num_chains` chain IDs
    /// with key pairs, as well as a map of the chain clients.
    async fn make_benchmark_chains(
        &mut self,
        num_chains: usize,
        balance: Amount,
    ) -> Result<
        (
            HashMap<ChainId, AccountSecretKey>,
            HashMap<ChainId, ChainClient<NodeProvider, S>>,
        ),
        Error,
    > {
        let start = Instant::now();
        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let operations_per_block = 900; // Over this we seem to hit the block size limits.

        let mut key_pairs = Vec::new();
        for _ in (0..num_chains).step_by(operations_per_block) {
            key_pairs.push(self.wallet.generate_key_pair());
        }
        let mut key_pairs_iter = key_pairs.into_iter();
        let admin_id = self.wallet.genesis_admin_chain();
        let default_chain_client = self.make_chain_client(default_chain_id)?;

        let mut chain_clients = HashMap::new();
        let mut benchmark_chains = HashMap::new();
        for i in (0..num_chains).step_by(operations_per_block) {
            let num_new_chains = operations_per_block.min(num_chains - i);
            let key_pair = key_pairs_iter.next().unwrap();

            let certificate = Self::execute_open_chains_operations(
                num_new_chains,
                &default_chain_client,
                balance,
                &key_pair,
                admin_id,
            )
            .await?;
            info!("Block executed successfully");

            let block = certificate.block();
            for i in 0..num_new_chains {
                let message_id = block
                    .message_id_for_operation(i, OPEN_CHAIN_MESSAGE_INDEX)
                    .expect("failed to create new chain");
                let chain_id = ChainId::child(message_id);
                benchmark_chains.insert(chain_id, key_pair.copy());
                self.client.track_chain(chain_id);

                let chain_client = self.make_chain_client_internal(
                    chain_id,
                    vec![key_pair.copy()],
                    None,
                    certificate.block().header.timestamp,
                    BlockHeight::ZERO,
                    None,
                );
                chain_client.process_inbox().await?;
                chain_clients.insert(chain_id, chain_client);
            }
        }

        info!(
            "Created {} chains in {} ms",
            num_chains,
            start.elapsed().as_millis()
        );

        info!("Updating wallet from client");
        self.update_wallet_from_client(&default_chain_client)
            .await?;
        info!("Retrying pending outgoing messages");
        default_chain_client
            .retry_pending_outgoing_messages()
            .await
            .context("outgoing messages to create the new chains should be delivered")?;
        info!("Processing inbox");
        default_chain_client.process_inbox().await?;

        Ok((benchmark_chains, chain_clients))
    }

    async fn execute_open_chains_operations(
        num_new_chains: usize,
        chain_client: &ChainClient<NodeProvider, S>,
        balance: Amount,
        key_pair: &AccountSecretKey,
        admin_id: ChainId,
    ) -> Result<ConfirmedBlockCertificate, Error> {
        let chain_id = chain_client.chain_id();
        let (epoch, committees) = chain_client.epoch_and_committees(chain_id).await?;
        let epoch = epoch.expect("default chain should be active");
        let config = OpenChainConfig {
            ownership: ChainOwnership::single_super(key_pair.public().into()),
            committees,
            admin_id,
            epoch,
            balance,
            application_permissions: Default::default(),
        };
        let operations = iter::repeat(Operation::System(SystemOperation::OpenChain(config)))
            .take(num_new_chains)
            .collect();
        info!("Executing {} OpenChain operations", num_new_chains);
        Ok(chain_client
            .execute_operations(operations, vec![])
            .await?
            .expect("should execute block with OpenChain operations"))
    }

    /// Supplies fungible tokens to the chains.
    async fn supply_fungible_tokens(
        &mut self,
        key_pairs: &HashMap<ChainId, AccountSecretKey>,
        application_id: ApplicationId,
        chain_clients: &HashMap<ChainId, ChainClient<NodeProvider, S>>,
    ) -> Result<(), Error> {
        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let default_key = self
            .wallet
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
                Benchmark::<S>::fungible_transfer(
                    application_id,
                    *chain_id,
                    default_key,
                    key_pair.public(),
                    amount,
                )
            })
            .collect();
        let chain_client = self.make_chain_client(default_chain_id)?;
        // Put at most 1000 fungible token operations in each block.
        for operation_chunk in operations.chunks(1000) {
            chain_client
                .execute_operations(operation_chunk.to_vec(), vec![])
                .await?
                .expect("should execute block with Transfer operations");
        }
        self.update_wallet_from_client(&chain_client).await?;
        // Make sure all chains have registered the application now.
        let mut join_set = task::JoinSet::new();
        for (chain_id, chain_client) in chain_clients.iter() {
            let chain_id = *chain_id;
            let chain_client = chain_client.clone();
            join_set.spawn(async move {
                let mut delay_ms = 0;
                let mut total_delay_ms = 0;
                loop {
                    linera_base::time::timer::sleep(Duration::from_millis(delay_ms)).await;
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
                        return Ok::<_, Error>(());
                    }

                    total_delay_ms += delay_ms;
                    // If we've been waiting already for more than 10 seconds, give up.
                    if total_delay_ms > 10_000 {
                        break;
                    }

                    if delay_ms == 0 {
                        delay_ms = 100;
                    } else {
                        delay_ms *= 2;
                    }
                }
                panic!("Could not instantiate application on chain {chain_id:?}");
            });
        }
        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }
}
