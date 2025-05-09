// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::num::NonZeroUsize;
use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use futures::Future;
use linera_base::{
    crypto::{CryptoHash, Signer, ValidatorPublicKey},
    data_types::{BlockHeight, ChainDescription, Timestamp},
    identifiers::{Account, AccountOwner, BlobId, BlobType, ChainId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{
    client::{BlanketMessagePolicy, ChainClient, Client, MessagePolicy, PendingProposal},
    data_types::{ChainInfoQuery, ClientOutcome},
    join_set_ext::JoinSet,
    node::{CrossChainMessageDelivery, ValidatorNode},
    Environment, JoinSetExt as _,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_storage::Storage;
use linera_version::VersionInfo;
use linera_views::views::ViewError;
use thiserror_context::Context;
use tracing::{debug, info};
#[cfg(feature = "benchmark")]
use {
    crate::benchmark::{Benchmark, BenchmarkError},
    futures::{stream, StreamExt, TryStreamExt},
    linera_base::{
        crypto::AccountPublicKey,
        data_types::{Amount, Epoch},
        identifiers::ApplicationId,
    },
    linera_core::client::ChainClientError,
    linera_execution::{
        committee::Committee,
        system::{OpenChainConfig, SystemOperation},
        Operation,
    },
    std::{collections::HashMap, iter},
    tokio::task,
};
#[cfg(feature = "fs")]
use {
    linera_base::{
        data_types::{BlobContent, Bytecode},
        identifiers::ModuleId,
        vm::VmRuntime,
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
    client_options::{ChainOwnershipConfig, ClientContextOptions},
    config::WalletState,
    error, util,
    wallet::{UserChain, Wallet},
    Error,
};

pub struct ClientContext<Env: Environment, W> {
    pub wallet: WalletState<W>,
    pub client: Arc<Client<Env>>,
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
impl<Env: Environment, W> chain_listener::ClientContext for ClientContext<Env, W>
where
    W: Persist<Target = Wallet> + Sync + 'static,
{
    type Environment = Env;

    fn wallet(&self) -> &Wallet {
        &self.wallet
    }

    fn storage(&self) -> &Env::Storage {
        self.client.storage_client()
    }

    async fn make_chain_client(&self, chain_id: ChainId) -> Result<ChainClient<Env>, Error> {
        self.make_chain_client(chain_id).await
    }

    fn client(&self) -> &Client<Env> {
        &self.client
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain(chain_id, owner, timestamp)
            .await
    }

    async fn update_wallet(&mut self, client: &ChainClient<Env>) -> Result<(), Error> {
        self.update_wallet_from_client(client).await
    }
}

impl<S, W> ClientContext<linera_core::environment::Impl<S, NodeProvider>, W>
where
    S: linera_core::environment::Storage,
    W: Persist<Target = Wallet>,
{
    pub fn new(
        storage: S,
        options: ClientContextOptions,
        wallet: W,
        signer: Box<dyn Signer>,
    ) -> Self {
        let node_provider = NodeProvider::new(NodeOptions {
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
        });
        let delivery = CrossChainMessageDelivery::new(options.wait_for_outgoing_messages);
        let chain_ids = wallet.chain_ids();
        let name = match chain_ids.len() {
            0 => "Client node".to_string(),
            1 => format!("Client node for {:.8}", chain_ids[0]),
            n => format!("Client node for {:.8} and {} others", chain_ids[0], n - 1),
        };
        let client = Client::new(
            linera_core::environment::Impl {
                network: node_provider,
                storage,
            },
            signer,
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
    pub fn new_test_client_context(storage: S, wallet: W, signer: Box<dyn Signer>) -> Self {
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
        let delivery = CrossChainMessageDelivery::new(true);
        let chain_ids = wallet.chain_ids();
        let name = match chain_ids.len() {
            0 => "Client node".to_string(),
            1 => format!("Client node for {:.8}", chain_ids[0]),
            n => format!("Client node for {:.8} and {} others", chain_ids[0], n - 1),
        };
        let client = Client::new(
            linera_core::environment::Impl {
                storage,
                network: NodeProvider::new(node_options),
            },
            signer,
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
}

impl<Env: Environment, W> ClientContext<Env, W>
where
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

    pub fn non_admin_chain(&self) -> ChainId {
        self.wallet
            .non_admin_chain()
            .expect("No non-admin chain specified in wallet with no non-admin chain")
    }

    pub async fn make_chain_client(&self, chain_id: ChainId) -> Result<ChainClient<Env>, Error> {
        // We only create clients for chains we have in the wallet, or for the admin chain.
        let chain: UserChain = match self.wallet.get(chain_id) {
            Some(chain) => chain.clone(),
            None => UserChain::make_other(chain_id, Timestamp::from(0)),
        };

        self.make_chain_client_internal(
            chain_id,
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_proposal,
            chain.owner,
        )
        .await
    }

    async fn make_chain_client_internal(
        &self,
        chain_id: ChainId,
        block_hash: Option<CryptoHash>,
        timestamp: Timestamp,
        next_block_height: BlockHeight,
        pending_proposal: Option<PendingProposal>,
        preferred_owner: Option<AccountOwner>,
    ) -> Result<ChainClient<Env>, Error> {
        let mut chain_client = self
            .client
            .create_chain_client(
                chain_id,
                self.wallet.genesis_admin_chain(),
                block_hash,
                timestamp,
                next_block_height,
                pending_proposal,
                preferred_owner,
            )
            .await?;
        chain_client.options_mut().message_policy = MessagePolicy::new(
            self.blanket_message_policy,
            self.restrict_chain_ids_to.clone(),
        );
        Ok(chain_client)
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

    pub async fn update_wallet_from_client<Env_: Environment>(
        &mut self,
        client: &ChainClient<Env_>,
    ) -> Result<(), Error> {
        self.wallet.as_mut().update_from_state(client);
        self.save_wallet().await
    }

    /// Remembers the new chain and its owner (if any) in the wallet.
    pub async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        if self.wallet.get(chain_id).is_none() {
            self.mutate_wallet(|w| {
                w.insert(UserChain {
                    chain_id,
                    owner,
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
        chain_client: &ChainClient<Env>,
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

    pub async fn assign_new_chain_to_key(
        &mut self,
        chain_id: ChainId,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        self.client.track_chain(chain_id);
        let chain_description_blob_id = BlobId::new(chain_id.0, BlobType::ChainDescription);
        let chain_description_blob = match self
            .client
            .storage_client()
            .read_blob(chain_description_blob_id)
            .await
        {
            Ok(blob) => blob,
            Err(ViewError::BlobsNotFound(blob_ids)) if blob_ids == [chain_description_blob_id] => {
                // we're missing the blob describing the chain we're assigning - try to
                // get it
                self.client
                    .ensure_has_chain_description(chain_id, self.wallet.genesis_admin_chain())
                    .await?
            }
            Err(err) => {
                return Err(err.into());
            }
        };
        let chain_description: ChainDescription =
            bcs::from_bytes(&chain_description_blob.into_bytes())
                .map_err(|e| error::Inner::Persistence(Box::new(e)))?;

        let config = chain_description.config();

        if !config.ownership.verify_owner(&owner) {
            tracing::error!(
                "The chain with the ID returned by the faucet is not owned by you. \
                Please make sure you are connecting to a genuine faucet."
            );
            return Err(error::Inner::ChainOwnership.into());
        }

        self.wallet_mut()
            .mutate(|w| w.assign_new_chain_to_owner(owner, chain_id, chain_description.timestamp()))
            .await
            .map_err(|e| error::Inner::Persistence(Box::new(e)))?
            .context("assigning new chain")?;
        Ok(())
    }

    /// Applies the given function to the chain client.
    ///
    /// Updates the wallet regardless of the outcome. As long as the function returns a round
    /// timeout, it will wait and retry.
    pub async fn apply_client_command<E, F, Fut, T>(
        &mut self,
        client: &ChainClient<Env>,
        mut f: F,
    ) -> Result<T, Error>
    where
        F: FnMut(&ChainClient<Env>) -> Fut,
        Fut: Future<Output = Result<ClientOutcome<T>, E>>,
        Error: From<E>,
    {
        client.prepare_chain().await?;
        // Try applying f optimistically without validator notifications. Return if committed.
        let result = f(client).await;
        self.update_wallet_from_client(client).await?;
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
            self.update_wallet_from_client(client).await?;
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
        let chain_client = self.make_chain_client(chain_id).await?;
        info!(
            ?ownership_config, %chain_id, preferred_owner=?chain_client.preferred_owner(),
            "Changing ownership of a chain"
        );
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

    pub async fn set_preferred_owner(
        &mut self,
        chain_id: Option<ChainId>,
        preferred_owner: AccountOwner,
    ) -> Result<(), Error> {
        let chain_id = chain_id.unwrap_or_else(|| self.default_chain());
        let mut chain_client = self.make_chain_client(chain_id).await?;
        let old_owner = chain_client.preferred_owner();
        info!(%chain_id, ?old_owner, %preferred_owner, "Changing preferred owner for chain");
        chain_client.set_preferred_owner(preferred_owner);
        self.update_wallet_from_client(&chain_client).await?;
        info!("New preferred owner set");
        Ok(())
    }

    pub async fn check_compatible_version_info(
        &self,
        address: &str,
        node: &impl ValidatorNode,
    ) -> Result<VersionInfo, Error> {
        match node.get_version_info().await {
            Ok(version_info) if version_info.is_compatible_with(&linera_version::VERSION_INFO) => {
                info!(
                    "Version information for validator {address}: {}",
                    version_info
                );
                Ok(version_info)
            }
            Ok(version_info) => Err(error::Inner::UnexpectedVersionInfo {
                remote: Box::new(version_info),
                local: Box::new(linera_version::VERSION_INFO.clone()),
            }
            .into()),
            Err(error) => Err(error::Inner::UnavailableVersionInfo {
                address: address.to_string(),
                error: Box::new(error),
            }
            .into()),
        }
    }

    pub async fn check_matching_network_description(
        &self,
        address: &str,
        node: &impl ValidatorNode,
    ) -> Result<CryptoHash, Error> {
        let network_description = self.wallet().genesis_config().network_description();
        match node.get_network_description().await {
            Ok(description) => {
                if description == network_description {
                    Ok(description.genesis_config_hash)
                } else {
                    Err(error::Inner::UnexpectedNetworkDescription {
                        remote: Box::new(description),
                        local: Box::new(network_description),
                    }
                    .into())
                }
            }
            Err(error) => Err(error::Inner::UnavailableNetworkDescription {
                address: address.to_string(),
                error: Box::new(error),
            }
            .into()),
        }
    }

    pub async fn check_validator_chain_info_response(
        &self,
        public_key: Option<&ValidatorPublicKey>,
        address: &str,
        node: &impl ValidatorNode,
        chain_id: ChainId,
    ) -> Result<(), Error> {
        let query = ChainInfoQuery::new(chain_id);
        match node.handle_chain_info_query(query).await {
            Ok(response) => {
                info!(
                    "Validator {address} sees chain {chain_id} at block height {} and epoch {:?}",
                    response.info.next_block_height, response.info.epoch,
                );
                if let Some(public_key) = public_key {
                    if response.check(public_key).is_ok() {
                        info!("Signature for public key {public_key} is OK.");
                    } else {
                        return Err(error::Inner::InvalidSignature {
                            public_key: *public_key,
                        }
                        .into());
                    }
                }
                Ok(())
            }
            Err(error) => Err(error::Inner::UnavailableChainInfo {
                address: address.to_string(),
                chain_id,
                error: Box::new(error),
            }
            .into()),
        }
    }
}

#[cfg(feature = "fs")]
impl<Env: Environment, W> ClientContext<Env, W>
where
    W: Persist<Target = Wallet>,
{
    pub async fn publish_module(
        &mut self,
        chain_client: &ChainClient<Env>,
        contract: PathBuf,
        service: PathBuf,
        vm_runtime: VmRuntime,
    ) -> Result<ModuleId, Error> {
        info!("Loading bytecode files");
        let contract_bytecode = Bytecode::load_from_file(&contract)
            .await
            .with_context(|| format!("failed to load contract bytecode from {:?}", &contract))?;
        let service_bytecode = Bytecode::load_from_file(&service)
            .await
            .with_context(|| format!("failed to load service bytecode from {:?}", &service))?;

        info!("Publishing module");
        let (blobs, module_id) =
            create_bytecode_blobs(contract_bytecode, service_bytecode, vm_runtime).await;
        let (module_id, _) = self
            .apply_client_command(chain_client, |chain_client| {
                let blobs = blobs.clone();
                let chain_client = chain_client.clone();
                async move {
                    chain_client
                        .publish_module_blobs(blobs, module_id)
                        .await
                        .context("Failed to publish module")
                }
            })
            .await?;

        info!("{}", "Module published successfully!");

        info!("Synchronizing client and processing inbox");
        self.process_inbox(chain_client).await?;
        Ok(module_id)
    }

    pub async fn publish_data_blob(
        &mut self,
        chain_client: &ChainClient<Env>,
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
        chain_client: &ChainClient<Env>,
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
impl<Env: Environment, W> ClientContext<Env, W>
where
    W: Persist<Target = Wallet>,
{
    pub async fn prepare_for_benchmark(
        &mut self,
        num_chains: usize,
        transactions_per_block: usize,
        tokens_per_chain: Amount,
        fungible_application_id: Option<ApplicationId>,
        pub_keys: Vec<AccountPublicKey>,
    ) -> Result<
        (
            HashMap<ChainId, ChainClient<Env>>,
            Epoch,
            Vec<(ChainId, Vec<Operation>, AccountOwner)>,
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
            .make_benchmark_chains(num_chains, tokens_per_chain, pub_keys)
            .await?;
        info!(
            "Got {} chains in {} ms",
            key_pairs.len(),
            start.elapsed().as_millis()
        );

        if let Some(id) = fungible_application_id {
            let start = Instant::now();
            self.supply_fungible_tokens(&key_pairs, id).await?;
            info!(
                "Supplied fungible tokens in {} ms",
                start.elapsed().as_millis()
            );
        }

        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let default_chain_client = self.make_chain_client(default_chain_id).await?;
        let (epoch, mut committees) = default_chain_client
            .epoch_and_committees(default_chain_id)
            .await?;
        let epoch = epoch.expect("default chain should have an epoch");
        let committee = committees
            .remove(&epoch)
            .expect("current epoch should have a committee");
        let blocks_infos = Benchmark::<Env>::make_benchmark_block_info(
            key_pairs,
            transactions_per_block,
            fungible_application_id,
        );

        Ok((chain_clients, epoch, blocks_infos, committee))
    }

    pub async fn wrap_up_benchmark(
        &mut self,
        chain_clients: HashMap<ChainId, ChainClient<Env>>,
        close_chains: bool,
        wrap_up_max_in_flight: usize,
    ) -> Result<(), Error> {
        if close_chains {
            info!("Closing chains...");
            let stream = stream::iter(chain_clients.values().cloned())
                .map(|chain_client| async move {
                    Benchmark::<Env>::close_benchmark_chain(&chain_client).await?;
                    info!("Closed chain {:?}", chain_client.chain_id());
                    Ok::<(), BenchmarkError>(())
                })
                .buffer_unordered(wrap_up_max_in_flight);
            stream.try_collect::<Vec<_>>().await?;
        } else {
            info!("Processing inbox for all chains...");
            let stream = stream::iter(chain_clients.values().cloned())
                .map(|chain_client| async move {
                    chain_client.process_inbox().await?;
                    info!("Processed inbox for chain {:?}", chain_client.chain_id());
                    Ok::<(), ChainClientError>(())
                })
                .buffer_unordered(wrap_up_max_in_flight);
            stream.try_collect::<Vec<_>>().await?;

            info!("Updating wallet from chain clients...");
            for chain_client in chain_clients.values() {
                self.wallet.as_mut().update_from_state(chain_client);
            }
            self.save_wallet().await?;
        }

        Ok(())
    }

    async fn process_inboxes_and_force_validator_updates(&mut self) {
        let mut chain_clients = vec![];
        for chain_id in &self.wallet.owned_chain_ids() {
            chain_clients.push(
                self.make_chain_client(*chain_id)
                    .await
                    .expect("chains in the wallet must exist"),
            );
        }

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
        chain_client: &ChainClient<Env>,
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

    /// Creates chains if necessary, and returns a map of exactly `num_chains` chain IDs
    /// with key pairs, as well as a map of the chain clients.
    async fn make_benchmark_chains(
        &mut self,
        num_chains: usize,
        balance: Amount,
        pub_keys: Vec<AccountPublicKey>,
    ) -> Result<
        (
            HashMap<ChainId, AccountOwner>,
            HashMap<ChainId, ChainClient<Env>>,
        ),
        Error,
    > {
        let mut benchmark_chains = HashMap::new();
        let mut chain_clients = HashMap::new();
        let start = Instant::now();
        for chain_id in self.wallet.owned_chain_ids() {
            if benchmark_chains.len() == num_chains {
                break;
            }
            // This should never panic, because `owned_chain_ids` only returns the owned chains that
            // we have a key pair for.
            let owner = self
                .wallet
                .get(chain_id)
                .and_then(|chain| chain.owner)
                .unwrap();
            let chain_client = self.make_chain_client(chain_id).await?;
            let ownership = chain_client.chain_info().await?.manager.ownership;
            if !ownership.owners.is_empty() || ownership.super_owners.len() != 1 {
                continue;
            }
            benchmark_chains.insert(chain_client.chain_id(), owner);
            chain_client.process_inbox().await?;
            chain_clients.insert(chain_id, chain_client);
        }
        info!(
            "Got {} chains from the wallet in {} ms",
            benchmark_chains.len(),
            start.elapsed().as_millis()
        );

        let chains_from_wallet = benchmark_chains.len();
        let num_chains_to_create = num_chains - chains_from_wallet;

        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let operations_per_block = 900; // Over this we seem to hit the block size limits.

        let mut pub_keys_iter = pub_keys.into_iter().take(num_chains_to_create);
        let default_chain_client = self.make_chain_client(default_chain_id).await?;

        for i in (0..num_chains_to_create).step_by(operations_per_block) {
            let num_new_chains = operations_per_block.min(num_chains_to_create - i);
            let pub_key = pub_keys_iter.next().unwrap();

            let certificate = Self::execute_open_chains_operations(
                num_new_chains,
                &default_chain_client,
                balance,
                pub_key.into(),
            )
            .await?;
            info!("Block executed successfully");

            let block = certificate.block();
            for i in 0..num_new_chains {
                let chain_id = block.body.blobs[i]
                    .iter()
                    .find(|blob| blob.id().blob_type == BlobType::ChainDescription)
                    .map(|blob| ChainId(blob.id().hash))
                    .expect("failed to create a new chain");
                benchmark_chains.insert(chain_id, pub_key.into());
                self.client.track_chain(chain_id);

                let mut chain_client = self
                    .make_chain_client_internal(
                        chain_id,
                        None,
                        certificate.block().header.timestamp,
                        BlockHeight::ZERO,
                        None,
                        Some(pub_key.into()),
                    )
                    .await?;
                chain_client.set_preferred_owner(pub_key.into());
                chain_client.process_inbox().await?;
                chain_clients.insert(chain_id, chain_client);
            }
        }

        if num_chains_to_create > 0 {
            info!(
                "Created {} chains in {} ms",
                num_chains_to_create,
                start.elapsed().as_millis()
            );
        }

        info!("Updating wallet from client");
        self.update_wallet_from_client(&default_chain_client)
            .await?;
        info!("Retrying pending outgoing messages");
        default_chain_client
            .retry_pending_outgoing_messages()
            .await
            .context("outgoing messages to create the new chains should be delivered")?;
        info!("Processing default chain inbox");
        default_chain_client.process_inbox().await?;

        Ok((benchmark_chains, chain_clients))
    }

    async fn execute_open_chains_operations(
        num_new_chains: usize,
        chain_client: &ChainClient<Env>,
        balance: Amount,
        owner: AccountOwner,
    ) -> Result<ConfirmedBlockCertificate, Error> {
        let config = OpenChainConfig {
            ownership: ChainOwnership::single_super(owner),
            balance,
            application_permissions: Default::default(),
        };
        let operations = iter::repeat_n(
            Operation::system(SystemOperation::OpenChain(config)),
            num_new_chains,
        )
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
        key_pairs: &HashMap<ChainId, AccountOwner>,
        application_id: ApplicationId,
    ) -> Result<(), Error> {
        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let default_key = self.wallet.get(default_chain_id).unwrap().owner.unwrap();
        let amount = Amount::from(1_000_000);
        let operations: Vec<_> = key_pairs
            .iter()
            .map(|(chain_id, owner)| {
                Benchmark::<Env>::fungible_transfer(
                    application_id,
                    *chain_id,
                    default_key,
                    *owner,
                    amount,
                )
            })
            .collect();
        let chain_client = self.make_chain_client(default_chain_id).await?;
        // Put at most 1000 fungible token operations in each block.
        for operation_chunk in operations.chunks(1000) {
            chain_client
                .execute_operations(operation_chunk.to_vec(), vec![])
                .await?
                .expect("should execute block with Transfer operations");
        }
        self.update_wallet_from_client(&chain_client).await?;

        Ok(())
    }
}
