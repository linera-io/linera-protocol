// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use futures::{Future, StreamExt as _, TryStreamExt as _};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Epoch, Timestamp},
    identifiers::{Account, AccountOwner, ChainId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
    util::future::FutureSyncExt as _,
};
use linera_chain::{manager::LockingBlock, types::ConfirmedBlockCertificate};
use linera_core::{
    client::{chain_client, ChainClient, Client, ListeningMode},
    data_types::{ChainInfo, ChainInfoQuery, ClientOutcome},
    join_set_ext::JoinSet,
    node::ValidatorNode,
    wallet, Environment, JoinSetExt as _, Wallet as _,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_version::VersionInfo;
use thiserror_context::Context;
use tracing::{debug, info, warn};
#[cfg(not(web))]
use {
    crate::{
        benchmark::{Benchmark, BenchmarkError},
        client_metrics::ClientMetrics,
    },
    futures::stream,
    linera_base::{
        crypto::AccountPublicKey,
        data_types::{Amount, BlockHeight},
        identifiers::{ApplicationId, BlobType},
    },
    linera_execution::{
        system::{OpenChainConfig, SystemOperation},
        Operation,
    },
    std::{collections::HashSet, iter, path::Path},
    tokio::{sync::mpsc, task},
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

use crate::{
    chain_listener::{self, ClientContext as _},
    client_options::{ChainOwnershipConfig, Options},
    config::GenesisConfig,
    error, util, Error,
};

/// Results from querying a validator about version, network description, and chain info.
pub struct ValidatorQueryResults {
    /// The validator's version information.
    pub version_info: Result<VersionInfo, Error>,
    /// The validator's genesis config hash.
    pub genesis_config_hash: Result<CryptoHash, Error>,
    /// The validator's chain info (if valid and signature check passed).
    pub chain_info: Result<ChainInfo, Error>,
}

impl ValidatorQueryResults {
    /// Returns a vector of references to all errors in the query results.
    pub fn errors(&self) -> Vec<&Error> {
        let mut errors = Vec::new();
        if let Err(e) = &self.version_info {
            errors.push(e);
        }
        if let Err(e) = &self.genesis_config_hash {
            errors.push(e);
        }
        if let Err(e) = &self.chain_info {
            errors.push(e);
        }
        errors
    }

    /// Prints validator information to stdout.
    ///
    /// Prints public key, address, and optionally weight, version info, and chain info.
    /// If `reference` is provided, only prints fields that differ from the reference.
    pub fn print(
        &self,
        public_key: Option<&ValidatorPublicKey>,
        address: Option<&str>,
        weight: Option<u64>,
        reference: Option<&ValidatorQueryResults>,
    ) {
        if let Some(key) = public_key {
            println!("Public key: {}", key);
        }
        if let Some(address) = address {
            println!("Address: {}", address);
        }
        if let Some(w) = weight {
            println!("Weight: {}", w);
        }

        let ref_version = reference.and_then(|ref_results| ref_results.version_info.as_ref().ok());
        match &self.version_info {
            Ok(version_info) => {
                if ref_version.is_none_or(|ref_v| ref_v.crate_version != version_info.crate_version)
                {
                    println!("Linera protocol: v{}", version_info.crate_version);
                }
                if ref_version.is_none_or(|ref_v| ref_v.rpc_hash != version_info.rpc_hash) {
                    println!("RPC API hash: {}", version_info.rpc_hash);
                }
                if ref_version.is_none_or(|ref_v| ref_v.graphql_hash != version_info.graphql_hash) {
                    println!("GraphQL API hash: {}", version_info.graphql_hash);
                }
                if ref_version.is_none_or(|ref_v| ref_v.wit_hash != version_info.wit_hash) {
                    println!("WIT API hash: {}", version_info.wit_hash);
                }
                if ref_version.is_none_or(|ref_v| {
                    (&ref_v.git_commit, ref_v.git_dirty)
                        != (&version_info.git_commit, version_info.git_dirty)
                }) {
                    println!(
                        "Source code: {}/tree/{}{}",
                        env!("CARGO_PKG_REPOSITORY"),
                        version_info.git_commit,
                        if version_info.git_dirty {
                            " (dirty)"
                        } else {
                            ""
                        }
                    );
                }
            }
            Err(err) => println!("Error getting version info: {err}"),
        }

        let ref_genesis_hash =
            reference.and_then(|ref_results| ref_results.genesis_config_hash.as_ref().ok());
        match &self.genesis_config_hash {
            Ok(hash) if ref_genesis_hash.is_some_and(|ref_hash| ref_hash == hash) => {}
            Ok(hash) => println!("Genesis config hash: {hash}"),
            Err(err) => println!("Error getting genesis config: {err}"),
        }

        let ref_info = reference.and_then(|ref_results| ref_results.chain_info.as_ref().ok());
        match &self.chain_info {
            Ok(info) => {
                if ref_info.is_none_or(|ref_info| info.block_hash != ref_info.block_hash) {
                    if let Some(hash) = info.block_hash {
                        println!("Block hash: {}", hash);
                    } else {
                        println!("Block hash: None");
                    }
                }
                if ref_info
                    .is_none_or(|ref_info| info.next_block_height != ref_info.next_block_height)
                {
                    println!("Next height: {}", info.next_block_height);
                }
                if ref_info.is_none_or(|ref_info| info.timestamp != ref_info.timestamp) {
                    println!("Timestamp: {}", info.timestamp);
                }
                if ref_info.is_none_or(|ref_info| info.epoch != ref_info.epoch) {
                    println!("Epoch: {}", info.epoch);
                }
                if ref_info.is_none_or(|ref_info| {
                    info.manager.current_round != ref_info.manager.current_round
                }) {
                    println!("Round: {}", info.manager.current_round);
                }
                if let Some(locking) = &info.manager.requested_locking {
                    match &**locking {
                        LockingBlock::Fast(proposal) => {
                            println!(
                                "Locking fast block from {}",
                                proposal.content.block.timestamp
                            );
                        }
                        LockingBlock::Regular(validated) => {
                            println!(
                                "Locking block {} in {} from {}",
                                validated.hash(),
                                validated.round,
                                validated.block().header.timestamp
                            );
                        }
                    }
                }
            }
            Err(err) => println!("Error getting chain info: {err}"),
        }
    }
}

pub struct ClientContext<Env: Environment> {
    pub client: Arc<Client<Env>>,
    // TODO(#5083): this doesn't really need to be stored
    pub genesis_config: crate::config::GenesisConfig,
    pub send_timeout: Duration,
    pub recv_timeout: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
    pub chain_listeners: JoinSet,
    // TODO(#5082): move this into the upstream UI layers (maybe just the CLI)
    pub default_chain: Option<ChainId>,
    #[cfg(not(web))]
    pub client_metrics: Option<ClientMetrics>,
}

impl<Env: Environment> chain_listener::ClientContext for ClientContext<Env> {
    type Environment = Env;

    fn wallet(&self) -> &Env::Wallet {
        self.client.wallet()
    }

    fn storage(&self) -> &Env::Storage {
        self.client.storage_client()
    }

    fn client(&self) -> &Arc<Client<Env>> {
        &self.client
    }

    #[cfg(not(web))]
    fn timing_sender(
        &self,
    ) -> Option<mpsc::UnboundedSender<(u64, linera_core::client::TimingType)>> {
        self.client_metrics
            .as_ref()
            .map(|metrics| metrics.timing_sender.clone())
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain(chain_id, owner, timestamp, epoch)
            .make_sync()
            .await
    }

    async fn update_wallet(&mut self, client: &ChainClient<Env>) -> Result<(), Error> {
        self.update_wallet_from_client(client).make_sync().await
    }
}

impl<S, Si, W> ClientContext<linera_core::environment::Impl<S, NodeProvider, Si, W>>
where
    S: linera_core::environment::Storage,
    Si: linera_core::environment::Signer,
    W: linera_core::environment::Wallet,
{
    // not worth refactoring this because
    // https://github.com/linera-io/linera-protocol/issues/5082
    // https://github.com/linera-io/linera-protocol/issues/5083
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        storage: S,
        wallet: W,
        signer: Si,
        options: &Options,
        default_chain: Option<ChainId>,
        genesis_config: GenesisConfig,
        block_cache_size: usize,
        execution_state_cache_size: usize,
    ) -> Result<Self, Error> {
        #[cfg(not(web))]
        let timing_config = options.to_timing_config();
        let node_provider = NodeProvider::new(NodeOptions {
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
        });
        let chain_modes: Vec<_> = wallet
            .items()
            .map_ok(|(id, chain)| {
                let mode = if chain.is_follow_only() {
                    ListeningMode::FollowChain
                } else {
                    ListeningMode::FullChain
                };
                (id, mode)
            })
            .try_collect()
            .await
            .map_err(error::Inner::wallet)?;
        let name = match chain_modes.len() {
            0 => "Client node".to_string(),
            1 => format!("Client node for {:.8}", chain_modes[0].0),
            n => format!(
                "Client node for {:.8} and {} others",
                chain_modes[0].0,
                n - 1
            ),
        };

        let client = Client::new(
            linera_core::environment::Impl {
                network: node_provider,
                storage,
                signer,
                wallet,
            },
            genesis_config.admin_chain_id(),
            options.long_lived_services,
            chain_modes,
            name,
            options.chain_worker_ttl,
            options.sender_chain_worker_ttl,
            options.to_chain_client_options(),
            block_cache_size,
            execution_state_cache_size,
            options.to_requests_scheduler_config(),
        );

        #[cfg(not(web))]
        let client_metrics = if timing_config.enabled {
            Some(ClientMetrics::new(timing_config))
        } else {
            None
        };

        Ok(ClientContext {
            client: Arc::new(client),
            default_chain,
            genesis_config,
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
            chain_listeners: JoinSet::default(),
            #[cfg(not(web))]
            client_metrics,
        })
    }
}

impl<Env: Environment> ClientContext<Env> {
    // TODO(#5084) this (and other injected dependencies) should not be re-exposed by the
    // client interface
    /// Returns a reference to the wallet.
    pub fn wallet(&self) -> &Env::Wallet {
        self.client.wallet()
    }

    /// Returns the ID of the admin chain.
    pub fn admin_chain_id(&self) -> ChainId {
        self.client.admin_chain_id()
    }

    /// Retrieve the default account. Current this is the common account of the default
    /// chain.
    pub fn default_account(&self) -> Account {
        Account::chain(self.default_chain())
    }

    /// Retrieve the default chain.
    pub fn default_chain(&self) -> ChainId {
        self.default_chain
            .expect("default chain requested but none set")
    }

    pub async fn first_non_admin_chain(&self) -> Result<ChainId, Error> {
        let admin_chain_id = self.admin_chain_id();
        std::pin::pin!(self
            .wallet()
            .chain_ids()
            .try_filter(|chain_id| futures::future::ready(*chain_id != admin_chain_id)))
        .next()
        .await
        .expect("No non-admin chain specified in wallet with no non-admin chain")
        .map_err(Error::wallet)
    }

    // TODO(#5084) this should match the `NodeProvider` from the `Environment`
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

    #[cfg(not(web))]
    pub fn client_metrics(&self) -> Option<&ClientMetrics> {
        self.client_metrics.as_ref()
    }

    pub async fn update_wallet_from_client<Env_: Environment>(
        &self,
        client: &ChainClient<Env_>,
    ) -> Result<(), Error> {
        let info = client.chain_info().await?;
        let chain_id = info.chain_id;
        let new_chain = wallet::Chain {
            pending_proposal: client.pending_proposal().clone(),
            owner: client.preferred_owner(),
            ..info.as_ref().into()
        };

        self.wallet()
            .insert(chain_id, new_chain)
            .await
            .map_err(error::Inner::wallet)?;

        Ok(())
    }

    /// Remembers the new chain and its owner (if any) in the wallet.
    pub async fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        owner: Option<AccountOwner>,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error> {
        self.wallet()
            .try_insert(
                chain_id,
                linera_core::wallet::Chain::new(owner, epoch, timestamp),
            )
            .await
            .map_err(error::Inner::wallet)?;
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
            result?
        };
        certificates.extend(new_certificates);
        if maybe_timeout.is_none() {
            return Ok(certificates);
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = chain_client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            let (new_certificates, maybe_timeout) = {
                let result = chain_client.process_inbox().await;
                self.update_wallet_from_client(chain_client).await?;
                result?
            };
            certificates.extend(new_certificates);
            if let Some(timestamp) = maybe_timeout {
                util::wait_for_next_round(&mut notification_stream, timestamp).await
            } else {
                return Ok(certificates);
            }
        }
    }

    pub async fn assign_new_chain_to_key(
        &mut self,
        chain_id: ChainId,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        self.client
            .extend_chain_mode(chain_id, ListeningMode::FullChain);
        let client = self.make_chain_client(chain_id).await?;
        let chain_description = client.get_chain_description().await?;
        let config = chain_description.config();

        if !config.ownership.is_owner(&owner) {
            tracing::error!(
                "The chain with the ID returned by the faucet is not owned by you. \
                Please make sure you are connecting to a genuine faucet."
            );
            return Err(error::Inner::ChainOwnership.into());
        }

        // Try to modify existing chain entry, setting the owner.
        let modified = self
            .wallet()
            .modify(chain_id, |chain| chain.owner = Some(owner))
            .await
            .map_err(error::Inner::wallet)?;
        // If the chain didn't exist, insert a new entry.
        if modified.is_none() {
            let timestamp = chain_description.timestamp();
            let epoch = chain_description.config().epoch;
            self.wallet()
                .insert(
                    chain_id,
                    wallet::Chain {
                        owner: Some(owner),
                        timestamp,
                        epoch: Some(epoch),
                        ..Default::default()
                    },
                )
                .await
                .map_err(error::Inner::wallet)
                .context("assigning new chain")?;
        }
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
        match result? {
            ClientOutcome::Committed(t) => return Ok(t),
            ClientOutcome::Conflict(certificate) => {
                return Err(chain_client::Error::Conflict(certificate.hash()).into());
            }
            ClientOutcome::WaitForTimeout(_) => {}
        }

        // Start listening for notifications, so we learn about new rounds and blocks.
        let (listener, _listen_handle, mut notification_stream) = client.listen().await?;
        self.chain_listeners.spawn_task(listener);

        loop {
            // Try applying f. Return if committed.
            let result = f(client).await;
            self.update_wallet_from_client(client).await?;
            let timeout = match result? {
                ClientOutcome::Committed(t) => return Ok(t),
                ClientOutcome::Conflict(certificate) => {
                    return Err(chain_client::Error::Conflict(certificate.hash()).into());
                }
                ClientOutcome::WaitForTimeout(timeout) => timeout,
            };
            // Otherwise wait and try again in the next round.
            util::wait_for_next_round(&mut notification_stream, timeout).await;
        }
    }

    pub async fn ownership(&mut self, chain_id: Option<ChainId>) -> Result<ChainOwnership, Error> {
        let chain_id = chain_id.unwrap_or_else(|| self.default_chain());
        let client = self.make_chain_client(chain_id).await?;
        let info = client.chain_info().await?;
        Ok(info.manager.ownership)
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
        let mut ownership = chain_client.query_chain_ownership().await?;
        ownership_config.update(&mut ownership)?;

        if ownership.super_owners.is_empty() && ownership.owners.is_empty() {
            tracing::error!("At least one owner or super owner of the chain has to be set.");
            return Err(error::Inner::ChainOwnership.into());
        }

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
                debug!(
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
        let network_description = self.genesis_config.network_description();
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
    ) -> Result<ChainInfo, Error> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        match node.handle_chain_info_query(query).await {
            Ok(response) => {
                debug!(
                    "Validator {address} sees chain {chain_id} at block height {} and epoch {:?}",
                    response.info.next_block_height, response.info.epoch,
                );
                if let Some(public_key) = public_key {
                    if response.check(*public_key).is_ok() {
                        debug!("Signature for public key {public_key} is OK.");
                    } else {
                        return Err(error::Inner::InvalidSignature {
                            public_key: *public_key,
                        }
                        .into());
                    }
                } else {
                    warn!("Not checking signature as public key was not given");
                }
                Ok(*response.info)
            }
            Err(error) => Err(error::Inner::UnavailableChainInfo {
                address: address.to_string(),
                chain_id,
                error: Box::new(error),
            }
            .into()),
        }
    }

    /// Query a validator for version info, network description, and chain info.
    ///
    /// Returns a `ValidatorQueryResults` struct with the results of all three queries.
    pub async fn query_validator(
        &self,
        address: &str,
        node: &impl ValidatorNode,
        chain_id: ChainId,
        public_key: Option<&ValidatorPublicKey>,
    ) -> ValidatorQueryResults {
        let version_info = self.check_compatible_version_info(address, node).await;
        let genesis_config_hash = self.check_matching_network_description(address, node).await;
        let chain_info = self
            .check_validator_chain_info_response(public_key, address, node, chain_id)
            .await;

        ValidatorQueryResults {
            version_info,
            genesis_config_hash,
            chain_info,
        }
    }

    /// Query the local node for version info, network description, and chain info.
    ///
    /// Returns a `ValidatorQueryResults` struct with the local node's information.
    pub async fn query_local_node(
        &self,
        chain_id: ChainId,
    ) -> Result<ValidatorQueryResults, Error> {
        let version_info = Ok(linera_version::VERSION_INFO.clone());
        let genesis_config_hash = Ok(self
            .genesis_config
            .network_description()
            .genesis_config_hash);
        let chain_info = self
            .make_chain_client(chain_id)
            .await?
            .chain_info_with_manager_values()
            .await
            .map(|info| *info)
            .map_err(|e| e.into());

        Ok(ValidatorQueryResults {
            version_info,
            genesis_config_hash,
            chain_info,
        })
    }
}

#[cfg(feature = "fs")]
impl<Env: Environment> ClientContext<Env> {
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

#[cfg(not(web))]
impl<Env: Environment> ClientContext<Env> {
    pub async fn prepare_for_benchmark(
        &mut self,
        num_chains: usize,
        tokens_per_chain: Amount,
        fungible_application_id: Option<ApplicationId>,
        pub_keys: Vec<AccountPublicKey>,
        chains_config_path: Option<&Path>,
    ) -> Result<(Vec<ChainClient<Env>>, Vec<ChainId>), Error> {
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
        let (benchmark_chains, chain_clients) = self
            .make_benchmark_chains(
                num_chains,
                tokens_per_chain,
                pub_keys,
                chains_config_path.is_some(),
            )
            .await?;
        info!(
            "Got {} chains in {} ms",
            num_chains,
            start.elapsed().as_millis()
        );

        if let Some(id) = fungible_application_id {
            let start = Instant::now();
            self.supply_fungible_tokens(&benchmark_chains, id).await?;
            info!(
                "Supplied fungible tokens in {} ms",
                start.elapsed().as_millis()
            );
            // Need to process inboxes to make sure the chains receive the supplied tokens.
            let start = Instant::now();
            for chain_client in &chain_clients {
                chain_client.process_inbox().await?;
            }
            info!(
                "Processed inboxes after supplying fungible tokens in {} ms",
                start.elapsed().as_millis()
            );
        }

        let all_chains = Benchmark::<Env>::get_all_chains(chains_config_path, &benchmark_chains)?;
        let known_chain_ids: HashSet<_> = benchmark_chains.iter().map(|(id, _)| *id).collect();
        let unknown_chain_ids: Vec<_> = all_chains
            .iter()
            .filter(|id| !known_chain_ids.contains(id))
            .copied()
            .collect();
        if !unknown_chain_ids.is_empty() {
            // The current client won't have the blobs for the chains in the other wallets. Even
            // though it will eventually get those blobs, we're getting a head start here and
            // fetching those blobs in advance.
            for chain_id in &unknown_chain_ids {
                self.client.get_chain_description(*chain_id).await?;
            }
        }

        Ok((chain_clients, all_chains))
    }

    pub async fn wrap_up_benchmark(
        &mut self,
        chain_clients: Vec<ChainClient<Env>>,
        close_chains: bool,
        wrap_up_max_in_flight: usize,
    ) -> Result<(), Error> {
        if close_chains {
            info!("Closing chains...");
            let stream = stream::iter(chain_clients)
                .map(|chain_client| async move {
                    Benchmark::<Env>::close_benchmark_chain(&chain_client).await?;
                    info!("Closed chain {:?}", chain_client.chain_id());
                    Ok::<(), BenchmarkError>(())
                })
                .buffer_unordered(wrap_up_max_in_flight);
            stream.try_collect::<Vec<_>>().await?;
        } else {
            info!("Processing inbox for all chains...");
            let stream = stream::iter(chain_clients.clone())
                .map(|chain_client| async move {
                    chain_client.process_inbox().await?;
                    info!("Processed inbox for chain {:?}", chain_client.chain_id());
                    Ok::<(), chain_client::Error>(())
                })
                .buffer_unordered(wrap_up_max_in_flight);
            stream.try_collect::<Vec<_>>().await?;

            info!("Updating wallet from chain clients...");
            for chain_client in chain_clients {
                let info = chain_client.chain_info().await?;
                let client_owner = chain_client.preferred_owner();
                let pending_proposal = chain_client.pending_proposal().clone();
                self.wallet()
                    .insert(
                        info.chain_id,
                        wallet::Chain {
                            pending_proposal,
                            owner: client_owner,
                            ..info.as_ref().into()
                        },
                    )
                    .await
                    .map_err(error::Inner::wallet)?;
            }
        }

        Ok(())
    }

    async fn process_inboxes_and_force_validator_updates(&mut self) {
        let mut join_set = task::JoinSet::new();

        let chain_clients: Vec<_> = self
            .wallet()
            .owned_chain_ids()
            .map_err(|e| error::Inner::wallet(e).into())
            .and_then(|id| self.make_chain_client(id))
            .try_collect()
            .await
            .unwrap();

        for chain_client in chain_clients {
            join_set.spawn(async move {
                Self::process_inbox_without_updating_wallet(&chain_client)
                    .await
                    .expect("Processing inbox should not fail!");
                chain_client
            });
        }

        for chain_client in join_set.join_all().await {
            self.update_wallet_from_client(&chain_client).await.unwrap();
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
        wallet_only: bool,
    ) -> Result<(Vec<(ChainId, AccountOwner)>, Vec<ChainClient<Env>>), Error> {
        let mut chains_found_in_wallet = 0;
        let mut benchmark_chains = Vec::with_capacity(num_chains);
        let mut chain_clients = Vec::with_capacity(num_chains);
        let start = Instant::now();
        let mut owned_chain_ids = std::pin::pin!(self.wallet().owned_chain_ids());
        while let Some(chain_id) = owned_chain_ids.next().await {
            let chain_id = chain_id.map_err(error::Inner::wallet)?;
            if chains_found_in_wallet == num_chains {
                break;
            }
            let chain_client = self.make_chain_client(chain_id).await?;
            let ownership = chain_client.chain_info().await?.manager.ownership;
            if !ownership.owners.is_empty() || ownership.super_owners.len() != 1 {
                continue;
            }
            chain_client.process_inbox().await?;
            benchmark_chains.push((
                chain_id,
                *ownership
                    .super_owners
                    .first()
                    .expect("should have a super owner"),
            ));
            chain_clients.push(chain_client);
            chains_found_in_wallet += 1;
        }
        info!(
            "Got {} chains from the wallet in {} ms",
            benchmark_chains.len(),
            start.elapsed().as_millis()
        );

        let num_chains_to_create = num_chains - chains_found_in_wallet;

        let default_chain_client = self.make_chain_client(self.default_chain()).await?;

        if num_chains_to_create > 0 {
            if wallet_only {
                return Err(
                    error::Inner::Benchmark(BenchmarkError::NotEnoughChainsInWallet(
                        num_chains,
                        chains_found_in_wallet,
                    ))
                    .into(),
                );
            }
            let mut pub_keys_iter = pub_keys.into_iter().take(num_chains_to_create);
            let operations_per_block = 900; // Over this we seem to hit the block size limits.
            for i in (0..num_chains_to_create).step_by(operations_per_block) {
                let num_new_chains = operations_per_block.min(num_chains_to_create - i);
                let pub_key = pub_keys_iter.next().unwrap();
                let owner = pub_key.into();

                let certificate = Self::execute_open_chains_operations(
                    num_new_chains,
                    &default_chain_client,
                    balance,
                    owner,
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
                    self.client
                        .extend_chain_mode(chain_id, ListeningMode::FullChain);

                    let mut chain_client = self.client.create_chain_client(
                        chain_id,
                        None,
                        BlockHeight::ZERO,
                        None,
                        Some(owner),
                        self.timing_sender(),
                        false,
                    );
                    chain_client.set_preferred_owner(owner);
                    chain_client.process_inbox().await?;
                    benchmark_chains.push((chain_id, owner));
                    chain_clients.push(chain_client);
                }
            }

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

        assert_eq!(
            benchmark_chains.len(),
            chain_clients.len(),
            "benchmark_chains and chain_clients must have the same size"
        );

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
        key_pairs: &[(ChainId, AccountOwner)],
        application_id: ApplicationId,
    ) -> Result<(), Error> {
        let default_chain_id = self.default_chain();
        let default_key = self
            .wallet()
            .get(default_chain_id)
            .await
            .unwrap()
            .unwrap()
            .owner
            .unwrap();
        // This should be enough to run the benchmark at 1M TPS for an hour.
        let amount = Amount::from_nanos(4);
        let operations: Vec<Operation> = key_pairs
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
