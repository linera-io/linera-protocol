// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::num::NonZeroUsize;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use futures::Future;
use linera_base::{
    crypto::KeyPair,
    data_types::{Blob, BlockHeight, Timestamp},
    identifiers::{Account, BlobId, ChainId},
    ownership::ChainOwnership,
    time::{Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{
    client::{BlanketMessagePolicy, ChainClient, Client, MessagePolicy},
    data_types::ClientOutcome,
    join_set_ext::{JoinSet, JoinSetExt as _},
    node::CrossChainMessageDelivery,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_storage::Storage;
use thiserror_context::Context;
use tracing::{debug, info};
#[cfg(feature = "benchmark")]
use {
    futures::{stream, StreamExt as _, TryStreamExt as _},
    linera_base::{
        crypto::PublicKey,
        data_types::Amount,
        identifiers::{AccountOwner, ApplicationId, Owner},
    },
    linera_chain::data_types::{
        BlockProposal, ExecutedBlock, ProposedBlock, SignatureAggregator, Vote,
    },
    linera_chain::types::{CertificateValue, GenericCertificate},
    linera_core::data_types::ChainInfoQuery,
    linera_execution::{
        committee::Epoch,
        system::{OpenChainConfig, Recipient, SystemOperation, OPEN_CHAIN_MESSAGE_INDEX},
        Operation,
    },
    linera_rpc::{
        config::NetworkProtocol, grpc::GrpcClient, mass_client::MassClient,
        simple::SimpleMassClient, RpcMessage,
    },
    linera_sdk::abis::fungible,
    std::{collections::HashMap, iter},
    tracing::{error, trace},
};
#[cfg(feature = "fs")]
use {
    linera_base::{
        crypto::CryptoHash,
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
        key_pair: Option<KeyPair>,
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
        let mut chain_client = self.client.create_chain_client(
            chain_id,
            known_key_pairs,
            self.wallet.genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_block.clone(),
            chain.pending_blobs.clone(),
        );
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
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain_internal(chain_id, key_pair, timestamp, BTreeMap::new())
            .await
    }

    #[cfg(test)]
    pub async fn update_wallet_for_new_chain_with_pending_blobs(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
        pending_blobs: BTreeMap<BlobId, Blob>,
    ) -> Result<(), Error> {
        self.update_wallet_for_new_chain_internal(chain_id, key_pair, timestamp, pending_blobs)
            .await
    }

    async fn update_wallet_for_new_chain_internal(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
        pending_blobs: BTreeMap<BlobId, Blob>,
    ) -> Result<(), Error> {
        if self.wallet.get(chain_id).is_none() {
            self.mutate_wallet(|w| {
                w.insert(UserChain {
                    chain_id,
                    key_pair: key_pair.as_ref().map(|kp| kp.copy()),
                    block_hash: None,
                    timestamp,
                    next_block_height: BlockHeight::ZERO,
                    pending_block: None,
                    pending_blobs,
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
    pub async fn process_inboxes_and_force_validator_updates(&mut self) {
        for chain_id in self.wallet.owned_chain_ids() {
            let chain_client = self
                .make_chain_client(chain_id)
                .expect("chains in the wallet must exist");
            self.process_inbox(&chain_client).await.unwrap();
            chain_client.update_validators(None).await.unwrap();
            self.update_wallet_from_client(&chain_client).await.unwrap();
        }
    }

    /// Creates chains if necessary, and returns a map of exactly `num_chains` chain IDs
    /// with key pairs.
    pub async fn make_benchmark_chains(
        &mut self,
        num_chains: usize,
        balance: Amount,
    ) -> Result<HashMap<ChainId, KeyPair>, Error> {
        let mut key_pairs = HashMap::new();
        for chain_id in self.wallet.owned_chain_ids() {
            if key_pairs.len() == num_chains {
                break;
            }
            let Some(key_pair) = self
                .wallet
                .get(chain_id)
                .and_then(|chain| chain.key_pair.as_ref().map(|kp| kp.copy()))
            else {
                continue;
            };
            let chain_client = self.make_chain_client(chain_id)?;
            let ownership = chain_client.chain_info().await?.manager.ownership;
            if !ownership.owners.is_empty() || ownership.super_owners.len() != 1 {
                continue;
            }
            key_pairs.insert(chain_id, key_pair);
        }

        let default_chain_id = self
            .wallet
            .default_chain()
            .expect("should have default chain");
        let chain_client = self.make_chain_client(default_chain_id)?;
        while key_pairs.len() < num_chains {
            let key_pair = self.wallet.generate_key_pair();
            let (epoch, committees) = chain_client.epoch_and_committees(default_chain_id).await?;
            let epoch = epoch.expect("default chain should be active");
            // Put at most 1000 OpenChain operations in each block.
            let num_new_chains = (num_chains - key_pairs.len()).min(1000);
            let config = OpenChainConfig {
                ownership: ChainOwnership::single_super(key_pair.public().into()),
                committees,
                admin_id: self.wallet.genesis_admin_chain(),
                epoch,
                balance,
                application_permissions: Default::default(),
            };
            let operations = iter::repeat(Operation::System(SystemOperation::OpenChain(config)))
                .take(num_new_chains)
                .collect();
            let certificate = chain_client
                .execute_operations(operations)
                .await?
                .expect("should execute block with OpenChain operations");
            let block = certificate.block();
            let timestamp = block.header.timestamp;
            for i in 0..num_new_chains {
                let message_id = block
                    .message_id_for_operation(i, OPEN_CHAIN_MESSAGE_INDEX)
                    .expect("failed to create new chain");
                let chain_id = ChainId::child(message_id);
                key_pairs.insert(chain_id, key_pair.copy());
                self.client.track_chain(chain_id);
                self.update_wallet_for_new_chain(chain_id, Some(key_pair.copy()), timestamp)
                    .await?;
            }
        }
        self.update_wallet_from_client(&chain_client).await?;
        let updated_chain_client = self.make_chain_client(default_chain_id)?;
        updated_chain_client
            .retry_pending_outgoing_messages()
            .await
            .context("outgoing messages to create the new chains should be delivered")?;

        for chain_id in key_pairs.keys() {
            let child_client = self.make_chain_client(*chain_id)?;
            child_client.process_inbox().await?;
            self.wallet.as_mut().update_from_state(&child_client).await;
            self.save_wallet().await?;
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
                Self::fungible_transfer(
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
        for operations in operations.chunks(1000) {
            chain_client
                .execute_operations(operations.to_vec())
                .await?
                .expect("should execute block with OpenChain operations");
        }
        self.update_wallet_from_client(&chain_client).await?;
        // Make sure all chains have registered the application now.
        let futures = key_pairs
            .keys()
            .map(|&chain_id| {
                let chain_client = self
                    .make_chain_client(chain_id)
                    .expect("chain should have been created");
                async move {
                    for i in 0..5 {
                        linera_base::time::timer::sleep(Duration::from_secs(i)).await;
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
                            return Ok::<_, Error>(chain_client);
                        }
                    }
                    panic!("Could not instantiate application on chain {chain_id:?}");
                }
            })
            .collect::<Vec<_>>();
        // We have to collect the futures to avoid a higher-ranked lifetime error:
        // https://github.com/rust-lang/rust/issues/102211#issuecomment-1673201352
        let clients = stream::iter(futures)
            .buffer_unordered(max_in_flight)
            .try_collect::<Vec<_>>()
            .await?;
        for client in clients {
            self.update_wallet_from_client(&client).await?;
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
        let mut next_recipient = self.wallet.last_chain().unwrap().chain_id;
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
                }),
            };
            let operations = iter::repeat(operation)
                .take(transactions_per_block)
                .collect();
            let chain = self.wallet.get(chain_id).expect("should have chain");
            let block = ProposedBlock {
                epoch: Epoch::ZERO,
                chain_id,
                incoming_bundles: Vec::new(),
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
            );
            proposals.push(RpcMessage::BlockProposal(Box::new(proposal)));
            next_recipient = chain.chain_id;
        }
        proposals
    }

    /// Tries to aggregate votes into certificates.
    pub fn make_benchmark_certificates_from_votes<T>(
        &self,
        votes: Vec<Vote<T>>,
    ) -> Vec<GenericCertificate<T>>
    where
        T: std::fmt::Debug + CertificateValue,
    {
        let committee = self.wallet.genesis_config().create_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for vote in votes {
            // We aggregate votes indexed by sender.
            let chain_id = vote.value().inner().chain_id();
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
        for client in self.make_validator_mass_clients() {
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
        for config in &self.wallet.genesis_config().committee.validators {
            let client: Box<dyn MassClient + Send> = match config.network.protocol {
                NetworkProtocol::Simple(protocol) => {
                    let network = config.network.clone_with_protocol(protocol);
                    Box::new(SimpleMassClient::new(
                        network,
                        self.send_timeout,
                        self.recv_timeout,
                    ))
                }
                NetworkProtocol::Grpc { .. } => {
                    let node_options = self.make_node_options();
                    let address = config.network.http_address();
                    Box::new(GrpcClient::create(address, node_options))
                }
            };

            validator_clients.push(client);
        }
        validator_clients
    }

    pub async fn update_wallet_from_certificates(
        &mut self,
        certificates: Vec<ConfirmedBlockCertificate>,
    ) {
        let node = self.client.local_node().clone();
        // Replay the certificates locally.
        for certificate in certificates {
            // No required certificates from other chains: This is only used with benchmark.
            node.handle_certificate(certificate, &()).await.unwrap();
        }
        // Last update the wallet.
        for chain in self.wallet.as_mut().chains_mut() {
            let query = ChainInfoQuery::new(chain.chain_id);
            let info = node.handle_chain_info_query(query).await.unwrap().info;
            // We don't have private keys but that's ok.
            chain.block_hash = info.block_hash;
            chain.next_block_height = info.next_block_height;
        }
        self.save_wallet().await.unwrap();
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

    /// Stages the execution of a block proposal.
    pub async fn stage_block_execution(
        &self,
        block: ProposedBlock,
    ) -> Result<ExecutedBlock, Error> {
        Ok(self
            .client
            .local_node()
            .stage_block_execution(block)
            .await?
            .0)
    }
}
