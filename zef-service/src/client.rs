// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

use bytes::Bytes;
use futures::stream::StreamExt;
use log::*;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use zef_base::{base_types::*, committee::Committee, messages::*, serialize::*};
use zef_core::{client::*, node::ValidatorNode};
use zef_service::{config::*, network, storage::MixedStorage, transport};
use zef_storage::{InMemoryStoreClient, Storage};

struct ClientContext {
    committee_config: CommitteeConfig,
    wallet_state_path: PathBuf,
    wallet_state: WalletState,
    storage_client: MixedStorage,
    buffer_size: usize,
    send_timeout: Duration,
    recv_timeout: Duration,
    cross_chain_delay: Duration,
    cross_chain_retries: usize,
}

impl ClientContext {
    async fn from_options(options: &ClientOptions) -> Self {
        let wallet_state_path = options.wallet_state_path.clone();
        let wallet_state =
            WalletState::read_or_create(&wallet_state_path).expect("Unable to read user chains");
        let (storage_client, committee_config): (MixedStorage, CommitteeConfig) = match options.cmd
        {
            ClientCommands::CreateGenesisConfig { .. } => {
                // This is a placeholder to avoid create a DB on disk at this point.
                (
                    Box::new(InMemoryStoreClient::default()),
                    CommitteeConfig {
                        validators: Vec::new(),
                    },
                )
            }
            _ => {
                // Every other command uses the real chain storage.
                let genesis_config = GenesisConfig::read(&options.genesis_config_path)
                    .expect("Fail to read initial chain config");
                let storage = zef_service::storage::make_storage(
                    options.storage_path.as_ref(),
                    &genesis_config,
                )
                .await
                .unwrap();
                (storage, genesis_config.committee)
            }
        };
        let send_timeout = Duration::from_micros(options.send_timeout_us);
        let recv_timeout = Duration::from_micros(options.recv_timeout_us);
        let cross_chain_delay = Duration::from_micros(options.cross_chain_delay_ms);

        ClientContext {
            committee_config,
            wallet_state_path,
            wallet_state,
            storage_client,
            buffer_size: options.buffer_size,
            send_timeout,
            recv_timeout,
            cross_chain_delay,
            cross_chain_retries: options.cross_chain_retries,
        }
    }

    fn make_validator_clients(&self) -> Vec<(ValidatorName, network::Client)> {
        let mut validator_clients = Vec::new();
        for config in &self.committee_config.validators {
            let config = config.clone();
            let client = network::Client::new(
                config.network_protocol,
                config.host,
                config.base_port,
                config.num_shards,
                self.buffer_size,
                self.send_timeout,
                self.recv_timeout,
            );
            validator_clients.push((config.name, client));
        }
        validator_clients
    }

    fn make_validator_mass_clients(&self, max_in_flight: u64) -> Vec<(u32, network::MassClient)> {
        let mut validator_clients = Vec::new();
        for config in &self.committee_config.validators {
            let client = network::MassClient::new(
                config.network_protocol,
                config.host.clone(),
                config.base_port,
                self.buffer_size,
                self.send_timeout,
                self.recv_timeout,
                max_in_flight / config.num_shards as u64, // Distribute window to diff shards
            );
            validator_clients.push((config.num_shards, client));
        }
        validator_clients
    }

    fn make_chain_client(
        &self,
        chain_id: ChainId,
    ) -> ChainClientState<network::Client, MixedStorage> {
        let chain = self.wallet_state.get(&chain_id).expect("Unknown chain");
        let validator_clients = self.make_validator_clients();
        ChainClientState::new(
            chain_id,
            chain
                .key_pair
                .as_ref()
                .map(|kp| kp.copy())
                .into_iter()
                .collect(),
            validator_clients,
            self.storage_client.clone(),
            chain.block_hash,
            chain.next_block_height,
            self.cross_chain_delay,
            self.cross_chain_retries,
        )
    }

    async fn update_recipient_chain(
        &mut self,
        certificate: Certificate,
        key_pair: Option<KeyPair>,
    ) -> Result<(), failure::Error> {
        let recipient = match &certificate.value {
            Value::Confirmed { block } => block.operation.recipient().unwrap().clone(),
            _ => failure::bail!("unexpected value in certificate"),
        };
        let validator_clients = self.make_validator_clients();
        let chain = self.wallet_state.get_or_insert(recipient.clone());
        let mut client = ChainClientState::new(
            recipient,
            chain
                .key_pair
                .as_ref()
                .map(|kp| kp.copy())
                .or(key_pair)
                .into_iter()
                .collect(),
            validator_clients,
            self.storage_client.clone(),
            chain.block_hash,
            chain.next_block_height,
            Duration::default(),
            0,
        );
        client.receive_certificate(certificate).await?;
        self.update_chain_from_state(&mut client).await;
        Ok(())
    }

    /// Make one block proposal per chain, up to `max_proposals` blocks.
    fn make_benchmark_block_proposals(
        &mut self,
        max_proposals: usize,
    ) -> (Vec<BlockProposal>, Vec<(ChainId, Bytes)>) {
        let mut proposals = Vec::new();
        let mut serialized_proposals = Vec::new();
        let mut next_recipient = self.wallet_state.last_chain().unwrap().chain_id.clone();
        for chain in self.wallet_state.chains_mut() {
            let key_pair = match &chain.key_pair {
                Some(kp) => kp,
                None => continue,
            };
            let block = Block {
                chain_id: chain.chain_id.clone(),
                incoming_messages: Vec::new(),
                operation: Operation::Transfer {
                    recipient: Address::Account(next_recipient),
                    amount: Amount::from(1),
                    user_data: UserData::default(),
                },
                previous_block_hash: chain.block_hash,
                height: chain.next_block_height,
            };
            debug!("Preparing block proposal: {:?}", block);
            let proposal = BlockProposal::new(
                BlockAndRound(block.clone(), RoundNumber::default()),
                key_pair,
            );
            proposals.push(proposal.clone());
            let serialized_proposal =
                serialize_message(&SerializedMessage::BlockProposal(Box::new(proposal)));
            serialized_proposals.push((chain.chain_id.clone(), serialized_proposal.into()));
            if serialized_proposals.len() >= max_proposals {
                break;
            }
            // Update the state of the wallet.
            let value = Value::Confirmed { block };
            chain.block_hash = Some(HashValue::new(&value));
            chain.next_block_height.try_add_assign_one().unwrap();

            next_recipient = chain.chain_id.clone();
        }
        (proposals, serialized_proposals)
    }

    /// Try to make certificates from proposals and server configs
    fn make_benchmark_certificates_from_proposals_and_server_configs(
        proposals: Vec<BlockProposal>,
        server_config: Vec<&std::path::Path>,
    ) -> Vec<(ChainId, Bytes)> {
        let mut keys = Vec::new();
        for file in server_config {
            let server_config =
                ValidatorServerConfig::read(file).expect("Fail to read server config");
            keys.push((server_config.validator.name, server_config.key));
        }
        let committee = Committee::make_simple(keys.iter().map(|(n, _)| *n).collect());
        assert!(
            keys.len() >= committee.quorum_threshold(),
            "Not enough server configs were provided with --server-configs"
        );
        let mut serialized_certificates = Vec::new();
        for proposal in proposals {
            let mut certificate = Certificate::new(
                Value::Confirmed {
                    block: proposal.block_and_round.0.clone(),
                },
                Vec::new(),
            );
            for i in 0..committee.quorum_threshold() {
                let (pubx, secx) = keys.get(i).unwrap();
                let sig = Signature::new(&certificate.value, secx);
                certificate.signatures.push((*pubx, sig));
            }
            let serialized_certificate =
                serialize_message(&SerializedMessage::Certificate(Box::new(certificate)));
            serialized_certificates.push((
                proposal.block_and_round.0.chain_id,
                serialized_certificate.into(),
            ));
        }
        serialized_certificates
    }

    /// Try to aggregate votes into certificates.
    fn make_benchmark_certificates_from_votes(&self, votes: Vec<Vote>) -> Vec<(ChainId, Bytes)> {
        let committee = self.committee_config.clone().into_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for vote in votes {
            // We aggregate votes indexed by sender.
            let chain_id = vote.value.chain_id().clone();
            if done_senders.contains(&chain_id) {
                continue;
            }
            debug!(
                "Processing vote on {:?}'s block by {:?}",
                chain_id, vote.validator,
            );
            let value = vote.value;
            let aggregator = aggregators
                .entry(chain_id.clone())
                .or_insert_with(|| SignatureAggregator::new(value, &committee));
            match aggregator.append(vote.validator, vote.signature) {
                Ok(Some(certificate)) => {
                    debug!("Found certificate: {:?}", certificate);
                    let buf =
                        serialize_message(&SerializedMessage::Certificate(Box::new(certificate)));
                    certificates.push((chain_id.clone(), buf.into()));
                    done_senders.insert(chain_id);
                }
                Ok(None) => {
                    debug!("Added one vote");
                }
                Err(error) => {
                    error!("Failed to aggregate vote: {}", error);
                }
            }
        }
        certificates
    }

    /// Broadcast a bulk of blocks to each validator.
    async fn mass_broadcast(
        &self,
        phase: &'static str,
        max_in_flight: u64,
        proposals: Vec<(ChainId, Bytes)>,
    ) -> Vec<Bytes> {
        let time_start = Instant::now();
        info!("Broadcasting {} {}", proposals.len(), phase);
        let validator_clients = self.make_validator_mass_clients(max_in_flight);
        let mut streams = Vec::new();
        for (num_shards, client) in validator_clients {
            // Re-index proposals by shard for this particular validator client.
            let mut sharded_blocks = HashMap::new();
            for (chain_id, buf) in &proposals {
                let shard = network::get_shard(num_shards, chain_id);
                sharded_blocks
                    .entry(shard)
                    .or_insert_with(Vec::new)
                    .push(buf.clone());
            }
            streams.push(client.run(sharded_blocks));
        }
        let responses = futures::stream::select_all(streams).concat().await;
        let time_elapsed = time_start.elapsed();
        warn!(
            "Received {} responses in {} ms.",
            responses.len(),
            time_elapsed.as_millis()
        );
        warn!(
            "Estimated server throughput: {} {} per sec",
            (proposals.len() as u128) * 1_000_000 / time_elapsed.as_micros(),
            phase
        );
        responses
    }

    fn save_chains(&self) {
        self.wallet_state
            .write(&self.wallet_state_path)
            .expect("Unable to write user chains");
        info!("Saved user chain states");
    }

    async fn update_chain_from_state<A, S>(&mut self, state: &mut ChainClientState<A, S>)
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
        S: Storage + Clone + 'static,
    {
        self.wallet_state.update_from_state(state).await
    }
}

fn deserialize_response(response: &[u8]) -> Option<ChainInfoResponse> {
    match deserialize_message(response) {
        Ok(SerializedMessage::ChainInfoResponse(info)) => Some(*info),
        Ok(SerializedMessage::Error(error)) => {
            error!("Received error value: {}", error);
            None
        }
        Ok(_) => {
            error!("Unexpected return value");
            None
        }
        Err(error) => {
            error!(
                "Unexpected error: {} while deserializing {:?}",
                error, response
            );
            None
        }
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "Zef Client",
    about = "A Byzantine-fault tolerant sidechain with low-latency finality and high throughput"
)]
struct ClientOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[structopt(long = "wallet")]
    wallet_state_path: PathBuf,

    /// Optional directory for the file storage of chain public states.
    #[structopt(long = "storage")]
    storage_path: Option<PathBuf>,

    /// Optional path to the file describing the initial user chains (aka genesis state)
    #[structopt(long = "genesis")]
    genesis_config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[structopt(long, default_value = "4000000")]
    send_timeout_us: u64,

    /// Timeout for receiving responses (us)
    #[structopt(long, default_value = "4000000")]
    recv_timeout_us: u64,

    /// Maximum size of datagrams received and sent (bytes)
    #[structopt(long, default_value = transport::DEFAULT_MAX_DATAGRAM_SIZE)]
    buffer_size: usize,

    /// Time between attempts while waiting on cross-chain updates (ms)
    #[structopt(long, default_value = "4000")]
    cross_chain_delay_ms: u64,

    #[structopt(long, default_value = "10")]
    cross_chain_retries: usize,

    /// Subcommands.
    #[structopt(subcommand)]
    cmd: ClientCommands,
}

#[derive(StructOpt)]
enum ClientCommands {
    /// Transfer funds
    #[structopt(name = "transfer")]
    Transfer {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,

        /// Recipient chain id
        #[structopt(long = "to")]
        recipient: ChainId,

        /// Amount to transfer
        amount: Amount,
    },

    /// Open (i.e. activate) a new chain deriving the UID from an existing one.
    #[structopt(name = "open_chain")]
    OpenChain {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[structopt(long = "to-owner")]
        owner: Option<Owner>,
    },

    /// Close (i.e. deactivate) an existing chain. (Consider `spend_and_transfer`
    /// instead for real-life use cases.)
    #[structopt(name = "close_chain")]
    CloseChain {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,
    },

    /// Obtain the balance of the chain directly from a quorum of validators.
    #[structopt(name = "query_balance")]
    QueryBalance {
        /// Chain id
        chain_id: ChainId,
    },

    /// Synchronize the local state of the chain (including a conservative estimation of the
    /// available balance) with a quorum validators.
    #[structopt(name = "sync_balance")]
    SynchronizeBalance {
        /// Chain id
        chain_id: ChainId,
    },

    /// Send one transfer per chain in bulk mode
    #[structopt(name = "benchmark")]
    Benchmark {
        /// Maximum number of blocks in flight
        #[structopt(long, default_value = "200")]
        max_in_flight: u64,

        /// Use a subset of the chains to generate N transfers
        #[structopt(long)]
        max_proposals: Option<usize>,

        /// Use server configuration files to generate certificates (instead of aggregating received votes).
        #[structopt(long)]
        server_configs: Option<Vec<String>>,
    },

    /// Create initial user chains and print information to be used for initialization of validator setup.
    #[structopt(name = "create_genesis_config")]
    CreateGenesisConfig {
        /// Sets the file describing the public configurations of all validators
        #[structopt(long = "committee")]
        committee_config_path: PathBuf,

        /// Known initial balance of the chain
        #[structopt(long, default_value = "0")]
        initial_funding: Balance,

        /// Number of additional chains to create
        num: u32,
    },
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let options = ClientOptions::from_args();
    let mut context = ClientContext::from_options(&options).await;
    match options.cmd {
        ClientCommands::Transfer {
            sender,
            recipient,
            amount,
        } => {
            let mut client_state = context.make_chain_client(sender);
            info!("Starting transfer");
            let time_start = Instant::now();
            let certificate = client_state
                .transfer_to_chain(amount, recipient.clone(), UserData::default())
                .await
                .unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            context.update_chain_from_state(&mut client_state).await;

            info!("Updating recipient's local chain");
            context
                .update_recipient_chain(certificate, None)
                .await
                .unwrap();
            context.save_chains();
        }

        ClientCommands::OpenChain { sender, owner } => {
            let mut client_state = context.make_chain_client(sender);
            let (new_owner, key_pair) = match owner {
                Some(key) => (key, None),
                None => {
                    let key_pair = KeyPair::generate();
                    (key_pair.public(), Some(key_pair))
                }
            };
            info!("Starting operation to open a new chain");
            let time_start = Instant::now();
            let certificate = client_state.open_chain(new_owner).await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            println!(
                "{}",
                certificate
                    .value
                    .confirmed_block()
                    .unwrap()
                    .operation
                    .recipient()
                    .unwrap()
            );
            context.update_chain_from_state(&mut client_state).await;

            info!("Updating recipient's local chain");
            context
                .update_recipient_chain(certificate, key_pair)
                .await
                .unwrap();
            context.save_chains();
        }

        ClientCommands::CloseChain { sender } => {
            let mut client_state = context.make_chain_client(sender);
            info!("Starting operation to close the chain");
            let time_start = Instant::now();
            let certificate = client_state.close_chain().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            context.update_chain_from_state(&mut client_state).await;
            context.save_chains();
        }

        ClientCommands::QueryBalance { chain_id } => {
            let mut client_state = context.make_chain_client(chain_id);
            info!("Starting query for the local balance");
            let time_start = Instant::now();
            let balance = client_state.local_balance().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Local balance obtained after {} us", time_total);
            println!("{}", balance);
            context.update_chain_from_state(&mut client_state).await;
            context.save_chains();
        }

        ClientCommands::SynchronizeBalance { chain_id } => {
            let mut client_state = context.make_chain_client(chain_id);
            info!("Synchronize chain information");
            let time_start = Instant::now();
            let balance = client_state.synchronize_balance().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Chain balance synchronized after {} us", time_total);
            println!("{}", balance);
            context.update_chain_from_state(&mut client_state).await;
            context.save_chains();
        }

        ClientCommands::Benchmark {
            max_in_flight,
            max_proposals,
            server_configs,
        } => {
            let max_proposals = max_proposals.unwrap_or_else(|| context.wallet_state.num_chains());
            warn!("Starting benchmark phase 1 (block proposals)");
            let (proposals, serialize_proposals) =
                context.make_benchmark_block_proposals(max_proposals);
            let responses = context
                .mass_broadcast("block proposals", max_in_flight, serialize_proposals)
                .await;
            let votes: Vec<_> = responses
                .into_iter()
                .filter_map(|buf| {
                    deserialize_response(&buf[..])
                        .and_then(|response| response.info.manager.pending().cloned())
                })
                .collect();
            warn!("Received {} valid votes.", votes.len());

            warn!("Starting benchmark phase 2 (certified blocks)");
            let certificates = if let Some(files) = server_configs {
                warn!("Using server configs provided by --server-configs");
                let files = files.iter().map(AsRef::as_ref).collect();
                ClientContext::make_benchmark_certificates_from_proposals_and_server_configs(
                    proposals, files,
                )
            } else {
                warn!("Using committee config");
                context.make_benchmark_certificates_from_votes(votes)
            };
            let responses = context
                .mass_broadcast("certificates", max_in_flight, certificates.clone())
                .await;
            let mut confirmed = HashSet::new();
            let num_valid =
                responses
                    .iter()
                    .fold(0, |acc, buf| match deserialize_response(&buf[..]) {
                        Some(response) => {
                            confirmed.insert(response.info.chain_id);
                            acc + 1
                        }
                        None => acc,
                    });
            warn!(
                "Received {} valid certificates for {} block proposals.",
                num_valid,
                confirmed.len()
            );

            warn!("Updating local state of user chains");
            context.save_chains();
        }

        ClientCommands::CreateGenesisConfig {
            committee_config_path,
            initial_funding,
            num,
        } => {
            let committee_config = CommitteeConfig::read(&committee_config_path)
                .expect("Unable to read committee config file");
            let mut genesis_config = GenesisConfig::new(committee_config);
            for i in 0..num {
                // Create keys.
                let chain =
                    UserChain::make_initial(ChainId::new(vec![BlockHeight::from(i as u64)]));
                // Public "genesis" state.
                genesis_config.chains.push((
                    chain.chain_id.clone(),
                    chain.key_pair.as_ref().unwrap().public(),
                    initial_funding,
                ));
                // Private keys.
                context.wallet_state.insert(chain);
            }
            context.save_chains();
            genesis_config.write(&options.genesis_config_path).unwrap();
        }
    }
}
