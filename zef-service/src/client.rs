// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

use zef_core::{
    base_types::*,
    client::*,
    committee::Committee,
    messages::*,
    serialize::*,
    storage::{InMemoryStoreClient, StorageClient},
};
use zef_service::{config::*, network, storage::Storage, transport};

use bytes::Bytes;
use futures::stream::StreamExt;
use log::*;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};
use structopt::StructOpt;

struct ClientContext {
    committee_config: CommitteeConfig,
    wallet_state_path: PathBuf,
    wallet_state: WalletState,
    storage_client: Storage,
    buffer_size: usize,
    send_timeout: Duration,
    recv_timeout: Duration,
    cross_shard_delay: Duration,
    cross_shard_retries: usize,
}

impl ClientContext {
    async fn from_options(options: &ClientOptions) -> Self {
        let committee_config = CommitteeConfig::read(&options.committee_config_path)
            .expect("Unable to read committee config file");
        let wallet_state_path = options.wallet_state_path.clone();
        let wallet_state =
            WalletState::read_or_create(&wallet_state_path).expect("Unable to read user accounts");
        let storage_client: Storage = match options.cmd {
            ClientCommands::CreateGenesisConfig { .. } => {
                // This is a placeholder to avoid create a DB on disk at this point.
                Box::new(InMemoryStoreClient::default())
            }
            _ => {
                // Every other command uses the real account storage.
                let genesis_config = options
                    .genesis_config_path
                    .as_ref()
                    .map(|path| {
                        GenesisConfig::read(path).expect("Fail to read initial account config")
                    })
                    .unwrap_or_default();
                zef_service::storage::make_storage(
                    options.storage_path.as_ref(),
                    committee_config.clone().into_committee(),
                    &genesis_config,
                )
                .await
                .unwrap()
            }
        };
        let send_timeout = Duration::from_micros(options.send_timeout_us);
        let recv_timeout = Duration::from_micros(options.recv_timeout_us);
        let cross_shard_delay = Duration::from_micros(options.cross_shard_delay_ms);

        ClientContext {
            committee_config,
            wallet_state_path,
            wallet_state,
            storage_client,
            buffer_size: options.buffer_size,
            send_timeout,
            recv_timeout,
            cross_shard_delay,
            cross_shard_retries: options.cross_shard_retries,
        }
    }

    fn make_authority_clients(&self) -> Vec<(AuthorityName, network::Client)> {
        let mut authority_clients = Vec::new();
        for config in &self.committee_config.authorities {
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
            authority_clients.push((config.name, client));
        }
        authority_clients
    }

    fn make_authority_mass_clients(&self, max_in_flight: u64) -> Vec<(u32, network::MassClient)> {
        let mut authority_clients = Vec::new();
        for config in &self.committee_config.authorities {
            let client = network::MassClient::new(
                config.network_protocol,
                config.host.clone(),
                config.base_port,
                self.buffer_size,
                self.send_timeout,
                self.recv_timeout,
                max_in_flight / config.num_shards as u64, // Distribute window to diff shards
            );
            authority_clients.push((config.num_shards, client));
        }
        authority_clients
    }

    fn make_account_client(
        &self,
        account_id: AccountId,
    ) -> AccountClientState<network::Client, Storage> {
        let account = self.wallet_state.get(&account_id).expect("Unknown account");
        let authority_clients = self.make_authority_clients();
        AccountClientState::new(
            account_id,
            account.key_pair.as_ref().map(|kp| kp.copy()),
            authority_clients,
            self.storage_client.clone(),
            account.next_sequence_number,
            self.cross_shard_delay,
            self.cross_shard_retries,
        )
    }

    async fn update_recipient_account(
        &mut self,
        certificate: Certificate,
        key_pair: Option<KeyPair>,
    ) -> Result<(), failure::Error> {
        let recipient = match &certificate.value {
            Value::Confirmed { request } => request.operation.recipient().unwrap().clone(),
            _ => failure::bail!("unexpected value in certificate"),
        };
        let authority_clients = self.make_authority_clients();
        let account = self.wallet_state.get_or_insert(recipient.clone());
        let mut client = AccountClientState::new(
            recipient,
            account.key_pair.as_ref().map(|kp| kp.copy()).or(key_pair),
            authority_clients,
            self.storage_client.clone(),
            account.next_sequence_number,
            Duration::default(),
            0,
        );
        client.receive_certificate(certificate).await?;
        self.update_account_from_state(&mut client).await;
        Ok(())
    }

    /// Make one request order per account, up to `max_orders` requests.
    fn make_benchmark_request_orders(
        &mut self,
        max_orders: usize,
    ) -> (Vec<RequestOrder>, Vec<(AccountId, Bytes)>) {
        let mut orders = Vec::new();
        let mut serialized_orders = Vec::new();
        let mut next_recipient = self.wallet_state.last_account().unwrap().account_id.clone();
        for account in self.wallet_state.accounts_mut() {
            let key_pair = match &account.key_pair {
                Some(kp) => kp,
                None => continue,
            };
            let request = Request {
                account_id: account.account_id.clone(),
                operation: Operation::Transfer {
                    recipient: Address::Account(next_recipient),
                    amount: Amount::from(1),
                    user_data: UserData::default(),
                },
                sequence_number: account.next_sequence_number,
                round: RoundNumber::default(),
            };
            debug!("Preparing request order: {:?}", request);
            account.next_sequence_number.try_add_assign_one().unwrap();
            let order = RequestOrder::new(request.clone(), key_pair);
            orders.push(order.clone());
            let serialized_order =
                serialize_message(&SerializedMessage::RequestOrder(Box::new(order)));
            serialized_orders.push((account.account_id.clone(), serialized_order.into()));
            if serialized_orders.len() >= max_orders {
                break;
            }

            next_recipient = account.account_id.clone();
        }
        (orders, serialized_orders)
    }

    /// Try to make certificates from orders and server configs
    fn make_benchmark_certificates_from_orders_and_server_configs(
        orders: Vec<RequestOrder>,
        server_config: Vec<&std::path::Path>,
    ) -> Vec<(AccountId, Bytes)> {
        let mut keys = Vec::new();
        for file in server_config {
            let server_config =
                AuthorityServerConfig::read(file).expect("Fail to read server config");
            keys.push((server_config.authority.name, server_config.key));
        }
        let committee = Committee::make_simple(keys.iter().map(|(n, _)| *n).collect());
        assert!(
            keys.len() >= committee.quorum_threshold(),
            "Not enough server configs were provided with --server-configs"
        );
        let mut serialized_certificates = Vec::new();
        for order in orders {
            let mut certificate = Certificate::new(
                Value::Confirmed {
                    request: order.request.clone(),
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
            serialized_certificates.push((order.request.account_id, serialized_certificate.into()));
        }
        serialized_certificates
    }

    /// Try to aggregate votes into certificates.
    fn make_benchmark_certificates_from_votes(&self, votes: Vec<Vote>) -> Vec<(AccountId, Bytes)> {
        let committee = self.committee_config.clone().into_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for vote in votes {
            // We aggregate votes indexed by sender.
            let account_id = vote.value.account_id().clone();
            if done_senders.contains(&account_id) {
                continue;
            }
            debug!(
                "Processing vote on {:?}'s request by {:?}",
                account_id, vote.authority,
            );
            let value = vote.value;
            let aggregator = aggregators
                .entry(account_id.clone())
                .or_insert_with(|| SignatureAggregator::new(value, &committee));
            match aggregator.append(vote.authority, vote.signature) {
                Ok(Some(certificate)) => {
                    debug!("Found certificate: {:?}", certificate);
                    let buf =
                        serialize_message(&SerializedMessage::Certificate(Box::new(certificate)));
                    certificates.push((account_id.clone(), buf.into()));
                    done_senders.insert(account_id);
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

    /// Broadcast a bulk of requests to each authority.
    async fn mass_broadcast_orders(
        &self,
        phase: &'static str,
        max_in_flight: u64,
        orders: Vec<(AccountId, Bytes)>,
    ) -> Vec<Bytes> {
        let time_start = Instant::now();
        info!("Broadcasting {} {} orders", orders.len(), phase);
        let authority_clients = self.make_authority_mass_clients(max_in_flight);
        let mut streams = Vec::new();
        for (num_shards, client) in authority_clients {
            // Re-index orders by shard for this particular authority client.
            let mut sharded_requests = HashMap::new();
            for (account_id, buf) in &orders {
                let shard = network::get_shard(num_shards, account_id);
                sharded_requests
                    .entry(shard)
                    .or_insert_with(Vec::new)
                    .push(buf.clone());
            }
            streams.push(client.run(sharded_requests));
        }
        let responses = futures::stream::select_all(streams).concat().await;
        let time_elapsed = time_start.elapsed();
        warn!(
            "Received {} responses in {} ms.",
            responses.len(),
            time_elapsed.as_millis()
        );
        warn!(
            "Estimated server throughput: {} {} orders per sec",
            (orders.len() as u128) * 1_000_000 / time_elapsed.as_micros(),
            phase
        );
        responses
    }

    fn save_accounts(&self) {
        self.wallet_state
            .write(&self.wallet_state_path)
            .expect("Unable to write user accounts");
        info!("Saved user account states");
    }

    async fn update_account_from_state<A, S>(&mut self, state: &mut AccountClientState<A, S>)
    where
        A: AuthorityClient + Send + Sync + 'static + Clone,
        S: StorageClient + Clone + 'static,
    {
        self.wallet_state.update_from_state(state).await
    }
}

fn deserialize_response(response: &[u8]) -> Option<AccountInfoResponse> {
    match deserialize_message(response) {
        Ok(SerializedMessage::AccountInfoResponse(info)) => Some(*info),
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
    /// Sets the file storing the private state of user accounts (an empty one will be created if missing)
    #[structopt(long = "wallet")]
    wallet_state_path: PathBuf,

    /// Optional directory for the file storage of account public states.
    #[structopt(long = "storage")]
    storage_path: Option<PathBuf>,

    /// Optional path to the file describing the initial user accounts (aka genesis state)
    #[structopt(long = "genesis")]
    genesis_config_path: Option<PathBuf>,

    /// Sets the file describing the public configurations of all authorities
    #[structopt(long = "committee")]
    committee_config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[structopt(long, default_value = "4000000")]
    send_timeout_us: u64,

    /// Timeout for receiving responses (us)
    #[structopt(long, default_value = "4000000")]
    recv_timeout_us: u64,

    /// Maximum size of datagrams received and sent (bytes)
    #[structopt(long, default_value = transport::DEFAULT_MAX_DATAGRAM_SIZE)]
    buffer_size: usize,

    /// Time between attempts while waiting on cross-shard updates (ms)
    #[structopt(long, default_value = "4000")]
    cross_shard_delay_ms: u64,

    #[structopt(long, default_value = "10")]
    cross_shard_retries: usize,

    /// Subcommands.
    #[structopt(subcommand)]
    cmd: ClientCommands,
}

#[derive(StructOpt)]
enum ClientCommands {
    /// Transfer funds
    #[structopt(name = "transfer")]
    Transfer {
        /// Sending account id (must be one of our accounts)
        #[structopt(long = "from")]
        sender: AccountId,

        /// Recipient account id
        #[structopt(long = "to")]
        recipient: AccountId,

        /// Amount to transfer
        amount: Amount,
    },

    /// Open (i.e. activate) a new account deriving the UID from an existing one.
    #[structopt(name = "open_account")]
    OpenAccount {
        /// Sending account id (must be one of our accounts)
        #[structopt(long = "from")]
        sender: AccountId,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[structopt(long = "to-owner")]
        owner: Option<AccountOwner>,
    },

    /// Close (i.e. deactivate) an existing account. (Consider `spend_and_transfer`
    /// instead for real-life use cases.)
    #[structopt(name = "close_account")]
    CloseAccount {
        /// Sending account id (must be one of our accounts)
        #[structopt(long = "from")]
        sender: AccountId,
    },

    /// Obtain the balance of the account directly from a quorum of authorities.
    #[structopt(name = "query_balance")]
    QueryBalance {
        /// Account id
        account_id: AccountId,
    },

    /// Synchronize the local state of the account (including a conservative estimation of the
    /// available balance) with a quorum authorities.
    #[structopt(name = "sync_balance")]
    SynchronizeBalance {
        /// Account id
        account_id: AccountId,
    },

    /// Send one transfer per account in bulk mode
    #[structopt(name = "benchmark")]
    Benchmark {
        /// Maximum number of requests in flight
        #[structopt(long, default_value = "200")]
        max_in_flight: u64,

        /// Use a subset of the accounts to generate N transfers
        #[structopt(long)]
        max_orders: Option<usize>,

        /// Use server configuration files to generate certificates (instead of aggregating received votes).
        #[structopt(long)]
        server_configs: Option<Vec<String>>,
    },

    /// Create initial user accounts and print information to be used for initialization of authority setup.
    #[structopt(name = "create_genesis_config")]
    CreateGenesisConfig {
        /// Known initial balance of the account
        #[structopt(long, default_value = "0")]
        initial_funding: Balance,

        /// Number of additional accounts to create
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
            let mut client_state = context.make_account_client(sender);
            info!("Starting transfer");
            let time_start = Instant::now();
            let certificate = client_state
                .transfer_to_account(amount, recipient.clone(), UserData::default())
                .await
                .unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            context.update_account_from_state(&mut client_state).await;

            info!("Updating recipient's local account");
            context
                .update_recipient_account(certificate, None)
                .await
                .unwrap();
            context.save_accounts();
        }

        ClientCommands::OpenAccount { sender, owner } => {
            let mut client_state = context.make_account_client(sender);
            let (new_owner, key_pair) = match owner {
                Some(key) => (key, None),
                None => {
                    let key_pair = KeyPair::generate();
                    (key_pair.public(), Some(key_pair))
                }
            };
            info!("Starting operation to open a new account");
            let time_start = Instant::now();
            let certificate = client_state.open_account(new_owner).await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            println!(
                "{}",
                certificate
                    .value
                    .confirmed_request()
                    .unwrap()
                    .operation
                    .recipient()
                    .unwrap()
            );
            context.update_account_from_state(&mut client_state).await;

            info!("Updating recipient's local account");
            context
                .update_recipient_account(certificate, key_pair)
                .await
                .unwrap();
            context.save_accounts();
        }

        ClientCommands::CloseAccount { sender } => {
            let mut client_state = context.make_account_client(sender);
            info!("Starting operation to close the account");
            let time_start = Instant::now();
            let certificate = client_state.close_account().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Operation confirmed after {} us", time_total);
            info!("{:?}", certificate);
            context.update_account_from_state(&mut client_state).await;
            context.save_accounts();
        }

        ClientCommands::QueryBalance { account_id } => {
            let mut client_state = context.make_account_client(account_id);
            info!("Starting query authorities for the account balance");
            let time_start = Instant::now();
            let balance = client_state.query_safe_balance().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Balance confirmed after {} us", time_total);
            println!("{}", balance);
            context.update_account_from_state(&mut client_state).await;
            context.save_accounts();
        }

        ClientCommands::SynchronizeBalance { account_id } => {
            let mut client_state = context.make_account_client(account_id);
            info!("Synchronize account information");
            let time_start = Instant::now();
            let balance = client_state.synchronize_balance().await.unwrap();
            let time_total = time_start.elapsed().as_micros();
            info!("Account balance synchronized after {} us", time_total);
            println!("{}", balance);
            context.update_account_from_state(&mut client_state).await;
            context.save_accounts();
        }

        ClientCommands::Benchmark {
            max_in_flight,
            max_orders,
            server_configs,
        } => {
            let max_orders = max_orders.unwrap_or_else(|| context.wallet_state.num_accounts());
            warn!("Starting benchmark phase 1 (request orders)");
            let (orders, serialize_orders) = context.make_benchmark_request_orders(max_orders);
            let responses = context
                .mass_broadcast_orders("request", max_in_flight, serialize_orders)
                .await;
            let votes: Vec<_> = responses
                .into_iter()
                .filter_map(|buf| {
                    deserialize_response(&buf[..])
                        .and_then(|response| response.info.manager.pending().cloned())
                })
                .collect();
            warn!("Received {} valid votes.", votes.len());

            warn!("Starting benchmark phase 2 (confirmation orders)");
            let certificates = if let Some(files) = server_configs {
                warn!("Using server configs provided by --server-configs");
                let files = files.iter().map(AsRef::as_ref).collect();
                ClientContext::make_benchmark_certificates_from_orders_and_server_configs(
                    orders, files,
                )
            } else {
                warn!("Using committee config");
                context.make_benchmark_certificates_from_votes(votes)
            };
            let responses = context
                .mass_broadcast_orders("confirmation", max_in_flight, certificates.clone())
                .await;
            let mut confirmed = HashSet::new();
            let num_valid =
                responses
                    .iter()
                    .fold(0, |acc, buf| match deserialize_response(&buf[..]) {
                        Some(response) => {
                            confirmed.insert(response.info.account_id);
                            acc + 1
                        }
                        None => acc,
                    });
            warn!(
                "Received {} valid confirmations for {} requests.",
                num_valid,
                confirmed.len()
            );

            warn!("Updating local state of user accounts");
            context.save_accounts();
        }

        ClientCommands::CreateGenesisConfig {
            initial_funding,
            num,
        } => {
            let mut genesis_config = GenesisConfig::default();
            for i in 0..num {
                // Create keys.
                let account =
                    UserAccount::make_initial(AccountId::new(vec![SequenceNumber::from(i as u64)]));
                // Public "genesis" state.
                genesis_config.accounts.push((
                    account.account_id.clone(),
                    account.key_pair.as_ref().unwrap().public(),
                    initial_funding,
                ));
                // Private keys.
                context.wallet_state.insert(account);
            }
            context.save_accounts();
            let path = options
                .genesis_config_path
                .as_ref()
                .expect("--genesis should be set");
            genesis_config.write(path).unwrap();
        }
    }
}
