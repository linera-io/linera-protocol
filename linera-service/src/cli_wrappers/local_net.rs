// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cli_wrappers::{ClientWrapper, LineraNet, LineraNetConfig, Network},
    util::ChildExt,
};
use std::path::Path;
use std::path::PathBuf;
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use linera_base::{
    command::{resolve_binary, CommandExt},
    data_types::Amount,
};
use linera_execution::ResourceControlPolicy;
use linera_storage_service::{
    child::StorageServiceGuard,
    common::ServiceStoreConfig,
};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tonic_health::pb::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::{info, warn};

#[cfg(any(test, feature = "test"))]
use {
    async_lock::RwLock,
    linera_base::sync::Lazy,
    std::ops::Deref,
    tempfile::tempdir,
    linera_storage_service::child::{get_free_port, StorageService},
    linera_storage_service::{
        client::service_config_from_endpoint,
        common::get_service_storage_binary,
    },
};

#[cfg(all(feature = "rocksdb", any(test, feature = "test")))]
use linera_views::rocks_db::create_rocks_db_test_config;

#[cfg(all(feature = "aws", any(test, feature = "test")))]
use linera_views::dynamo_db::create_dynamo_db_test_config;

#[cfg(all(feature = "scylladb", any(test, feature = "test")))]
use linera_views::scylla_db::create_scylla_db_test_config;

#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::RocksDbStoreConfig;

#[cfg(feature = "aws")]
use linera_views::dynamo_db::DynamoDbStoreConfig;

#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbStoreConfig;


trait LocalServerInternal: Sized {
    type Config;

    #[cfg(any(test, feature = "test"))]
    async fn new_test() -> Result<Self>;

    fn get_config(&self) -> Self::Config;
}

struct LocalServerServiceInternal {
    service_config: ServiceStoreConfig,
    _service_guard: StorageServiceGuard,
}

impl LocalServerInternal for LocalServerServiceInternal {
    type Config = ServiceStoreConfig;

    #[cfg(any(test, feature = "test"))]
    async fn new_test() -> Result<Self> {
        let endpoint = get_free_port().await.unwrap();
        let service_config = service_config_from_endpoint(&endpoint)?;
        let binary = get_service_storage_binary().await?.display().to_string();
        let service = StorageService::new(&endpoint, binary);
        let _service_guard = service.run().await?;
        Ok(Self {
            service_config,
            _service_guard,
        })
    }

    fn get_config(&self) -> Self::Config {
        self.service_config.clone()
    }
}

#[cfg(feature = "rocksdb")]
struct LocalServerRocksDbInternal {
    rocks_db_config: RocksDbStoreConfig,
    _temp_dir: Option<TempDir>,
}

#[cfg(feature = "rocksdb")]
impl LocalServerInternal for LocalServerRocksDbInternal {
    type Config = RocksDbStoreConfig;

    #[cfg(any(test, feature = "test"))]
    async fn new_test() -> Result<Self> {
        let (rocks_db_config, temp_dir) = create_rocks_db_test_config().await;
        let _temp_dir = Some(temp_dir);
        Ok(Self {
            rocks_db_config,
            _temp_dir,
        })
    }

    fn get_config(&self) -> Self::Config {
        self.rocks_db_config.clone()
    }
}

#[cfg(any(test, feature = "test"))]
struct LocalServer<L> {
    internal_server: RwLock<Option<L>>,
}

#[cfg(any(test, feature = "test"))]
impl<L> Default for LocalServer<L>
where
    L: LocalServerInternal,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test"))]
impl<L> LocalServer<L>
where
    L: LocalServerInternal,
{
    pub fn new() -> Self {
        Self {
            internal_server: RwLock::new(None),
        }
    }

    pub async fn get_config(&self) -> L::Config {
        let mut server = self.internal_server.write().await;
        if server.is_none() {
            *server = Some(L::new_test().await.expect("local server"));
        }
        let Some(internal_server) = server.deref() else {
            unreachable!();
        };
        internal_server.get_config()
    }
}

// A static data to store the integration test server
#[cfg(any(test, feature = "test"))]
static LOCAL_SERVER_SERVICE: Lazy<LocalServer<LocalServerServiceInternal>> =
    Lazy::new(LocalServer::new);

#[cfg(all(feature = "rocksdb", any(test, feature = "test")))]
static LOCAL_SERVER_ROCKS_DB: Lazy<LocalServer<LocalServerRocksDbInternal>> =
    Lazy::new(LocalServer::new);

/// A server config to be used for the construction of the config.
#[derive(Debug)]
pub enum LocalServerConfig {
    Service {
        service_config: ServiceStoreConfig,
    },
    #[cfg(feature = "rocksdb")]
    RocksDb {
        rocks_db_config: RocksDbStoreConfig,
    },
    #[cfg(feature = "aws")]
    DynamoDb {
        dynamo_db_config: DynamoDbStoreConfig,
    },
    #[cfg(feature = "scylladb")]
    ScyllaDb {
        scylla_db_config: ScyllaDbStoreConfig,
    },
}

impl LocalServerConfig {
    #[cfg(any(test, feature = "test"))]
    async fn make_testing_config(database: Database) -> Self {
        match database {
            Database::Service => {
                let service_config = LOCAL_SERVER_SERVICE.get_config().await;
                LocalServerConfig::Service { service_config }
            }
            #[cfg(feature = "rocksdb")]
            Database::RocksDb => {
                let rocks_db_config = LOCAL_SERVER_ROCKS_DB.get_config().await;
                LocalServerConfig::RocksDb { rocks_db_config }
            }
            #[cfg(feature = "aws")]
            Database::DynamoDb => {
                let dynamo_db_config = create_dynamo_db_test_config().await;
                LocalServerConfig::DynamoDb { dynamo_db_config }
            }
            #[cfg(feature = "scylladb")]
            Database::ScyllaDb => {
                let scylla_db_config = create_scylla_db_test_config().await;
                LocalServerConfig::ScyllaDb { scylla_db_config }
            }
        }
    }
}

pub enum LocalServerConfigBuilder {
    #[cfg(any(test, feature = "test"))]
    TestConfig,
    ExistingConfig {
        local_server_config: LocalServerConfig,
    }
}

impl LocalServerConfigBuilder {
    #[allow(unused_variables)]
    pub async fn build(self, database: Database) -> LocalServerConfig {
        match self {
            #[cfg(any(test, feature = "test"))]
            LocalServerConfigBuilder::TestConfig => {
                LocalServerConfig::make_testing_config(database).await
            },
            LocalServerConfigBuilder::ExistingConfig{ local_server_config } => {
                local_server_config
            }
        }
    }
}

/// Path used for the run can come from a path whose lifetime is controlled
/// by an external user or as a temporary directory
#[derive(Clone)]
pub enum PathProvider {
    ExternalPath { path_buf: PathBuf },
    TemporaryDirectory { tmp_dir: Arc<TempDir> },
}

impl PathProvider {
    pub fn path(&self) -> &Path {
        match self {
            PathProvider::ExternalPath { path_buf } => {
                path_buf.as_path()
            },
            PathProvider::TemporaryDirectory { tmp_dir } => {
                tmp_dir.path()
            },
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_test() -> Result<Self> {
        let tmp_dir = Arc::new(tempdir()?);
        Ok(PathProvider::TemporaryDirectory { tmp_dir })
    }

    pub fn new(path: &Path) -> Self {
        let path_buf = path.to_path_buf();
        PathProvider::ExternalPath { path_buf }
    }
}





/// The information needed to start a [`LocalNet`].
pub struct LocalNetConfig {
    pub database: Database,
    pub network: Network,
    pub testing_prng_seed: Option<u64>,
    pub table_name: String,
    pub num_other_initial_chains: u32,
    pub initial_amount: Amount,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub policy: ResourceControlPolicy,
    pub server_config_builder: LocalServerConfigBuilder,
    pub path_provider: PathProvider,
}

/// A set of Linera validators running locally as native processes.
pub struct LocalNet {
    database: Database,
    network: Network,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    num_initial_validators: usize,
    num_shards: usize,
    validator_names: BTreeMap<usize, String>,
    running_validators: BTreeMap<usize, Validator>,
    table_name: String,
    set_init: HashSet<(usize, usize)>,
    server_config: LocalServerConfig,
    path_provider: PathProvider,
}

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the binary when starting a server.
const SERVER_ENV: &str = "LINERA_SERVER_PARAMS";

/// Description of the database engine to use inside a local Linera network.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Database {
    Service,
    #[cfg(feature = "rocksdb")]
    RocksDb,
    #[cfg(feature = "aws")]
    DynamoDb,
    #[cfg(feature = "scylladb")]
    ScyllaDb,
}

/// The processes of a running validator.
struct Validator {
    proxy: Child,
    servers: Vec<Child>,
}

impl Validator {
    fn new(proxy: Child) -> Self {
        Self {
            proxy,
            servers: vec![],
        }
    }

    async fn terminate(&mut self) -> Result<()> {
        self.proxy
            .kill()
            .await
            .context("terminating validator proxy")?;
        for server in &mut self.servers {
            server
                .kill()
                .await
                .context("terminating validator server")?;
        }
        Ok(())
    }

    fn add_server(&mut self, server: Child) {
        self.servers.push(server)
    }

    #[cfg(any(test, feature = "test"))]
    async fn terminate_server(&mut self, index: usize) -> Result<()> {
        let mut server = self.servers.remove(index);
        server
            .kill()
            .await
            .context("terminating validator server")?;
        Ok(())
    }

    fn ensure_is_running(&mut self) -> Result<()> {
        self.proxy.ensure_is_running()?;
        for child in &mut self.servers {
            child.ensure_is_running()?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl LocalNetConfig {
    pub fn new_test(database: Database, network: Network) -> Self {
        let num_shards = match database {
            #[cfg(feature = "rocksdb")]
            Database::RocksDb => 1,
            _ => 4,
        };
        let server_config_builder = LocalServerConfigBuilder::TestConfig;
        let path_provider = PathProvider::new_test().unwrap();
        Self {
            database,
            network,
            num_other_initial_chains: 10,
            initial_amount: Amount::from_tokens(1_000_000),
            policy: ResourceControlPolicy::devnet(),
            testing_prng_seed: Some(37),
            table_name: linera_views::test_utils::generate_test_namespace(),
            num_initial_validators: 4,
            num_shards,
            server_config_builder,
            path_provider,
        }
    }
}

#[async_trait]
impl LineraNetConfig for LocalNetConfig {
    type Net = LocalNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let server_config = self.server_config_builder.build(self.database).await;
        #[cfg(feature = "rocksdb")]
        ensure!(
            self.num_shards == 1 || self.database != Database::RocksDb,
            "Multiple shards not supported with RocksDB"
        );
        let mut net = LocalNet::new(
            self.database,
            self.network,
            self.testing_prng_seed,
            self.table_name,
            self.num_initial_validators,
            self.num_shards,
            server_config,
            self.path_provider,
        )?;
        let client = net.make_client().await;
        ensure!(
            self.num_initial_validators > 0,
            "There should be at least one initial validator"
        );
        net.generate_initial_validator_config().await.unwrap();
        client
            .create_genesis_config(
                self.num_other_initial_chains,
                self.initial_amount,
                self.policy,
            )
            .await
            .unwrap();
        net.run().await.unwrap();
        Ok((net, client))
    }

    async fn policy(&self) -> ResourceControlPolicy {
        self.policy.clone()
    }
}

#[async_trait]
impl LineraNet for LocalNet {
    async fn ensure_is_running(&mut self) -> Result<()> {
        for validator in self.running_validators.values_mut() {
            validator.ensure_is_running().context("in local network")?;
        }
        Ok(())
    }

    async fn make_client(&mut self) -> ClientWrapper {
        let client = ClientWrapper::new(
            self.path_provider.clone(),
            self.network,
            self.testing_prng_seed,
            self.next_client_id,
        );
        if let Some(seed) = self.testing_prng_seed {
            self.testing_prng_seed = Some(seed + 1);
        }
        self.next_client_id += 1;
        client
    }

    async fn terminate(&mut self) -> Result<()> {
        for validator in self.running_validators.values_mut() {
            validator.terminate().await.context("in local network")?
        }
        Ok(())
    }
}

impl LocalNet {
    fn new(
        database: Database,
        network: Network,
        testing_prng_seed: Option<u64>,
        table_name: String,
        num_initial_validators: usize,
        num_shards: usize,
        server_config: LocalServerConfig,
        path_provider: PathProvider,
    ) -> Result<Self> {
        Ok(Self {
            database,
            network,
            testing_prng_seed,
            next_client_id: 0,
            num_initial_validators,
            num_shards,
            validator_names: BTreeMap::new(),
            running_validators: BTreeMap::new(),
            table_name,
            set_init: HashSet::new(),
            server_config,
            path_provider,
        })
    }

    async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = resolve_binary(name, env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command.current_dir(self.path_provider.path());
        Ok(command)
    }

    pub fn proxy_port(validator: usize) -> usize {
        9000 + validator * 100
    }

    fn shard_port(validator: usize, shard: usize) -> usize {
        9000 + validator * 100 + shard + 1
    }

    fn internal_port(validator: usize) -> usize {
        10000 + validator * 100
    }

    fn proxy_metrics_port(validator: usize) -> usize {
        11000 + validator * 100
    }

    fn shard_metrics_port(validator: usize, shard: usize) -> usize {
        11000 + validator * 100 + shard + 1
    }

    fn configuration_string(&self, server_number: usize) -> Result<String> {
        let n = server_number;
        let path = self.path_provider.path().join(format!("validator_{n}.toml"));
        let port = Self::proxy_port(n);
        let internal_port = Self::internal_port(n);
        let metrics_port = Self::proxy_metrics_port(n);
        let external_protocol = self.network.external();
        let internal_protocol = self.network.internal();
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "127.0.0.1"
                port = {port}
                internal_host = "127.0.0.1"
                internal_port = {internal_port}
                metrics_host = "127.0.0.1"
                metrics_port = {metrics_port}
                external_protocol = {external_protocol}
                internal_protocol = {internal_protocol}
            "#
        );
        for k in 0..self.num_shards {
            let shard_port = Self::shard_port(n, k);
            let shard_metrics_port = Self::shard_metrics_port(n, k);
            content.push_str(&format!(
                r#"

                [[shards]]
                host = "127.0.0.1"
                port = {shard_port}
                metrics_host = "127.0.0.1"
                metrics_port = {shard_metrics_port}
                "#
            ));
        }
        fs_err::write(&path, content)?;
        path.into_os_string().into_string().map_err(|error| {
            anyhow!(
                "could not parse OS string into string: {}",
                error.to_string_lossy()
            )
        })
    }

    async fn generate_initial_validator_config(&mut self) -> Result<()> {
        let mut command = self.command_for_binary("linera-server").await?;
        command.arg("generate");
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
            self.testing_prng_seed = Some(seed + 1);
        }
        command.arg("--validators");
        for i in 0..self.num_initial_validators {
            command.arg(&self.configuration_string(i)?);
        }
        let output = command
            .args(["--committee", "committee.json"])
            .spawn_and_wait_for_stdout()
            .await?;
        self.validator_names = output
            .split_whitespace()
            .map(str::to_string)
            .enumerate()
            .collect();
        Ok(())
    }

    async fn run_proxy(&self, validator: usize) -> Result<Child> {
        let child = self
            .command_for_binary("linera-proxy")
            .await?
            .arg(format!("server_{}.json", validator))
            .spawn_into()?;

        match self.network {
            Network::Grpc => {
                let port = Self::proxy_port(validator);
                let nickname = format!("validator proxy {validator}");
                Self::ensure_grpc_server_has_started(&nickname, port).await?;
            }
            Network::Tcp | Network::Udp => {
                info!("Letting validator proxy {validator} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        Ok(child)
    }

    async fn ensure_grpc_server_has_started(nickname: &str, port: usize) -> Result<()> {
        let connection = tonic::transport::Endpoint::new(format!("http://127.0.0.1:{port}"))
            .context("endpoint should always parse")?
            .connect_lazy();
        let mut client = HealthClient::new(connection);
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let result = client.check(HealthCheckRequest::default()).await;
            if result.is_ok() && result.unwrap().get_ref().status() == ServingStatus::Serving {
                info!("Successfully started {nickname}");
                return Ok(());
            } else {
                warn!("Waiting for {nickname} to start");
            }
        }
        bail!("Failed to start {nickname}");
    }

    async fn run_server(&mut self, validator: usize, shard: usize) -> Result<Child> {
        let shard_str = match self.database {
            #[cfg(feature = "rocksdb")]
            Database::RocksDb => format!("_{}", shard),
            _ => String::new(),
        };
        let namespace = format!("{}_server_{}{}_db", self.table_name, validator, shard_str);
        let key = match self.database {
            #[cfg(feature = "rocksdb")]
            Database::RocksDb => (validator, shard),
            _ => (validator, 0),
        };
        let storage = match self.database {
            Database::Service => {
                let LocalServerConfig::Service { service_config } =
                    &self.server_config
                else {
                    unreachable!();
                };
                let endpoint = &service_config.endpoint;
                format!("service:{}:{}", endpoint, namespace)
            }
            #[cfg(feature = "rocksdb")]
            Database::RocksDb => {
                let LocalServerConfig::RocksDb { rocks_db_config } =
                    &self.server_config
                else {
                    unreachable!();
                };
                let path_buf = rocks_db_config.path_buf.to_str().unwrap();
                format!("rocksdb:{}:{}", path_buf, namespace)
            }
            #[cfg(feature = "aws")]
            Database::DynamoDb => {
                format!("dynamodb:{}:localstack", namespace)
            }
            #[cfg(feature = "scylladb")]
            Database::ScyllaDb => {
                format!("scylladb:{}", namespace)
            }
        };
        if !self.set_init.contains(&key) {
            let max_try = 4;
            let mut i_try = 0;
            loop {
                let mut command = self.command_for_binary("linera-server").await?;
                if let Ok(var) = env::var(SERVER_ENV) {
                    command.args(var.split_whitespace());
                }
                command.arg("initialize");
                let result = command
                    .args(["--storage", &storage])
                    .args(["--genesis", "genesis.json"])
                    .spawn_and_wait_for_stdout()
                    .await;
                if result.is_ok() {
                    break;
                }
                warn!(
                    "Failed to initialize storage={} using linera-server, i_try={}, error={:?}",
                    storage, i_try, result
                );
                i_try += 1;
                if i_try == max_try {
                    bail!("Failed to initialize after {} attempts", max_try);
                }
                let one_second = std::time::Duration::from_secs(1);
                std::thread::sleep(one_second);
            }
            self.set_init.insert(key);
        }

        let mut command = self.command_for_binary("linera-server").await?;
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        command.arg("run");
        let child = command
            .args(["--storage", &storage])
            .args(["--server", &format!("server_{}.json", validator)])
            .args(["--shard", &shard.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn_into()?;

        match self.network {
            Network::Grpc => {
                let port = Self::shard_port(validator, shard);
                let nickname = format!("validator server {validator}:{shard}");
                Self::ensure_grpc_server_has_started(&nickname, port).await?;
            }
            Network::Tcp | Network::Udp => {
                info!("Letting validator server {validator}:{shard} start");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        Ok(child)
    }

    async fn run(&mut self) -> Result<()> {
        for validator in 0..self.num_initial_validators {
            self.start_validator(validator).await?;
        }
        Ok(())
    }

    pub async fn start_validator(&mut self, validator: usize) -> Result<()> {
        let proxy = self.run_proxy(validator).await?;
        let mut validator_proxy = Validator::new(proxy);
        for shard in 0..self.num_shards {
            let server = self.run_server(validator, shard).await?;
            validator_proxy.add_server(server);
        }
        self.running_validators.insert(validator, validator_proxy);
        Ok(())
    }
}

#[cfg(any(test, feature = "test"))]
impl LocalNet {
    pub fn validator_name(&self, validator: usize) -> Option<&String> {
        self.validator_names.get(&validator)
    }

    pub async fn generate_validator_config(&mut self, validator: usize) -> Result<()> {
        let stdout = self
            .command_for_binary("linera-server")
            .await?
            .arg("generate")
            .arg("--validators")
            .arg(&self.configuration_string(validator)?)
            .spawn_and_wait_for_stdout()
            .await?;
        self.validator_names
            .insert(validator, stdout.trim().to_string());
        Ok(())
    }

    pub async fn terminate_server(&mut self, validator: usize, shard: usize) -> Result<()> {
        self.running_validators
            .get_mut(&validator)
            .context("server not found")?
            .terminate_server(shard)
            .await?;
        Ok(())
    }

    pub fn remove_validator(&mut self, validator: usize) -> Result<()> {
        self.running_validators
            .remove(&validator)
            .context("validator not found")?;
        Ok(())
    }

    pub async fn start_server(&mut self, validator: usize, shard: usize) -> Result<()> {
        let server = self.run_server(validator, shard).await?;
        self.running_validators
            .get_mut(&validator)
            .context("could not find server")?
            .add_server(server);
        Ok(())
    }
}
