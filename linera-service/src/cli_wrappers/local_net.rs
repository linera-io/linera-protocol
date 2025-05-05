// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::sync::LazyLock;
use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail, ensure, Context, Result};
#[cfg(with_testing)]
use async_lock::RwLock;
use async_trait::async_trait;
use linera_base::{
    command::{resolve_binary, CommandExt},
    data_types::Amount,
};
use linera_client::client_options::ResourceControlPolicyConfig;
use linera_core::node::ValidatorNodeProvider;
use linera_rpc::config::CrossChainConfig;
#[cfg(all(feature = "storage-service", with_testing))]
use linera_storage_service::common::storage_service_test_endpoint;
#[cfg(all(feature = "rocksdb", feature = "scylladb", with_testing))]
use linera_views::rocks_db::{RocksDbSpawnMode, RocksDbStore};
#[cfg(all(feature = "scylladb", with_testing))]
use linera_views::{scylla_db::ScyllaDbStore, store::TestKeyValueStore as _};
use tempfile::{tempdir, TempDir};
use tokio::process::{Child, Command};
use tonic::transport::{channel::ClientTlsConfig, Endpoint};
use tonic_health::pb::{
    health_check_response::ServingStatus, health_client::HealthClient, HealthCheckRequest,
};
use tracing::{error, info, warn};

use crate::{
    cli_wrappers::{
        ClientWrapper, LineraNet, LineraNetConfig, Network, NetworkConfig, OnClientDrop,
    },
    storage::{StorageConfig, StorageConfigNamespace},
    util::ChildExt,
};

pub enum ProcessInbox {
    Skip,
    Automatic,
}

#[cfg(with_testing)]
static PORT_PROVIDER: LazyLock<RwLock<u16>> = LazyLock::new(|| RwLock::new(7080));

/// Provides a port for the node service. Increment the port numbers.
#[cfg(with_testing)]
pub async fn get_node_port() -> u16 {
    let mut port = PORT_PROVIDER.write().await;
    let port_ret = *port;
    *port += 1;
    info!("get_node_port returning port_ret={}", port_ret);
    assert!(port_selector::is_free(port_ret));
    port_ret
}

#[cfg(with_testing)]
async fn make_testing_config(database: Database) -> Result<StorageConfig> {
    match database {
        Database::Service => {
            #[cfg(feature = "storage-service")]
            {
                let endpoint = storage_service_test_endpoint()
                    .expect("Reading LINERA_STORAGE_SERVICE environment variable");
                Ok(StorageConfig::Service { endpoint })
            }
            #[cfg(not(feature = "storage-service"))]
            panic!("Database::Service is selected without the feature storage_service");
        }
        Database::DynamoDb => {
            #[cfg(feature = "dynamodb")]
            {
                let use_dynamodb_local = true;
                Ok(StorageConfig::DynamoDb { use_dynamodb_local })
            }
            #[cfg(not(feature = "dynamodb"))]
            panic!("Database::DynamoDb is selected without the feature dynamodb");
        }
        Database::ScyllaDb => {
            #[cfg(feature = "scylladb")]
            {
                let config = ScyllaDbStore::new_test_config().await?;
                Ok(StorageConfig::ScyllaDb {
                    uri: config.inner_config.uri,
                })
            }
            #[cfg(not(feature = "scylladb"))]
            panic!("Database::ScyllaDb is selected without the feature scylladb");
        }
        Database::DualRocksDbScyllaDb => {
            #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
            {
                let rocksdb_config = RocksDbStore::new_test_config().await?;
                let scylla_config = ScyllaDbStore::new_test_config().await?;
                let spawn_mode = RocksDbSpawnMode::get_spawn_mode_from_runtime();
                Ok(StorageConfig::DualRocksDbScyllaDb {
                    path_with_guard: rocksdb_config.inner_config.path_with_guard,
                    spawn_mode,
                    uri: scylla_config.inner_config.uri,
                })
            }
            #[cfg(not(all(feature = "rocksdb", feature = "scylladb")))]
            panic!("Database::DualRocksDbScyllaDb is selected without the features rocksdb and scylladb");
        }
    }
}

pub enum StorageConfigBuilder {
    #[cfg(with_testing)]
    TestConfig,
    ExistingConfig {
        storage_config: StorageConfig,
    },
}

impl StorageConfigBuilder {
    #[allow(unused_variables)]
    pub async fn build(self, database: Database) -> Result<StorageConfig> {
        match self {
            #[cfg(with_testing)]
            StorageConfigBuilder::TestConfig => make_testing_config(database).await,
            StorageConfigBuilder::ExistingConfig { storage_config } => Ok(storage_config),
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
            PathProvider::ExternalPath { path_buf } => path_buf.as_path(),
            PathProvider::TemporaryDirectory { tmp_dir } => tmp_dir.path(),
        }
    }

    pub fn create_temporary_directory() -> Result<Self> {
        let tmp_dir = Arc::new(tempdir()?);
        Ok(PathProvider::TemporaryDirectory { tmp_dir })
    }

    pub fn new(path: &Option<String>) -> anyhow::Result<Self> {
        Ok(match path {
            None => {
                let tmp_dir = Arc::new(tempfile::tempdir()?);
                PathProvider::TemporaryDirectory { tmp_dir }
            }
            Some(path) => {
                let path = Path::new(path);
                let path_buf = path.to_path_buf();
                PathProvider::ExternalPath { path_buf }
            }
        })
    }
}

/// The information needed to start a [`LocalNet`].
pub struct LocalNetConfig {
    pub database: Database,
    pub network: NetworkConfig,
    pub testing_prng_seed: Option<u64>,
    pub namespace: String,
    pub num_other_initial_chains: u32,
    pub initial_amount: Amount,
    pub num_initial_validators: usize,
    pub num_shards: usize,
    pub policy_config: ResourceControlPolicyConfig,
    pub cross_chain_config: CrossChainConfig,
    pub storage_config_builder: StorageConfigBuilder,
    pub path_provider: PathProvider,
    pub num_block_exporters: u32,
}

/// A set of Linera validators running locally as native processes.
pub struct LocalNet {
    network: NetworkConfig,
    testing_prng_seed: Option<u64>,
    next_client_id: usize,
    num_initial_validators: usize,
    num_shards: usize,
    validator_keys: BTreeMap<usize, (String, String)>,
    running_validators: BTreeMap<usize, Validator>,
    initialized_validator_storages: BTreeMap<usize, StorageConfigNamespace>,
    common_namespace: String,
    common_storage_config: StorageConfig,
    cross_chain_config: CrossChainConfig,
    path_provider: PathProvider,
    num_block_exporters: u32,
}

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the binary when starting a server.
const SERVER_ENV: &str = "LINERA_SERVER_PARAMS";

/// Description of the database engine to use inside a local Linera network.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Database {
    Service,
    DynamoDb,
    ScyllaDb,
    DualRocksDbScyllaDb,
}

/// The processes of a running validator.
struct Validator {
    proxy: Child,
    servers: Vec<Child>,
    exporters: Vec<Child>,
}

impl Validator {
    fn new(proxy: Child) -> Self {
        Self {
            proxy,
            servers: vec![],
            exporters: vec![],
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

    fn add_block_exporter(&mut self, exporter: Child) {
        self.exporters.push(exporter);
    }

    #[cfg(with_testing)]
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

        for exporter in &mut self.exporters {
            exporter.ensure_is_running()?;
        }

        Ok(())
    }
}

#[cfg(with_testing)]
impl LocalNetConfig {
    pub fn new_test(database: Database, network: Network) -> Self {
        let num_shards = 4;
        let storage_config_builder = StorageConfigBuilder::TestConfig;
        let path_provider = PathProvider::create_temporary_directory().unwrap();
        let internal = network.drop_tls();
        let external = network;
        let network = NetworkConfig { internal, external };
        let cross_chain_config = CrossChainConfig::default();
        Self {
            database,
            network,
            num_other_initial_chains: 2,
            initial_amount: Amount::from_tokens(1_000_000),
            policy_config: ResourceControlPolicyConfig::Testnet,
            cross_chain_config,
            testing_prng_seed: Some(37),
            namespace: linera_views::random::generate_test_namespace(),
            num_initial_validators: 4,
            num_shards,
            storage_config_builder,
            path_provider,
            num_block_exporters: 0,
        }
    }
}

#[async_trait]
impl LineraNetConfig for LocalNetConfig {
    type Net = LocalNet;

    async fn instantiate(self) -> Result<(Self::Net, ClientWrapper)> {
        let storage_config = self.storage_config_builder.build(self.database).await?;
        let mut net = LocalNet::new(
            self.network,
            self.testing_prng_seed,
            self.namespace,
            self.num_initial_validators,
            self.num_shards,
            storage_config,
            self.cross_chain_config,
            self.path_provider,
            self.num_block_exporters,
        )?;
        let client = net.make_client().await;
        ensure!(
            self.num_initial_validators > 0,
            "There should be at least one initial validator"
        );
        net.generate_initial_validator_config().await?;
        client
            .create_genesis_config(
                self.num_other_initial_chains,
                self.initial_amount,
                self.policy_config,
                Some(vec!["localhost".to_owned()]),
            )
            .await?;
        net.run().await?;
        Ok((net, client))
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
            self.network.external,
            self.testing_prng_seed,
            self.next_client_id,
            OnClientDrop::LeakChains,
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
    #[expect(clippy::too_many_arguments)]
    fn new(
        network: NetworkConfig,
        testing_prng_seed: Option<u64>,
        common_namespace: String,
        num_initial_validators: usize,
        num_shards: usize,
        common_storage_config: StorageConfig,
        cross_chain_config: CrossChainConfig,
        path_provider: PathProvider,
        num_block_exporters: u32,
    ) -> Result<Self> {
        Ok(Self {
            network,
            testing_prng_seed,
            next_client_id: 0,
            num_initial_validators,
            num_shards,
            validator_keys: BTreeMap::new(),
            running_validators: BTreeMap::new(),
            initialized_validator_storages: BTreeMap::new(),
            common_namespace,
            common_storage_config,
            cross_chain_config,
            path_provider,
            num_block_exporters,
        })
    }

    async fn command_for_binary(&self, name: &'static str) -> Result<Command> {
        let path = resolve_binary(name, env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command.current_dir(self.path_provider.path());
        Ok(command)
    }

    #[cfg(with_testing)]
    pub fn genesis_config(&self) -> Result<linera_client::config::GenesisConfig> {
        let path = self.path_provider.path();
        crate::util::read_json(path.join("genesis.json"))
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

    fn block_exporter_port(validator: usize, exporter_id: usize) -> usize {
        12000 + validator * 100 + exporter_id + 1
    }

    fn configuration_string(&self, server_number: usize) -> Result<String> {
        let n = server_number;
        let path = self
            .path_provider
            .path()
            .join(format!("validator_{n}.toml"));
        let port = Self::proxy_port(n);
        let internal_port = Self::internal_port(n);
        let metrics_port = Self::proxy_metrics_port(n);
        let external_protocol = self.network.external.toml();
        let internal_protocol = self.network.internal.toml();
        let external_host = self.network.external.localhost();
        let internal_host = self.network.internal.localhost();
        let mut content = format!(
            r#"
                server_config_path = "server_{n}.json"
                host = "{external_host}"
                port = {port}
                internal_host = "{internal_host}"
                internal_port = {internal_port}
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
                host = "{internal_host}"
                port = {shard_port}
                metrics_port = {shard_metrics_port}
                "#
            ));
        }

        for j in 0..self.num_block_exporters {
            let host = Network::Grpc.localhost();
            let port = Self::block_exporter_port(n, j as usize);
            let config_content = format!(
                r#"

                [[block_exporters]]
                host = "{host}"
                port = {port}
                "#
            );

            content.push_str(&config_content);
            let exporter_config = self.generate_block_exporter_config(n, j);
            let config_path = self
                .path_provider
                .path()
                .join(format!("exporter_config_{n}:{j}.toml"));

            fs_err::write(&config_path, &exporter_config)?;
        }

        fs_err::write(&path, content)?;
        path.into_os_string().into_string().map_err(|error| {
            anyhow!(
                "could not parse OS string into string: {}",
                error.to_string_lossy()
            )
        })
    }

    fn generate_block_exporter_config(&self, validator: usize, exporter_id: u32) -> String {
        let n = validator;
        let host = Network::Grpc.localhost();
        let port = Self::block_exporter_port(n, exporter_id as usize);
        let config = format!(
            r#"
            id = {exporter_id}

            [service_config]
            host = "{host}"
            port = {port}
            "#
        );

        config
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
        self.validator_keys = output
            .split_whitespace()
            .map(str::to_string)
            .map(|keys| keys.split(',').map(str::to_string).collect::<Vec<_>>())
            .enumerate()
            .map(|(i, keys)| {
                let validator_key = keys[0].to_string();
                let account_key = keys[1].to_string();
                (i, (validator_key, account_key))
            })
            .collect();
        Ok(())
    }

    async fn run_proxy(&mut self, validator: usize) -> Result<Child> {
        let storage = self
            .initialized_validator_storages
            .get(&validator)
            .expect("initialized storage");
        let child = self
            .command_for_binary("linera-proxy")
            .await?
            .arg(format!("server_{}.json", validator))
            .args(["--storage", &storage.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn_into()?;

        let port = Self::proxy_port(validator);
        let nickname = format!("validator proxy {validator}");
        match self.network.external {
            Network::Grpc => {
                Self::ensure_grpc_server_has_started(&nickname, port, "http").await?;
            }
            Network::Grpcs => {
                Self::ensure_grpc_server_has_started(&nickname, port, "https").await?;
            }
            Network::Tcp => {
                Self::ensure_simple_server_has_started(&nickname, port, "tcp").await?;
            }
            Network::Udp => {
                Self::ensure_simple_server_has_started(&nickname, port, "udp").await?;
            }
        }
        Ok(child)
    }

    async fn run_exporter(&mut self, validator: usize, exporter_id: u32) -> Result<Child> {
        let config_path = format!("exporter_config_{validator}:{exporter_id}.toml");
        let storage = self
            .initialized_validator_storages
            .get(&validator)
            .expect("initialized storage");

        let child = self
            .command_for_binary("linera-exporter")
            .await?
            .arg(config_path)
            .args(["--storage", &storage.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn_into()?;

        match self.network.internal {
            Network::Grpc => {
                let port = Self::block_exporter_port(validator, exporter_id as usize);
                let nickname = format!("block exporter {validator}:{exporter_id}");
                Self::ensure_grpc_server_has_started(&nickname, port, "http").await?;
            }
            Network::Grpcs => {
                let port = Self::block_exporter_port(validator, exporter_id as usize);
                let nickname = format!("block exporter  {validator}:{exporter_id}");
                Self::ensure_grpc_server_has_started(&nickname, port, "https").await?;
            }
            Network::Tcp | Network::Udp => {
                unreachable!("Only allowed options are grpc and grpcs")
            }
        }

        Ok(child)
    }

    pub async fn ensure_grpc_server_has_started(
        nickname: &str,
        port: usize,
        scheme: &str,
    ) -> Result<()> {
        let endpoint = match scheme {
            "http" => Endpoint::new(format!("http://localhost:{port}"))
                .context("endpoint should always parse")?,
            "https" => {
                use linera_rpc::CERT_PEM;
                let certificate = tonic::transport::Certificate::from_pem(CERT_PEM);
                let tls_config = ClientTlsConfig::new().ca_certificate(certificate);
                Endpoint::new(format!("https://localhost:{port}"))
                    .context("endpoint should always parse")?
                    .tls_config(tls_config)?
            }
            _ => bail!("Only supported scheme are http and https"),
        };
        let connection = endpoint.connect_lazy();
        let mut client = HealthClient::new(connection);
        linera_base::time::timer::sleep(Duration::from_millis(100)).await;
        for i in 0..10 {
            linera_base::time::timer::sleep(Duration::from_millis(i * 500)).await;
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

    pub async fn ensure_simple_server_has_started(
        nickname: &str,
        port: usize,
        protocol: &str,
    ) -> Result<()> {
        use linera_core::node::ValidatorNode as _;

        let options = linera_rpc::NodeOptions {
            send_timeout: Duration::from_secs(5),
            recv_timeout: Duration::from_secs(5),
            retry_delay: Duration::from_secs(1),
            max_retries: 1,
        };
        let provider = linera_rpc::simple::SimpleNodeProvider::new(options);
        let address = format!("{protocol}:127.0.0.1:{port}");
        // All "simple" services (i.e. proxy and "server") are based on `RpcMessage` and
        // support `VersionInfoQuery`.
        let node = provider.make_node(&address)?;
        linera_base::time::timer::sleep(Duration::from_millis(100)).await;
        for i in 0..10 {
            linera_base::time::timer::sleep(Duration::from_millis(i * 500)).await;
            let result = node.get_version_info().await;
            if result.is_ok() {
                info!("Successfully started {nickname}");
                return Ok(());
            } else {
                warn!("Waiting for {nickname} to start");
            }
        }
        bail!("Failed to start {nickname}");
    }

    async fn initialize_storage(&mut self, validator: usize) -> Result<()> {
        let namespace = format!("{}_server_{}_db", self.common_namespace, validator);
        let storage_config = self.common_storage_config.clone();
        let storage = StorageConfigNamespace {
            storage_config,
            namespace,
        };

        let mut command = self.command_for_binary("linera-server").await?;
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        command.arg("initialize");
        command
            .args(["--storage", &storage.to_string()])
            .args(["--genesis", "genesis.json"])
            .spawn_and_wait_for_stdout()
            .await?;

        self.initialized_validator_storages
            .insert(validator, storage);
        Ok(())
    }

    async fn run_server(&mut self, validator: usize, shard: usize) -> Result<Child> {
        let mut storage = self
            .initialized_validator_storages
            .get(&validator)
            .expect("initialized storage")
            .clone();

        // For the storage backends with a local directory, make sure that we don't reuse
        // the same directory for all the shards.
        storage.storage_config.maybe_append_shard_path(shard)?;

        let mut command = self.command_for_binary("linera-server").await?;
        if let Ok(var) = env::var(SERVER_ENV) {
            command.args(var.split_whitespace());
        }
        command
            .arg("run")
            .args(["--storage", &storage.to_string()])
            .args(["--server", &format!("server_{}.json", validator)])
            .args(["--shard", &shard.to_string()])
            .args(["--genesis", "genesis.json"])
            .args(self.cross_chain_config.to_args());
        let child = command.spawn_into()?;

        let port = Self::shard_port(validator, shard);
        let nickname = format!("validator server {validator}:{shard}");
        match self.network.internal {
            Network::Grpc => {
                Self::ensure_grpc_server_has_started(&nickname, port, "http").await?;
            }
            Network::Grpcs => {
                Self::ensure_grpc_server_has_started(&nickname, port, "https").await?;
            }
            Network::Tcp => {
                Self::ensure_simple_server_has_started(&nickname, port, "tcp").await?;
            }
            Network::Udp => {
                Self::ensure_simple_server_has_started(&nickname, port, "udp").await?;
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

    /// Start a validator.
    pub async fn start_validator(&mut self, index: usize) -> Result<()> {
        self.initialize_storage(index).await?;
        self.restart_validator(index).await
    }

    /// Restart a validator. This is similar to `start_validator` except that the
    /// database was already initialized once.
    pub async fn restart_validator(&mut self, index: usize) -> Result<()> {
        let proxy = self.run_proxy(index).await?;
        let mut validator = Validator::new(proxy);
        for shard in 0..self.num_shards {
            let server = self.run_server(index, shard).await?;
            validator.add_server(server);
        }
        for block_exporter in 0..self.num_block_exporters {
            let exporter = self.run_exporter(index, block_exporter).await?;
            validator.add_block_exporter(exporter);
        }
        self.running_validators.insert(index, validator);
        Ok(())
    }

    /// Terminates all the processes of a given validator.
    pub async fn stop_validator(&mut self, index: usize) -> Result<()> {
        if let Some(mut validator) = self.running_validators.remove(&index) {
            if let Err(error) = validator.terminate().await {
                error!("Failed to stop validator {index}: {error}");
                return Err(error);
            }
        }
        Ok(())
    }

    /// Returns a [`linera_rpc::Client`] to interact directly with a `validator`.
    pub async fn validator_client(&mut self, validator: usize) -> Result<linera_rpc::Client> {
        let node_provider = linera_rpc::NodeProvider::new(linera_rpc::NodeOptions {
            send_timeout: Duration::from_secs(1),
            recv_timeout: Duration::from_secs(1),
            retry_delay: Duration::ZERO,
            max_retries: 0,
        });

        Ok(node_provider.make_node(&self.validator_address(validator))?)
    }

    /// Returns the address to connect to a validator's proxy.
    pub fn validator_address(&self, validator: usize) -> String {
        let port = Self::proxy_port(validator);
        let schema = self.network.external.schema();

        format!("{schema}:localhost:{port}")
    }
}

#[cfg(with_testing)]
impl LocalNet {
    /// Returns the validating key and an account key of the validator.
    pub fn validator_keys(&self, validator: usize) -> Option<&(String, String)> {
        self.validator_keys.get(&validator)
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
        let keys = stdout
            .trim()
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        self.validator_keys
            .insert(validator, (keys[0].clone(), keys[1].clone()));
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
            .context("could not find validator")?
            .add_server(server);
        Ok(())
    }
}
