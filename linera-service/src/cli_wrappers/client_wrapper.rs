// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    cli_wrappers::{Network, NodeService},
    config::WalletState,
    util,
    util::CommandExt,
};
use anyhow::{bail, Context, Result};
use linera_base::{
    abi::ContractAbi,
    crypto::PublicKey,
    data_types::{Amount, RoundNumber},
    identifiers::{ApplicationId, BytecodeId, ChainId, MessageId, Owner},
};
use serde::ser::Serialize;
use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::process::Command;
use tracing::{info, warn};

/// The name of the environment variable that allows specifying additional arguments to be passed
/// to the node-service command of the client.
const CLIENT_SERVICE_ENV: &str = "LINERA_CLIENT_SERVICE_PARAMS";

pub struct ClientWrapper {
    testing_prng_seed: Option<u64>,
    storage: String,
    wallet: String,
    max_pending_messages: usize,
    network: Network,
    pub tmp_dir: Arc<TempDir>,
}

impl ClientWrapper {
    pub(crate) fn new(
        tmp_dir: Arc<TempDir>,
        network: Network,
        testing_prng_seed: Option<u64>,
        id: usize,
    ) -> Self {
        let storage = format!("rocksdb:{}/client_{}.db", tmp_dir.path().display(), id);
        let wallet = format!("wallet_{}.json", id);
        Self {
            testing_prng_seed,
            storage,
            wallet,
            max_pending_messages: 10_000,
            network,
            tmp_dir,
        }
    }

    pub async fn project_new(&self, project_name: &str, linera_root: &Path) -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let mut command = self.command().await?;
        command
            .current_dir(tmp.path())
            .arg("project")
            .arg("new")
            .arg(project_name)
            .arg("--linera-root")
            .arg(linera_root)
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(tmp)
    }

    pub async fn project_publish<T: Serialize>(
        &self,
        path: PathBuf,
        required_application_ids: Vec<String>,
        publisher: impl Into<Option<ChainId>>,
        argument: &T,
    ) -> Result<String> {
        let json_parameters = serde_json::to_string(&())?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.command().await?;
        command
            .arg("project")
            .arg("publish-and-create")
            .arg(path)
            .args(publisher.into().iter().map(ChainId::to_string))
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument]);
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(required_application_ids);
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().to_string())
    }

    pub async fn project_test(&self, path: &Path) -> Result<()> {
        self.command()
            .await
            .context("failed to create project test command")?
            .current_dir(path)
            .arg("project")
            .arg("test")
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    async fn command(&self) -> Result<Command> {
        let path = util::resolve_binary("linera", env!("CARGO_PKG_NAME")).await?;
        let mut command = Command::new(path);
        command
            .current_dir(self.tmp_dir.path())
            .args(["--wallet", &self.wallet])
            .args(["--storage", &self.storage])
            .args([
                "--max-pending-messages",
                &self.max_pending_messages.to_string(),
            ])
            .args(["--send-timeout-us", "10000000"])
            .args(["--recv-timeout-us", "10000000"])
            .arg("--wait-for-outgoing-messages");
        Ok(command)
    }

    pub async fn create_genesis_config(&self) -> Result<()> {
        let mut command = self.command().await?;
        command
            .args(["create-genesis-config", "10"])
            .args(["--initial-funding", "10"])
            .args(["--committee", "committee.json"])
            .args(["--genesis", "genesis.json"]);
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    pub async fn wallet_init(&self, chain_ids: &[ChainId]) -> Result<()> {
        let mut command = self.command().await?;
        command
            .args(["wallet", "init"])
            .args(["--genesis", "genesis.json"]);
        if let Some(seed) = self.testing_prng_seed {
            command.arg("--testing-prng-seed").arg(seed.to_string());
        }
        if !chain_ids.is_empty() {
            let ids = chain_ids.iter().map(ChainId::to_string);
            command.arg("--with-other-chains").args(ids);
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    pub async fn publish_and_create<A: ContractAbi>(
        &self,
        contract: PathBuf,
        service: PathBuf,
        parameters: &A::Parameters,
        argument: &A::InitializationArgument,
        required_application_ids: &[ApplicationId],
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_parameters = serde_json::to_string(parameters)?;
        let json_argument = serde_json::to_string(argument)?;
        let mut command = self.command().await?;
        command
            .arg("publish-and-create")
            .args([contract, service])
            .args(publisher.into().iter().map(ChainId::to_string))
            .args(["--json-parameters", &json_parameters])
            .args(["--json-argument", &json_argument]);
        if !required_application_ids.is_empty() {
            command.arg("--required-application-ids");
            command.args(
                required_application_ids
                    .iter()
                    .map(ApplicationId::to_string),
            );
        }
        let stdout = command.spawn_and_wait_for_stdout().await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    pub async fn publish_bytecode(
        &self,
        contract: PathBuf,
        service: PathBuf,
        publisher: impl Into<Option<ChainId>>,
    ) -> Result<BytecodeId> {
        let stdout = self
            .command()
            .await?
            .arg("publish-bytecode")
            .args([contract, service])
            .args(publisher.into().iter().map(ChainId::to_string))
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(stdout.trim().parse()?)
    }

    pub async fn create_application<A: ContractAbi>(
        &self,
        bytecode_id: &BytecodeId,
        argument: &A::InitializationArgument,
        creator: impl Into<Option<ChainId>>,
    ) -> Result<ApplicationId<A>> {
        let json_argument = serde_json::to_string(argument)?;
        let stdout = self
            .command()
            .await?
            .arg("create-application")
            .arg(bytecode_id.to_string())
            .args(["--json-argument", &json_argument])
            .args(creator.into().iter().map(ChainId::to_string))
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(stdout.trim().parse::<ApplicationId>()?.with_abi())
    }

    pub async fn run_node_service(&self, port: impl Into<Option<u16>>) -> Result<NodeService> {
        let port = port.into().unwrap_or(8080);
        let mut command = self.command().await?;
        command.arg("service");
        if let Ok(var) = env::var(CLIENT_SERVICE_ENV) {
            command.args(var.split_whitespace());
        }
        let child = command
            .args(["--port".to_string(), port.to_string()])
            .spawn_into()?;
        let client = reqwest::Client::new();
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs(i)).await;
            let request = client
                .get(format!("http://localhost:{}/", port))
                .send()
                .await;
            if request.is_ok() {
                info!("Node service has started");
                return Ok(NodeService::new(port, child));
            } else {
                warn!("Waiting for node service to start");
            }
        }
        bail!("Failed to start node service");
    }

    pub async fn query_validators(&self, chain_id: Option<ChainId>) -> Result<()> {
        let mut command = self.command().await?;
        command.arg("query-validators");
        if let Some(chain_id) = chain_id {
            command.arg(&chain_id.to_string());
        }
        command.spawn_and_wait_for_stdout().await?;
        Ok(())
    }

    pub async fn query_balance(&self, chain_id: ChainId) -> Result<Amount> {
        let stdout = self
            .command()
            .await?
            .arg("query-balance")
            .arg(&chain_id.to_string())
            .spawn_and_wait_for_stdout()
            .await?;
        let amount = stdout
            .trim()
            .parse()
            .context("error while parsing the result of `linera query-balance`")?;
        Ok(amount)
    }

    pub async fn transfer(&self, amount: Amount, from: ChainId, to: ChainId) -> Result<()> {
        self.command()
            .await?
            .arg("transfer")
            .arg(amount.to_string())
            .args(["--from", &from.to_string()])
            .args(["--to", &to.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    #[cfg(benchmark)]
    async fn benchmark(&self, max_in_flight: usize) -> Result<()> {
        self.command()
            .await
            .arg("benchmark")
            .args(["--max-in-flight", &max_in_flight.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn open_chain(
        &self,
        from: ChainId,
        to_public_key: Option<PublicKey>,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.command().await?;
        command
            .arg("open-chain")
            .args(["--from", &from.to_string()]);

        if let Some(public_key) = to_public_key {
            command.args(["--to-public-key", &public_key.to_string()]);
        }

        let stdout = command.spawn_and_wait_for_stdout().await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("no message ID in output")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("no chain ID in output")?)?;

        Ok((message_id, chain_id))
    }

    pub async fn open_and_assign(&self, client: &ClientWrapper) -> Result<ChainId> {
        let our_chain = self
            .get_wallet()?
            .default_chain()
            .context("no default chain found")?;
        let key = client.keygen().await?;
        let (message_id, new_chain) = self.open_chain(our_chain, Some(key)).await?;
        assert_eq!(new_chain, client.assign(key, message_id).await?);
        Ok(new_chain)
    }

    pub async fn open_multi_owner_chain(
        &self,
        from: ChainId,
        to_public_keys: Vec<PublicKey>,
        weights: Vec<u64>,
        multi_leader_rounds: RoundNumber,
    ) -> Result<(MessageId, ChainId)> {
        let mut command = self.command().await?;
        command
            .arg("open-multi-owner-chain")
            .args(["--from", &from.to_string()])
            .arg("--to-public-keys")
            .args(to_public_keys.iter().map(PublicKey::to_string))
            .arg("--weights")
            .args(weights.iter().map(u64::to_string))
            .args(["--multi-leader-rounds", &multi_leader_rounds.to_string()]);

        let stdout = command.spawn_and_wait_for_stdout().await?;
        let mut split = stdout.split('\n');
        let message_id: MessageId = split.next().context("no message ID in output")?.parse()?;
        let chain_id = ChainId::from_str(split.next().context("no chain ID in output")?)?;

        Ok((message_id, chain_id))
    }

    pub fn get_wallet(&self) -> Result<WalletState> {
        WalletState::from_file(self.wallet_path().as_path())
    }

    pub fn wallet_path(&self) -> PathBuf {
        self.tmp_dir.path().join(&self.wallet)
    }

    pub fn storage_path(&self) -> &str {
        &self.storage
    }

    pub fn get_owner(&self) -> Option<Owner> {
        let wallet = self.get_wallet().ok()?;
        let chain_id = wallet.default_chain()?;
        let public_key = wallet.get(chain_id)?.key_pair.as_ref()?.public();
        Some(public_key.into())
    }

    pub async fn is_chain_present_in_wallet(&self, chain: ChainId) -> bool {
        self.get_wallet()
            .ok()
            .map_or(false, |wallet| wallet.get(chain).is_some())
    }

    pub async fn set_validator(&self, name: &str, port: usize, votes: usize) -> Result<()> {
        let address = format!("{}:127.0.0.1:{}", self.network.external_short(), port);
        self.command()
            .await?
            .arg("set-validator")
            .args(["--name", name])
            .args(["--address", &address])
            .args(["--votes", &votes.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn remove_validator(&self, name: &str) -> Result<()> {
        self.command()
            .await?
            .arg("remove-validator")
            .args(["--name", name])
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(())
    }

    pub async fn keygen(&self) -> Result<PublicKey> {
        let stdout = self
            .command()
            .await?
            .arg("keygen")
            .spawn_and_wait_for_stdout()
            .await?;
        Ok(PublicKey::from_str(stdout.trim())?)
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.get_wallet().ok()?.default_chain()
    }

    pub async fn assign(&self, key: PublicKey, message_id: MessageId) -> Result<ChainId> {
        let stdout = self
            .command()
            .await?
            .arg("assign")
            .args(["--key", &key.to_string()])
            .args(["--message-id", &message_id.to_string()])
            .spawn_and_wait_for_stdout()
            .await?;

        let chain_id = ChainId::from_str(stdout.trim())?;

        Ok(chain_id)
    }

    pub async fn synchronize_balance(&self, chain_id: ChainId) -> Result<Amount> {
        let stdout = self
            .command()
            .await?
            .arg("sync-balance")
            .arg(&chain_id.to_string())
            .spawn_and_wait_for_stdout()
            .await?;
        let amount = stdout
            .trim()
            .parse()
            .context("error while parsing the result of `linera sync-balance`")?;
        Ok(amount)
    }
}
