// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(clippy::large_futures)]

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{FutureExt as _, SinkExt, StreamExt};
use linera_core::{node::NodeError, JoinSetExt as _};
use linera_rpc::{
    config::{
        NetworkProtocol, ShardConfig, ValidatorInternalNetworkPreConfig,
        ValidatorPublicNetworkPreConfig,
    },
    simple::{MessageHandler, TransportProtocol},
    RpcMessage,
};
#[cfg(with_metrics)]
use linera_service::prometheus_server;
use linera_service::{
    config::{GenesisConfig, Import, ValidatorServerConfig},
    grpc_proxy::GrpcProxy,
    storage::{run_with_storage, Runnable, StorageConfigNamespace},
    util,
};
use linera_storage::Storage;
use linera_views::{common::CommonStoreConfig, views::ViewError};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

/// Options for running the proxy.
#[derive(clap::Parser, Debug, Clone)]
#[command(
    name = "Linera Proxy",
    about = "A proxy to redirect incoming requests to Linera Server shards",
    version = linera_version::VersionInfo::default_clap_str(),
)]
pub struct ProxyOptions {
    /// Path to server configuration.
    config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[arg(long = "send-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    send_timeout: Duration,

    /// Timeout for receiving responses (us)
    #[arg(long = "recv-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    recv_timeout: Duration,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_PROXY_TOKIO_THREADS")]
    tokio_threads: Option<usize>,

    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfigNamespace,

    /// The maximal number of simultaneous queries to the database
    #[arg(long)]
    max_concurrent_queries: Option<usize>,

    /// The maximal number of stream queries to the database
    #[arg(long, default_value = "10")]
    max_stream_queries: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    cache_size: usize,

    /// Path to the file describing the initial user chains (aka genesis state)
    #[arg(long = "genesis")]
    genesis_config_path: PathBuf,
}

/// A Linera Proxy, either gRPC or over 'Simple Transport', meaning TCP or UDP.
/// The proxy can be configured to have a gRPC ingress and egress, or a combination
/// of TCP / UDP ingress and egress.
enum Proxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Simple(SimpleProxy<S>),
    Grpc(GrpcProxy<S>),
}

struct ProxyContext {
    config: ValidatorServerConfig,
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl ProxyContext {
    pub fn from_options(options: &ProxyOptions) -> Result<Self> {
        let config = ValidatorServerConfig::read(&options.config_path)?;
        Ok(Self {
            config,
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
        })
    }
}

#[async_trait]
impl Runnable for ProxyContext {
    type Output = ();

    async fn run<S>(self, storage: S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(util::listen_for_shutdown_signals(shutdown_notifier.clone()));
        let proxy = Proxy::from_context(self, storage).await?;
        match proxy {
            Proxy::Simple(simple_proxy) => simple_proxy.run(shutdown_notifier).await,
            Proxy::Grpc(grpc_proxy) => grpc_proxy.run(shutdown_notifier).await,
        }
    }
}

impl<S> Proxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Constructs and configures the [`Proxy`] given [`ProxyContext`].
    async fn from_context(context: ProxyContext, storage: S) -> Result<Self> {
        let internal_protocol = context.config.internal_network.protocol;
        let external_protocol = context.config.validator.network.protocol;
        let proxy = match (internal_protocol, external_protocol) {
            (NetworkProtocol::Grpc { .. }, NetworkProtocol::Grpc(tls)) => {
                Self::Grpc(GrpcProxy::new(
                    context.config.validator.network,
                    context.config.internal_network,
                    context.send_timeout,
                    context.recv_timeout,
                    tls,
                    storage,
                ))
            }
            (
                NetworkProtocol::Simple(internal_transport),
                NetworkProtocol::Simple(public_transport),
            ) => Self::Simple(SimpleProxy {
                internal_config: context
                    .config
                    .internal_network
                    .clone_with_protocol(internal_transport),
                public_config: context
                    .config
                    .validator
                    .network
                    .clone_with_protocol(public_transport),
                send_timeout: context.send_timeout,
                recv_timeout: context.recv_timeout,
                storage,
            }),
            _ => {
                bail!(
                    "network protocol mismatch: cannot have {} and {} ",
                    internal_protocol,
                    external_protocol,
                );
            }
        };

        Ok(proxy)
    }
}

#[derive(Debug, Clone)]
pub struct SimpleProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    public_config: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    internal_config: ValidatorInternalNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
    storage: S,
}

#[async_trait]
impl<S> MessageHandler for SimpleProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(skip_all, fields(chain_id = ?message.target_chain_id()))]
    async fn handle_message(&mut self, message: RpcMessage) -> Option<RpcMessage> {
        if message.is_local_message() {
            match self.try_local_message(message).await {
                Ok(maybe_response) => {
                    return maybe_response;
                }
                Err(error) => {
                    error!(error = %error, "Failed to handle local message");
                    return None;
                }
            }
        }

        let Some(chain_id) = message.target_chain_id() else {
            error!("Can't proxy message without chain ID");
            return None;
        };

        let shard = self.internal_config.get_shard_for(chain_id).clone();
        let protocol = self.internal_config.protocol;

        match Self::try_proxy_message(
            message,
            shard,
            protocol,
            self.send_timeout,
            self.recv_timeout,
        )
        .await
        {
            Ok(maybe_response) => maybe_response,
            Err(error) => {
                error!(error = %error, "Failed to proxy message");
                None
            }
        }
    }
}

impl<S> SimpleProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(skip_all, fields(port = self.public_config.port, metrics_port = self.internal_config.metrics_port), err)]
    async fn run(self, shutdown_signal: CancellationToken) -> Result<()> {
        info!("Starting simple server");
        let mut join_set = JoinSet::new();
        let address = self.get_listen_address(self.public_config.port);

        #[cfg(with_metrics)]
        Self::start_metrics(
            self.get_listen_address(self.internal_config.metrics_port),
            shutdown_signal.clone(),
        );

        self.public_config
            .protocol
            .spawn_server(address, self, shutdown_signal, &mut join_set)
            .join()
            .await?;

        join_set.await_all_tasks().await;

        Ok(())
    }

    #[cfg(with_metrics)]
    pub fn start_metrics(address: SocketAddr, shutdown_signal: CancellationToken) {
        prometheus_server::start_metrics(address, shutdown_signal)
    }

    fn get_listen_address(&self, port: u16) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], port))
    }

    async fn try_proxy_message(
        message: RpcMessage,
        shard: ShardConfig,
        protocol: TransportProtocol,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Result<Option<RpcMessage>> {
        let mut connection = protocol.connect((shard.host, shard.port)).await?;
        tokio::time::timeout(send_timeout, connection.send(message)).await??;
        let message = tokio::time::timeout(recv_timeout, connection.next())
            .await?
            .transpose()?;
        Ok(message)
    }

    async fn try_local_message(&self, message: RpcMessage) -> Result<Option<RpcMessage>> {
        use RpcMessage::*;

        match message {
            VersionInfoQuery => {
                // We assume each shard is running the same version as the proxy
                Ok(Some(linera_version::VersionInfo::default().into()))
            }
            DownloadBlob(blob_id) => Ok(Some(
                self.storage
                    .read_hashed_blob(*blob_id)
                    .await?
                    .into_inner()
                    .into(),
            )),
            DownloadCertificateValue(hash) => Ok(Some(
                self.storage
                    .read_hashed_certificate_value(*hash)
                    .await?
                    .into_inner()
                    .into(),
            )),
            DownloadCertificate(hash) => {
                Ok(Some(self.storage.read_certificate(*hash).await?.into()))
            }
            BlockProposal(_)
            | LiteCertificate(_)
            | Certificate(_)
            | ChainInfoQuery(_)
            | CrossChainRequest(_)
            | Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoResponse(_)
            | DownloadBlobResponse(_)
            | DownloadCertificateValueResponse(_)
            | DownloadCertificateResponse(_) => {
                Err(anyhow::Error::from(NodeError::UnexpectedMessage))
            }
        }
    }
}

fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = <ProxyOptions as clap::Parser>::parse();

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(options.run())
}

impl ProxyOptions {
    async fn run(&self) -> Result<()> {
        let common_config = CommonStoreConfig {
            max_concurrent_queries: self.max_concurrent_queries,
            max_stream_queries: self.max_stream_queries,
            cache_size: self.cache_size,
        };
        let full_storage_config = self.storage_config.add_common_config(common_config).await?;
        let genesis_config = GenesisConfig::read(&self.genesis_config_path)
            .expect("Fail to read initial chain config");
        run_with_storage(
            full_storage_config,
            &genesis_config,
            None,
            ProxyContext::from_options(self)?,
        )
        .boxed()
        .await
    }
}
