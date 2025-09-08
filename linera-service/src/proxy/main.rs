// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(clippy::large_futures)]

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, ensure, Result};
use async_trait::async_trait;
use futures::{FutureExt as _, SinkExt, StreamExt};
use linera_base::listen_for_shutdown_signals;
use linera_client::config::ValidatorServerConfig;
use linera_core::{node::NodeError, JoinSetExt as _};
#[cfg(with_metrics)]
use linera_metrics::prometheus_server;
use linera_rpc::{
    config::{
        NetworkProtocol, ShardConfig, ValidatorInternalNetworkPreConfig,
        ValidatorPublicNetworkPreConfig,
    },
    simple::{MessageHandler, TransportProtocol},
    RpcMessage,
};
use linera_sdk::linera_base_types::Blob;
use linera_service::{
    storage::{CommonStorageOptions, Runnable, StorageConfig},
    util,
};
use linera_storage::{ResultReadCertificates, Storage};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

mod grpc;
use grpc::GrpcProxy;

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

    /// Timeout for sending queries (ms)
    #[arg(long = "send-timeout-ms",
          default_value = "4000",
          value_parser = util::parse_millis,
          env = "LINERA_PROXY_SEND_TIMEOUT")]
    send_timeout: Duration,

    /// Timeout for receiving responses (ms)
    #[arg(long = "recv-timeout-ms",
          default_value = "4000",
          value_parser = util::parse_millis,
          env = "LINERA_PROXY_RECV_TIMEOUT")]
    recv_timeout: Duration,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_PROXY_TOKIO_THREADS")]
    tokio_threads: Option<usize>,

    /// The number of Tokio blocking threads to use.
    #[arg(long, env = "LINERA_PROXY_TOKIO_BLOCKING_THREADS")]
    tokio_blocking_threads: Option<usize>,

    /// Storage configuration for the blockchain history, chain states and binary blobs.
    #[arg(long = "storage")]
    storage_config: StorageConfig,

    /// Common storage options.
    #[command(flatten)]
    common_storage_options: CommonStorageOptions,

    /// Runs a specific proxy instance.
    #[arg(long)]
    id: Option<usize>,
}

/// A Linera Proxy, either gRPC or over 'Simple Transport', meaning TCP or UDP.
/// The proxy can be configured to have a gRPC ingress and egress, or a combination
/// of TCP / UDP ingress and egress.
enum Proxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Simple(Box<SimpleProxy<S>>),
    Grpc(GrpcProxy<S>),
}

struct ProxyContext {
    config: ValidatorServerConfig,
    send_timeout: Duration,
    recv_timeout: Duration,
    id: usize,
}

impl ProxyContext {
    pub fn from_options(options: &ProxyOptions) -> Result<Self> {
        let config = util::read_json(&options.config_path)?;
        Ok(Self {
            config,
            send_timeout: options.send_timeout,
            recv_timeout: options.recv_timeout,
            id: options.id.unwrap_or(0),
        })
    }
}

#[async_trait]
impl Runnable for ProxyContext {
    type Output = Result<(), anyhow::Error>;

    async fn run<S>(self, storage: S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));
        let proxy = Proxy::from_context(self, storage)?;
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
    fn from_context(context: ProxyContext, storage: S) -> Result<Self> {
        let internal_protocol = context.config.internal_network.protocol;
        let external_protocol = context.config.validator.network.protocol;
        let proxy = match (internal_protocol, external_protocol) {
            (NetworkProtocol::Grpc { .. }, NetworkProtocol::Grpc(tls)) => {
                Self::Grpc(GrpcProxy::new(
                    context.config.internal_network,
                    context.send_timeout,
                    context.recv_timeout,
                    tls,
                    storage,
                    context.id,
                ))
            }
            (
                NetworkProtocol::Simple(internal_transport),
                NetworkProtocol::Simple(public_transport),
            ) => Self::Simple(Box::new(SimpleProxy {
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
                id: context.id,
            })),
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
    id: usize,
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
            shard.clone(),
            protocol,
            self.send_timeout,
            self.recv_timeout,
        )
        .await
        {
            Ok(maybe_response) => maybe_response,
            Err(error) => {
                error!(error = %error, "Failed to proxy message to {}", shard.address());
                None
            }
        }
    }
}

impl<S> SimpleProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(name = "SimpleProxy::run", skip_all, fields(port = self.public_config.port, metrics_port = self.metrics_port()), err)]
    async fn run(self, shutdown_signal: CancellationToken) -> Result<()> {
        info!("Starting proxy");
        let mut join_set = JoinSet::new();
        let address = self.get_listen_address();

        #[cfg(with_metrics)]
        Self::start_metrics(address, shutdown_signal.clone());

        self.public_config
            .protocol
            .spawn_server(address, self, shutdown_signal, &mut join_set)
            .join()
            .await?;

        join_set.await_all_tasks().await;

        Ok(())
    }

    fn port(&self) -> u16 {
        self.internal_config
            .proxies
            .get(self.id)
            .unwrap_or_else(|| panic!("proxy with id {} must be present", self.id))
            .public_port
    }

    fn metrics_port(&self) -> u16 {
        self.internal_config
            .proxies
            .get(self.id)
            .unwrap_or_else(|| panic!("proxy with id {} must be present", self.id))
            .metrics_port
    }

    #[cfg(with_metrics)]
    pub fn start_metrics(address: SocketAddr, shutdown_signal: CancellationToken) {
        prometheus_server::start_metrics(address, shutdown_signal)
    }

    fn get_listen_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.port()))
    }

    async fn try_proxy_message(
        message: RpcMessage,
        shard: ShardConfig,
        protocol: TransportProtocol,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Result<Option<RpcMessage>> {
        let mut connection = protocol.connect((shard.host, shard.port)).await?;
        linera_base::time::timer::timeout(send_timeout, connection.send(message)).await??;
        let message = linera_base::time::timer::timeout(recv_timeout, connection.next())
            .await?
            .transpose()?;
        Ok(message)
    }

    async fn try_local_message(&self, message: RpcMessage) -> Result<Option<RpcMessage>> {
        use RpcMessage::*;

        match message {
            VersionInfoQuery => {
                // We assume each shard is running the same version as the proxy
                Ok(Some(RpcMessage::VersionInfoResponse(
                    linera_version::VersionInfo::default().into(),
                )))
            }
            NetworkDescriptionQuery => {
                let description = self
                    .storage
                    .read_network_description()
                    .await?
                    .ok_or_else(|| anyhow!("Cannot find network description in the database"))?;
                Ok(Some(RpcMessage::NetworkDescriptionResponse(Box::new(
                    description,
                ))))
            }
            UploadBlob(content) => {
                let blob = Blob::new(*content);
                let id = blob.id();
                ensure!(
                    self.storage.maybe_write_blobs(&[blob]).await?[0],
                    "Blob not found"
                );
                Ok(Some(RpcMessage::UploadBlobResponse(Box::new(id))))
            }
            DownloadBlob(blob_id) => {
                let blob = self.storage.read_blob(*blob_id).await?;
                let blob = blob.ok_or_else(|| anyhow!("Blob not found {}", blob_id))?;
                let content = blob.into_content();
                Ok(Some(RpcMessage::DownloadBlobResponse(Box::new(content))))
            }
            DownloadConfirmedBlock(hash) => {
                let block = self.storage.read_confirmed_block(*hash).await?;
                let block = block.ok_or_else(|| anyhow!("Missing confirmed block {hash}"))?;
                Ok(Some(RpcMessage::DownloadConfirmedBlockResponse(Box::new(
                    block,
                ))))
            }
            DownloadCertificates(hashes) => {
                let certificates = self.storage.read_certificates(hashes.clone()).await?;
                let certificates = match ResultReadCertificates::new(certificates, hashes) {
                    ResultReadCertificates::Certificates(certificates) => certificates,
                    ResultReadCertificates::InvalidHashes(hashes) => {
                        bail!("Missing certificates: {hashes:?}")
                    }
                };
                Ok(Some(RpcMessage::DownloadCertificatesResponse(certificates)))
            }
            DownloadCertificatesByHeights(chain_id, heights) => {
                let shard = self.internal_config.get_shard_for(chain_id).clone();
                let protocol = self.internal_config.protocol;

                let chain_info_query = RpcMessage::ChainInfoQuery(Box::new(
                    linera_core::data_types::ChainInfoQuery::new(chain_id)
                        .with_sent_certificate_hashes_by_heights(heights),
                ));

                let hashes = match Self::try_proxy_message(
                    chain_info_query,
                    shard.clone(),
                    protocol,
                    self.send_timeout,
                    self.recv_timeout,
                )
                .await
                {
                    Ok(Some(RpcMessage::ChainInfoResponse(response))) => {
                        response.info.requested_sent_certificate_hashes
                    }
                    _ => bail!("Failed to retrieve sent certificate hashes"),
                };
                let certificates = self.storage.read_certificates(hashes.clone()).await?;
                let certificates = match ResultReadCertificates::new(certificates, hashes) {
                    ResultReadCertificates::Certificates(certificates) => certificates,
                    ResultReadCertificates::InvalidHashes(hashes) => {
                        bail!("Missing certificates: {hashes:?}")
                    }
                };

                Ok(Some(RpcMessage::DownloadCertificatesByHeightsResponse(
                    certificates,
                )))
            }
            BlobLastUsedBy(blob_id) => {
                let blob_state = self.storage.read_blob_state(*blob_id).await?;
                let blob_state = blob_state.ok_or_else(|| anyhow!("Blob not found {}", blob_id))?;
                let last_used_by = blob_state
                    .last_used_by
                    .ok_or_else(|| anyhow!("Blob not found {}", blob_id))?;
                Ok(Some(RpcMessage::BlobLastUsedByResponse(Box::new(
                    last_used_by,
                ))))
            }
            BlobLastUsedByCertificate(blob_id) => {
                let blob_state = self.storage.read_blob_state(*blob_id).await?;
                let blob_state = blob_state.ok_or_else(|| anyhow!("Blob not found {}", blob_id))?;
                let last_used_by = blob_state
                    .last_used_by
                    .ok_or_else(|| anyhow!("Blob not found {}", blob_id))?;
                let certificate = self
                    .storage
                    .read_certificate(last_used_by)
                    .await?
                    .ok_or_else(|| anyhow!("Certificate not found {}", last_used_by))?;
                Ok(Some(RpcMessage::BlobLastUsedByCertificateResponse(
                    Box::new(certificate),
                )))
            }
            MissingBlobIds(blob_ids) => Ok(Some(RpcMessage::MissingBlobIdsResponse(
                self.storage.missing_blobs(&blob_ids).await?,
            ))),
            BlockProposal(_)
            | LiteCertificate(_)
            | TimeoutCertificate(_)
            | ConfirmedCertificate(_)
            | ValidatedCertificate(_)
            | ChainInfoQuery(_)
            | CrossChainRequest(_)
            | Vote(_)
            | Error(_)
            | ChainInfoResponse(_)
            | VersionInfoResponse(_)
            | NetworkDescriptionResponse(_)
            | DownloadBlobResponse(_)
            | DownloadPendingBlob(_)
            | DownloadPendingBlobResponse(_)
            | HandlePendingBlob(_)
            | BlobLastUsedByResponse(_)
            | BlobLastUsedByCertificateResponse(_)
            | MissingBlobIdsResponse(_)
            | DownloadConfirmedBlockResponse(_)
            | DownloadCertificatesResponse(_)
            | UploadBlobResponse(_)
            | DownloadCertificatesByHeightsResponse(_) => {
                Err(anyhow::Error::from(NodeError::UnexpectedMessage))
            }
        }
    }
}

fn main() -> Result<()> {
    let options = <ProxyOptions as clap::Parser>::parse();
    let server_config: ValidatorServerConfig =
        util::read_json(&options.config_path).expect("Fail to read server config");
    let public_key = &server_config.validator.public_key;

    linera_base::tracing::init(&format!("validator-{public_key}-proxy"));

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    if let Some(blocking_threads) = options.tokio_blocking_threads {
        runtime.max_blocking_threads(blocking_threads);
    }

    runtime.enable_all().build()?.block_on(options.run())
}

impl ProxyOptions {
    async fn run(&self) -> Result<()> {
        let store_config = self
            .storage_config
            .add_common_storage_options(&self.common_storage_options)
            .await?;
        store_config
            .run_with_storage(None, ProxyContext::from_options(self)?)
            .boxed()
            .await?
    }
}
