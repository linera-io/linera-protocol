// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]

use std::{
    fmt::Debug,
    marker::PhantomData,
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt as _};
use linera_base::identifiers::ChainId;
use linera_core::{
    data_types::CertificatesByHeightRequest, notifier::ChannelNotifier, JoinSetExt as _,
};
#[cfg(with_metrics)]
use linera_metrics::monitoring_server;
use linera_rpc::{
    config::{ProxyConfig, ShardConfig, TlsConfig, ValidatorInternalNetworkConfig},
    grpc::{
        api::{
            self,
            notifier_service_server::{NotifierService, NotifierServiceServer},
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            BlobContent, BlobId, BlobIds, BlockProposal, Certificate, CertificatesBatchRequest,
            CertificatesBatchResponse, ChainInfoResult, CryptoHash, HandlePendingBlobRequest,
            LiteCertificate, NetworkDescription, Notification, NotificationBatch,
            PendingBlobRequest, PendingBlobResult, RawCertificate, RawCertificatesBatch,
            SubscriptionRequest, VersionInfo,
        },
        pool::GrpcConnectionPool,
        GrpcProtoConversionError, GrpcProxyable, GRPC_CHUNKED_MESSAGE_FILL_LIMIT,
        GRPC_MAX_MESSAGE_SIZE,
    },
};
use linera_sdk::{linera_base_types::Blob, views::ViewError};
use linera_storage::{ResultReadCertificates, Storage};
use prost::Message;
use tokio::{select, task::JoinSet};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    transport::{Channel, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tonic_web::GrpcWebLayer;
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, info, instrument, Instrument as _, Level};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        linear_bucket_interval, register_histogram_vec, register_int_counter_vec,
    };
    use prometheus::{HistogramVec, IntCounterVec};

    pub static PROXY_REQUEST_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "proxy_request_latency",
            "Proxy request latency",
            &[],
            linear_bucket_interval(1.0, 50.0, 2000.0),
        )
    });
    pub static PROXY_REQUEST_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec("proxy_request_count", "Proxy request count", &[])
    });

    pub static PROXY_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "proxy_request_success",
            "Proxy request success",
            &["method_name"],
        )
    });

    pub static PROXY_REQUEST_ERROR: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec(
            "proxy_request_error",
            "Proxy request error",
            &["method_name"],
        )
    });
}

#[derive(Clone)]
pub struct PrometheusMetricsMiddlewareLayer;

#[derive(Clone)]
pub struct PrometheusMetricsMiddlewareService<T> {
    service: T,
}

impl<S> Layer<S> for PrometheusMetricsMiddlewareLayer {
    type Service = PrometheusMetricsMiddlewareService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusMetricsMiddlewareService { service }
    }
}

impl<S, Req> Service<Req> for PrometheusMetricsMiddlewareService<S>
where
    S::Future: Send + 'static,
    S: Service<Req> + std::marker::Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        #[cfg(with_metrics)]
        let start = linera_base::time::Instant::now();
        let future = self.service.call(request);
        async move {
            let response = future.await?;
            #[cfg(with_metrics)]
            {
                metrics::PROXY_REQUEST_LATENCY
                    .with_label_values(&[])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                metrics::PROXY_REQUEST_COUNT.with_label_values(&[]).inc();
            }
            Ok(response)
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct GrpcProxy<S>(Arc<GrpcProxyInner<S>>);

struct GrpcProxyInner<S> {
    internal_config: ValidatorInternalNetworkConfig,
    worker_connection_pool: GrpcConnectionPool,
    notifier: ChannelNotifier<Result<Notification, Status>>,
    tls: TlsConfig,
    storage: S,
    id: usize,
}

impl<S> GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub fn new(
        internal_config: ValidatorInternalNetworkConfig,
        connect_timeout: Duration,
        timeout: Duration,
        tls: TlsConfig,
        storage: S,
        id: usize,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            internal_config,
            worker_connection_pool: GrpcConnectionPool::default()
                .with_connect_timeout(connect_timeout)
                .with_timeout(timeout),
            notifier: ChannelNotifier::default(),
            tls,
            storage,
            id,
        }))
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }

    fn config(&self) -> &ProxyConfig {
        self.0
            .internal_config
            .proxies
            .get(self.0.id)
            .expect("No proxy config provided.")
    }

    fn as_notifier_service(&self) -> NotifierServiceServer<Self> {
        NotifierServiceServer::new(self.clone())
    }

    fn public_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.config().public_port))
    }

    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.config().metrics_port))
    }

    fn internal_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.config().private_port))
    }

    fn shard_for(&self, proxyable: &impl GrpcProxyable) -> Option<ShardConfig> {
        Some(
            self.0
                .internal_config
                .get_shard_for(proxyable.chain_id()?)
                .clone(),
        )
    }

    fn worker_client_for_shard(
        &self,
        shard: &ShardConfig,
    ) -> Result<ValidatorWorkerClient<Channel>> {
        let address = shard.http_address();
        let channel = self.0.worker_connection_pool.channel(address)?;
        let client = ValidatorWorkerClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        Ok(client)
    }

    /// Runs the proxy. If either the public server or private server dies for whatever
    /// reason we'll kill the proxy.
    #[instrument(
        name = "GrpcProxy::run",
        skip_all,
        fields(
            public_address = %self.public_address(),
            internal_address = %self.internal_address(),
            metrics_address = %self.metrics_address(),
        ),
        err,
    )]
    pub async fn run(self, shutdown_signal: CancellationToken) -> Result<()> {
        info!("Starting proxy");
        let mut join_set = JoinSet::new();

        #[cfg(with_metrics)]
        monitoring_server::start_metrics(self.metrics_address(), shutdown_signal.clone());

        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ValidatorNodeServer<GrpcProxy<S>>>()
            .await;
        let internal_server = join_set.spawn_task(
            Server::builder()
                .add_service(self.as_notifier_service())
                .serve(self.internal_address())
                .in_current_span(),
        );
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(linera_rpc::FILE_DESCRIPTOR_SET)
            .build_v1()?;
        let public_server = join_set.spawn_task(
            self.public_server()?
                .max_concurrent_streams(
                    // we subtract one to make sure
                    // that the value is not
                    // interpreted as "not set"
                    Some(u32::MAX - 1),
                )
                .layer(
                    ServiceBuilder::new()
                        .layer(PrometheusMetricsMiddlewareLayer)
                        .into_inner(),
                )
                .layer(
                    // enable
                    // [CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS)
                    // for the proxy to originate anywhere
                    tower_http::cors::CorsLayer::permissive(),
                )
                .layer(GrpcWebLayer::new())
                .accept_http1(true)
                .add_service(health_service)
                .add_service(self.as_validator_node())
                .add_service(reflection_service)
                .serve_with_shutdown(self.public_address(), shutdown_signal.cancelled_owned())
                .in_current_span(),
        );

        select! {
            internal_res = internal_server => internal_res??,
            public_res = public_server => public_res??,
        }
        Ok(())
    }

    /// Pre-configures the public server with no services attached.
    /// If a certificate and key are defined, creates a TLS server.
    fn public_server(&self) -> Result<Server> {
        match self.0.tls {
            TlsConfig::Tls => {
                use linera_rpc::{CERT_PEM, KEY_PEM};
                let identity = Identity::from_pem(CERT_PEM, KEY_PEM);
                let tls_config = ServerTlsConfig::new().identity(identity);
                Ok(Server::builder().tls_config(tls_config)?)
            }
            TlsConfig::ClearText => Ok(Server::builder()),
        }
    }

    #[instrument(skip_all, fields(remote_addr = ?request.remote_addr(), chain_id = ?request.get_ref().chain_id()))]
    fn worker_client<R>(
        &self,
        request: Request<R>,
    ) -> Result<(ValidatorWorkerClient<Channel>, R), Status>
    where
        R: Debug + GrpcProxyable,
    {
        debug!("proxying request from {:?}", request.remote_addr());
        let inner = request.into_inner();
        let shard = self
            .shard_for(&inner)
            .ok_or_else(|| Status::not_found("could not find shard for message"))?;
        let client = self
            .worker_client_for_shard(&shard)
            .map_err(|_| Status::internal("could not connect to shard"))?;
        Ok((client, inner))
    }

    #[allow(clippy::result_large_err)]
    fn log_and_return_proxy_request_outcome(
        result: Result<Response<ChainInfoResult>, Status>,
        method_name: &str,
    ) -> Result<Response<ChainInfoResult>, Status> {
        #![allow(unused_variables)]
        match result {
            Ok(chain_info_result) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_SUCCESS
                    .with_label_values(&[method_name])
                    .inc();
                Ok(chain_info_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_ERROR
                    .with_label_values(&[method_name])
                    .inc();
                Err(status)
            }
        }
    }

    /// Returns the appropriate gRPC status for the given [`ViewError`].
    fn view_error_to_status(err: ViewError) -> Status {
        let mut status = match &err {
            ViewError::BcsError(_) => Status::invalid_argument(err.to_string()),
            ViewError::StoreError { .. }
            | ViewError::TokioJoinError(_)
            | ViewError::TryLockError(_)
            | ViewError::InconsistentEntries
            | ViewError::PostLoadValuesError
            | ViewError::IoError(_) => Status::internal(err.to_string()),
            ViewError::KeyTooLong | ViewError::ArithmeticError(_) => {
                Status::out_of_range(err.to_string())
            }
            ViewError::NotFound(_) | ViewError::MissingEntries(_) => {
                Status::not_found(err.to_string())
            }
        };
        status.set_source(Arc::new(err));
        status
    }
}

#[async_trait]
impl<S> ValidatorNode for GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    type SubscribeStream = UnboundedReceiverStream<Result<Notification, Status>>;

    #[instrument(skip_all, err(Display), fields(method = "handle_block_proposal"))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_block_proposal(inner).await,
            "handle_block_proposal",
        )
    }

    #[instrument(skip_all, err(Display), fields(method = "handle_lite_certificate"))]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_lite_certificate(inner).await,
            "handle_lite_certificate",
        )
    }

    #[instrument(
        skip_all,
        err(Display),
        fields(method = "handle_confirmed_certificate")
    )]
    async fn handle_confirmed_certificate(
        &self,
        request: Request<api::HandleConfirmedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_confirmed_certificate(inner).await,
            "handle_confirmed_certificate",
        )
    }

    #[instrument(
        skip_all,
        err(Display),
        fields(method = "handle_validated_certificate")
    )]
    async fn handle_validated_certificate(
        &self,
        request: Request<api::HandleValidatedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_validated_certificate(inner).await,
            "handle_validated_certificate",
        )
    }

    #[instrument(skip_all, err(Display), fields(method = "handle_timeout_certificate"))]
    async fn handle_timeout_certificate(
        &self,
        request: Request<api::HandleTimeoutCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_timeout_certificate(inner).await,
            "handle_timeout_certificate",
        )
    }

    #[instrument(skip_all, err(Display), fields(method = "handle_chain_info_query"))]
    async fn handle_chain_info_query(
        &self,
        request: Request<api::ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_chain_info_query(inner).await,
            "handle_chain_info_query",
        )
    }

    #[instrument(skip_all, err(Display), fields(method = "subscribe"))]
    async fn subscribe(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscription_request = request.into_inner();
        let chain_ids = subscription_request
            .chain_ids
            .into_iter()
            .map(ChainId::try_from)
            .collect::<Result<Vec<ChainId>, _>>()?;
        // The empty notification seems to be needed in some cases to force
        // completion of HTTP2 headers.
        let rx = self
            .0
            .notifier
            .subscribe_with_ack(chain_ids, Ok(Notification::default()));
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    #[instrument(skip_all, err(Display))]
    async fn get_version_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<VersionInfo>, Status> {
        // We assume each shard is running the same version as the proxy
        Ok(Response::new(linera_version::VersionInfo::default().into()))
    }

    #[instrument(skip_all, err(Display), fields(method = "get_network_description"))]
    async fn get_network_description(
        &self,
        _request: Request<()>,
    ) -> Result<Response<NetworkDescription>, Status> {
        let description = self
            .0
            .storage
            .read_network_description()
            .await
            .map_err(Self::view_error_to_status)?
            .ok_or_else(|| Status::not_found("Cannot find network description in the database"))?;
        Ok(Response::new(description.into()))
    }

    #[instrument(skip_all, err(Display), fields(method = "get_shard_info"))]
    async fn get_shard_info(
        &self,
        request: Request<api::ChainId>,
    ) -> Result<Response<api::ShardInfo>, Status> {
        let chain_id = request.into_inner().try_into()?;
        let shard_id = self.0.internal_config.get_shard_id(chain_id);
        let total_shards = self.0.internal_config.shards.len();

        let shard_info = api::ShardInfo {
            shard_id: shard_id as u64,
            total_shards: total_shards as u64,
        };

        Ok(Response::new(shard_info))
    }

    #[instrument(skip_all, err(Display), fields(method = "upload_blob"))]
    async fn upload_blob(&self, request: Request<BlobContent>) -> Result<Response<BlobId>, Status> {
        let content: linera_sdk::linera_base_types::BlobContent =
            request.into_inner().try_into()?;
        let blob = Blob::new(content);
        let id = blob.id();
        let result = self.0.storage.maybe_write_blobs(&[blob]).await;
        if !result.map_err(Self::view_error_to_status)?[0] {
            return Err(Status::not_found("Blob not found"));
        }
        Ok(Response::new(id.try_into()?))
    }

    #[instrument(skip_all, err(Display), fields(method = "download_blob"))]
    async fn download_blob(
        &self,
        request: Request<BlobId>,
    ) -> Result<Response<BlobContent>, Status> {
        let blob_id = request.into_inner().try_into()?;
        let blob = self
            .0
            .storage
            .read_blob(blob_id)
            .await
            .map_err(Self::view_error_to_status)?;
        let blob = blob.ok_or_else(|| Status::not_found(format!("Blob not found {}", blob_id)))?;
        Ok(Response::new(blob.into_content().try_into()?))
    }

    #[instrument(skip_all, err(Display), fields(method = "download_pending_blob"))]
    async fn download_pending_blob(
        &self,
        request: Request<PendingBlobRequest>,
    ) -> Result<Response<PendingBlobResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        #[cfg_attr(not(with_metrics), expect(clippy::needless_match))]
        match client.download_pending_blob(inner).await {
            Ok(blob_result) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_SUCCESS
                    .with_label_values(&["download_pending_blob"])
                    .inc();
                Ok(blob_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_ERROR
                    .with_label_values(&["download_pending_blob"])
                    .inc();
                Err(status)
            }
        }
    }

    #[instrument(skip_all, err(Display), fields(method = "handle_pending_blob"))]
    async fn handle_pending_blob(
        &self,
        request: Request<HandlePendingBlobRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request)?;
        #[cfg_attr(not(with_metrics), expect(clippy::needless_match))]
        match client.handle_pending_blob(inner).await {
            Ok(blob_result) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_SUCCESS
                    .with_label_values(&["handle_pending_blob"])
                    .inc();
                Ok(blob_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                metrics::PROXY_REQUEST_ERROR
                    .with_label_values(&["handle_pending_blob"])
                    .inc();
                Err(status)
            }
        }
    }

    #[instrument(skip_all, err(Display), fields(method = "download_certificate"))]
    async fn download_certificate(
        &self,
        request: Request<CryptoHash>,
    ) -> Result<Response<Certificate>, Status> {
        let hash = request.into_inner().try_into()?;
        let certificate: linera_chain::types::Certificate = self
            .0
            .storage
            .read_certificate(hash)
            .await
            .map_err(Self::view_error_to_status)?
            .ok_or(Status::not_found(hash.to_string()))?
            .into();
        Ok(Response::new(certificate.try_into()?))
    }

    #[instrument(skip_all, err(Display), fields(method = "download_certificates"))]
    async fn download_certificates(
        &self,
        request: Request<CertificatesBatchRequest>,
    ) -> Result<Response<CertificatesBatchResponse>, Status> {
        let hashes: Vec<linera_base::crypto::CryptoHash> = request
            .into_inner()
            .hashes
            .into_iter()
            .map(linera_base::crypto::CryptoHash::try_from)
            .collect::<Result<Vec<linera_base::crypto::CryptoHash>, _>>()?;

        let mut grpc_message_limiter: GrpcMessageLimiter<linera_chain::types::Certificate> =
            GrpcMessageLimiter::new(GRPC_CHUNKED_MESSAGE_FILL_LIMIT);

        let mut returned_certificates = vec![];

        'outer: for batch in hashes.chunks(100) {
            let certificates = self
                .0
                .storage
                .read_certificates(batch)
                .await
                .map_err(Self::view_error_to_status)?;
            let certificates = match ResultReadCertificates::new(certificates, batch.to_vec()) {
                ResultReadCertificates::Certificates(certificates) => certificates,
                ResultReadCertificates::InvalidHashes(hashes) => {
                    return Err(Status::not_found(format!("{:?}", hashes)))
                }
            };
            for certificate in certificates {
                if grpc_message_limiter.fits::<Certificate>(certificate.clone().into())? {
                    returned_certificates.push(linera_chain::types::Certificate::from(certificate));
                } else {
                    break 'outer;
                }
            }
        }

        Ok(Response::new(CertificatesBatchResponse::try_from(
            returned_certificates,
        )?))
    }

    #[instrument(
        skip_all,
        err(Display),
        fields(method = "download_certificates_by_heights")
    )]
    async fn download_certificates_by_heights(
        &self,
        request: Request<api::DownloadCertificatesByHeightsRequest>,
    ) -> Result<Response<CertificatesBatchResponse>, Status> {
        let original_request: CertificatesByHeightRequest = request.into_inner().try_into()?;
        let chain_id = original_request.chain_id;
        let heights = original_request.heights;

        let certificates_by_height: Vec<_> = self
            .0
            .storage
            .read_certificates_by_heights(chain_id, &heights)
            .await
            .map_err(Self::view_error_to_status)?
            .into_iter()
            .flatten()
            .collect();

        let mut limiter: GrpcMessageLimiter<linera_chain::types::Certificate> =
            GrpcMessageLimiter::new(GRPC_CHUNKED_MESSAGE_FILL_LIMIT);

        let returned_certificates =
            limiter.take_if(certificates_by_height, |lim, certificate| {
                let cert: linera_chain::types::Certificate = certificate.into();
                Ok(lim.fits::<Certificate>(cert.clone())?.then_some(cert))
            })?;

        Ok(Response::new(CertificatesBatchResponse::try_from(
            returned_certificates,
        )?))
    }

    #[instrument(skip_all, err(Display))]
    async fn download_raw_certificates_by_heights(
        &self,
        request: Request<api::DownloadCertificatesByHeightsRequest>,
    ) -> Result<Response<api::RawCertificatesBatch>, Status> {
        let original_request: CertificatesByHeightRequest = request.into_inner().try_into()?;
        let chain_id = original_request.chain_id;
        let heights = original_request.heights;

        let raw_certificates_by_height = self
            .0
            .storage
            .read_certificates_by_heights_raw(chain_id, &heights)
            .await
            .map_err(Self::view_error_to_status)?
            .into_iter()
            .flatten()
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

        let mut limiter: GrpcMessageLimiter<linera_chain::types::Certificate> =
            GrpcMessageLimiter::new(GRPC_CHUNKED_MESSAGE_FILL_LIMIT);

        let certificates = limiter.take_if(raw_certificates_by_height, |lim, (lite, block)| {
            Ok(lim
                .fits_raw(lite.len() + block.len())
                .then_some(RawCertificate {
                    lite_certificate: lite,
                    confirmed_block: block,
                }))
        })?;

        Ok(Response::new(RawCertificatesBatch { certificates }))
    }

    #[instrument(skip_all, err(level = Level::WARN), fields(
        method = "blob_last_used_by"
    ))]
    async fn blob_last_used_by(
        &self,
        request: Request<BlobId>,
    ) -> Result<Response<CryptoHash>, Status> {
        let blob_id = request.into_inner().try_into()?;
        let blob_state = self
            .0
            .storage
            .read_blob_state(blob_id)
            .await
            .map_err(Self::view_error_to_status)?;
        let blob_state =
            blob_state.ok_or_else(|| Status::not_found(format!("Blob not found {}", blob_id)))?;
        let last_used_by = blob_state
            .last_used_by
            .ok_or_else(|| Status::not_found(format!("Blob not found {}", blob_id)))?;
        Ok(Response::new(last_used_by.into()))
    }

    #[instrument(skip_all, err(level = Level::WARN), fields(
        method = "blob_last_used_by_certificate"
    ))]
    async fn blob_last_used_by_certificate(
        &self,
        request: Request<BlobId>,
    ) -> Result<Response<Certificate>, Status> {
        let cert_hash = self.blob_last_used_by(request).await?;
        let request = Request::new(cert_hash.into_inner());
        self.download_certificate(request).await
    }

    #[instrument(skip_all, err(level = Level::WARN), fields(
        method = "missing_blob_ids"
    ))]
    async fn missing_blob_ids(
        &self,
        request: Request<BlobIds>,
    ) -> Result<Response<BlobIds>, Status> {
        let blob_ids: Vec<linera_base::identifiers::BlobId> = request.into_inner().try_into()?;
        let missing_blob_ids = self
            .0
            .storage
            .missing_blobs(&blob_ids)
            .await
            .map_err(Self::view_error_to_status)?;
        Ok(Response::new(missing_blob_ids.try_into()?))
    }
}

#[async_trait]
impl<S> NotifierService for GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(skip_all, err(Display), fields(method = "notify_batch"))]
    async fn notify_batch(
        &self,
        request: Request<NotificationBatch>,
    ) -> Result<Response<()>, Status> {
        for notification in request.into_inner().notifications {
            let chain_id = notification
                .chain_id
                .clone()
                .ok_or_else(|| Status::invalid_argument("Missing field: chain_id."))?
                .try_into()?;
            self.0.notifier.notify_chain(&chain_id, &Ok(notification));
        }
        Ok(Response::new(()))
    }
}

/// A message limiter that keeps track of the remaining capacity in bytes.
struct GrpcMessageLimiter<T> {
    remaining: usize,
    _phantom: PhantomData<T>,
}

impl<T> GrpcMessageLimiter<T> {
    fn new(limit: usize) -> Self {
        Self {
            remaining: limit,
            _phantom: PhantomData,
        }
    }

    #[cfg(test)]
    fn empty() -> Self {
        Self::new(0)
    }

    // Returns true if the element, after serialising to proto bytes, fits within the remaining capacity.
    fn fits<U>(&mut self, el: T) -> Result<bool, GrpcProtoConversionError>
    where
        U: TryFrom<T, Error = GrpcProtoConversionError> + Message,
    {
        let required = U::try_from(el).map(|proto| proto.encoded_len())?;
        Ok(self.fits_raw(required))
    }

    /// Adds the given number of bytes to the remaining capacity.
    ///
    /// Returns whether we managed to fit the element.
    fn fits_raw(&mut self, bytes_len: usize) -> bool {
        if self.remaining < bytes_len {
            return false;
        }
        self.remaining = self.remaining.saturating_sub(bytes_len);
        true
    }

    /// Collects items while the predicate returns `Some`.
    ///
    /// The `try_take` closure should check if the item should be taken and return:
    /// - `Ok(Some(output))` to take the item (transformed)
    /// - `Ok(None)` to stop collection
    /// - `Err(_)` on error
    fn take_if<I, O, F>(&mut self, items: I, mut try_take: F) -> Result<Vec<O>, Status>
    where
        I: IntoIterator,
        F: FnMut(&mut Self, I::Item) -> Result<Option<O>, Status>,
    {
        let mut result = vec![];
        for item in items {
            match try_take(self, item)? {
                Some(output) => result.push(output),
                None => break,
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod proto_message_cap {
    use linera_base::crypto::CryptoHash;
    use linera_chain::{
        data_types::BlockExecutionOutcome,
        types::{Block, Certificate, ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_sdk::linera_base_types::{
        ChainId, TestString, ValidatorKeypair, ValidatorSignature,
    };

    use super::{CertificatesBatchResponse, GrpcMessageLimiter};

    fn test_certificate() -> Certificate {
        let keypair = ValidatorKeypair::generate();
        let validator = keypair.public_key;
        let signature = ValidatorSignature::new(&TestString::new("Test"), &keypair.secret_key);
        let block = Block::new(
            linera_chain::test::make_first_block(ChainId(CryptoHash::test_hash("root_chain"))),
            BlockExecutionOutcome::default(),
        );
        let signatures = vec![(validator, signature)];
        Certificate::Confirmed(ConfirmedBlockCertificate::new(
            ConfirmedBlock::new(block),
            Default::default(),
            signatures,
        ))
    }

    #[test]
    fn takes_up_to_limit() {
        let certificate = test_certificate();
        let single_cert_size = prost::Message::encoded_len(
            &CertificatesBatchResponse::try_from(vec![certificate.clone()]).unwrap(),
        );
        let certificates = vec![certificate.clone(), certificate.clone()];

        let mut empty_limiter = GrpcMessageLimiter::empty();
        assert!(!empty_limiter
            .fits::<super::Certificate>(certificate.clone())
            .unwrap());

        let mut single_message_limiter = GrpcMessageLimiter::new(single_cert_size);
        assert_eq!(
            certificates
                .clone()
                .into_iter()
                .take_while(|cert| single_message_limiter
                    .fits::<super::Certificate>(cert.clone())
                    .unwrap())
                .collect::<Vec<_>>(),
            vec![certificate.clone()]
        );

        let mut double_message_limiter = GrpcMessageLimiter::new(single_cert_size * 2);
        assert_eq!(
            certificates
                .into_iter()
                .take_while(|cert| double_message_limiter
                    .fits::<super::Certificate>(cert.clone())
                    .unwrap())
                .collect::<Vec<_>>(),
            vec![certificate.clone(), certificate.clone()]
        );
    }
}
