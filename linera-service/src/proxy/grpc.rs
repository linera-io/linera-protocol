// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// `tracing::instrument` is not compatible with this nightly Clippy lint
#![allow(unknown_lints)]
#![allow(clippy::blocks_in_conditions)]

#[cfg(with_metrics)]
use std::sync::LazyLock;
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
use linera_client::config::GenesisConfig;
use linera_core::{notifier::ChannelNotifier, JoinSetExt as _};
use linera_rpc::{
    config::{
        ShardConfig, TlsConfig, ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig,
    },
    grpc::{
        api::{
            self,
            notifier_service_server::{NotifierService, NotifierServiceServer},
            validator_node_server::{ValidatorNode, ValidatorNodeServer},
            validator_worker_client::ValidatorWorkerClient,
            BlobContent, BlobId, BlobIds, BlockProposal, Certificate, CertificatesBatchRequest,
            CertificatesBatchResponse, ChainInfoQuery, ChainInfoResult, CryptoHash,
            HandlePendingBlobRequest, LiteCertificate, Notification, PendingBlobRequest,
            PendingBlobResult, SubscriptionRequest, VersionInfo,
        },
        pool::GrpcConnectionPool,
        GrpcProtoConversionError, GrpcProxyable, GRPC_CHUNKED_MESSAGE_FILL_LIMIT,
        GRPC_MAX_MESSAGE_SIZE,
    },
};
use linera_sdk::{base::Blob, views::ViewError};
use linera_storage::Storage;
use prost::Message;
use tokio::{select, task::JoinSet};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    transport::{Channel, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tower::{builder::ServiceBuilder, Layer, Service};
use tracing::{debug, info, instrument, Instrument as _, Level};
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        bucket_latencies, register_histogram_vec, register_int_counter_vec,
    },
    prometheus::{HistogramVec, IntCounterVec},
};

#[cfg(with_metrics)]
use crate::prometheus_server;

#[cfg(with_metrics)]
static PROXY_REQUEST_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "proxy_request_latency",
        "Proxy request latency",
        &[],
        bucket_latencies(500.0),
    )
});

#[cfg(with_metrics)]
static PROXY_REQUEST_COUNT: LazyLock<IntCounterVec> =
    LazyLock::new(|| register_int_counter_vec("proxy_request_count", "Proxy request count", &[]));

#[cfg(with_metrics)]
static PROXY_REQUEST_SUCCESS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "proxy_request_success",
        "Proxy request success",
        &["method_name"],
    )
});

#[cfg(with_metrics)]
static PROXY_REQUEST_ERROR: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "proxy_request_error",
        "Proxy request error",
        &["method_name"],
    )
});

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
                PROXY_REQUEST_LATENCY
                    .with_label_values(&[])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                PROXY_REQUEST_COUNT.with_label_values(&[]).inc();
            }
            Ok(response)
        }
        .boxed()
    }
}

#[derive(Clone)]
pub struct GrpcProxy<S>(Arc<GrpcProxyInner<S>>);

struct GrpcProxyInner<S> {
    public_config: ValidatorPublicNetworkConfig,
    internal_config: ValidatorInternalNetworkConfig,
    genesis_config: GenesisConfig,
    worker_connection_pool: GrpcConnectionPool,
    notifier: ChannelNotifier<Result<Notification, Status>>,
    tls: TlsConfig,
    storage: S,
}

impl<S> GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub fn new(
        public_config: ValidatorPublicNetworkConfig,
        internal_config: ValidatorInternalNetworkConfig,
        genesis_config: GenesisConfig,
        connect_timeout: Duration,
        timeout: Duration,
        tls: TlsConfig,
        storage: S,
    ) -> Self {
        Self(Arc::new(GrpcProxyInner {
            public_config,
            internal_config,
            genesis_config,
            worker_connection_pool: GrpcConnectionPool::default()
                .with_connect_timeout(connect_timeout)
                .with_timeout(timeout),
            notifier: ChannelNotifier::default(),
            tls,
            storage,
        }))
    }

    fn as_validator_node(&self) -> ValidatorNodeServer<Self> {
        ValidatorNodeServer::new(self.clone())
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }

    fn as_notifier_service(&self) -> NotifierServiceServer<Self> {
        NotifierServiceServer::new(self.clone())
    }

    fn public_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.public_config.port))
    }

    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.metrics_port))
    }

    fn internal_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.0.internal_config.port))
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
        info!("Starting gRPC server");
        let mut join_set = JoinSet::new();

        #[cfg(with_metrics)]
        prometheus_server::start_metrics(self.metrics_address(), shutdown_signal.clone());

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
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
                .layer(
                    ServiceBuilder::new()
                        .layer(PrometheusMetricsMiddlewareLayer)
                        .into_inner(),
                )
                .accept_http1(true)
                .add_service(health_service)
                .add_service(tonic_web::enable(self.as_validator_node()))
                .add_service(tonic_web::enable(reflection_service))
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

    async fn worker_client<R>(
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

    fn log_and_return_proxy_request_outcome(
        result: Result<Response<ChainInfoResult>, Status>,
        method_name: &str,
    ) -> Result<Response<ChainInfoResult>, Status> {
        #![allow(unused_variables)]
        match result {
            Ok(chain_info_result) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_SUCCESS
                    .with_label_values(&[method_name])
                    .inc();
                Ok(chain_info_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_ERROR.with_label_values(&[method_name]).inc();
                Err(status)
            }
        }
    }

    /// Returns the appropriate gRPC status for the given [`ViewError`].
    fn error_to_status(err: ViewError) -> Status {
        let mut status = match &err {
            ViewError::TooLargeValue | ViewError::BcsError(_) => {
                Status::invalid_argument(err.to_string())
            }
            ViewError::StoreError { .. }
            | ViewError::TokioJoinError(_)
            | ViewError::TryLockError(_)
            | ViewError::InconsistentEntries
            | ViewError::PostLoadValuesError
            | ViewError::IoError(_) => Status::internal(err.to_string()),
            ViewError::KeyTooLong | ViewError::ArithmeticError(_) => {
                Status::out_of_range(err.to_string())
            }
            ViewError::NotFound(_)
            | ViewError::BlobsNotFound(_)
            | ViewError::CannotAcquireCollectionEntry
            | ViewError::MissingEntries => Status::not_found(err.to_string()),
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

    #[instrument(skip_all, err(Display))]
    async fn handle_block_proposal(
        &self,
        request: Request<BlockProposal>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_block_proposal(inner).await,
            "handle_block_proposal",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_lite_certificate(
        &self,
        request: Request<LiteCertificate>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_lite_certificate(inner).await,
            "handle_lite_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_confirmed_certificate(
        &self,
        request: Request<api::HandleConfirmedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_confirmed_certificate(inner).await,
            "handle_confirmed_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_validated_certificate(
        &self,
        request: Request<api::HandleValidatedCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_validated_certificate(inner).await,
            "handle_validated_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_timeout_certificate(
        &self,
        request: Request<api::HandleTimeoutCertificateRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_timeout_certificate(inner).await,
            "handle_timeout_certificate",
        )
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_chain_info_query(
        &self,
        request: Request<ChainInfoQuery>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        Self::log_and_return_proxy_request_outcome(
            client.handle_chain_info_query(inner).await,
            "handle_chain_info_query",
        )
    }

    #[instrument(skip_all, err(Display))]
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

    #[instrument(skip_all, err(Display))]
    async fn get_genesis_config_hash(
        &self,
        _request: Request<()>,
    ) -> Result<Response<CryptoHash>, Status> {
        Ok(Response::new(self.0.genesis_config.hash().into()))
    }

    #[instrument(skip_all, err(Display))]
    async fn upload_blob(&self, request: Request<BlobContent>) -> Result<Response<BlobId>, Status> {
        let content: linera_sdk::base::BlobContent = request.into_inner().try_into()?;
        let blob = Blob::new(content);
        let id = blob.id();
        let result = self.0.storage.maybe_write_blobs(&[blob]).await;
        if !result.map_err(Self::error_to_status)?[0] {
            return Err(Status::not_found("Blob not found"));
        }
        Ok(Response::new(id.try_into()?))
    }

    #[instrument(skip_all, err(Display))]
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
            .map_err(Self::error_to_status)?;
        Ok(Response::new(blob.into_content().try_into()?))
    }

    #[instrument(skip_all, err(Display))]
    async fn download_pending_blob(
        &self,
        request: Request<PendingBlobRequest>,
    ) -> Result<Response<PendingBlobResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        #[cfg_attr(not(with_metrics), expect(clippy::needless_match))]
        match client.download_pending_blob(inner).await {
            Ok(blob_result) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_SUCCESS
                    .with_label_values(&["download_pending_blob"])
                    .inc();
                Ok(blob_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_ERROR
                    .with_label_values(&["download_pending_blob"])
                    .inc();
                Err(status)
            }
        }
    }

    #[instrument(skip_all, err(Display))]
    async fn handle_pending_blob(
        &self,
        request: Request<HandlePendingBlobRequest>,
    ) -> Result<Response<ChainInfoResult>, Status> {
        let (mut client, inner) = self.worker_client(request).await?;
        #[cfg_attr(not(with_metrics), expect(clippy::needless_match))]
        match client.handle_pending_blob(inner).await {
            Ok(blob_result) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_SUCCESS
                    .with_label_values(&["handle_pending_blob"])
                    .inc();
                Ok(blob_result)
            }
            Err(status) => {
                #[cfg(with_metrics)]
                PROXY_REQUEST_ERROR
                    .with_label_values(&["handle_pending_blob"])
                    .inc();
                Err(status)
            }
        }
    }

    #[instrument(skip_all, err(Display))]
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
            .map_err(Self::error_to_status)?
            .into();
        Ok(Response::new(certificate.try_into()?))
    }

    #[instrument(skip_all, err(Display))]
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

        // Use 70% of the max message size as a buffer capacity.
        // Leave 30% as overhead.
        let mut grpc_message_limiter: GrpcMessageLimiter<linera_chain::types::Certificate> =
            GrpcMessageLimiter::new(GRPC_CHUNKED_MESSAGE_FILL_LIMIT);

        let mut certificates = vec![];

        'outer: for batch in hashes.chunks(100) {
            for certificate in self
                .0
                .storage
                .read_certificates(batch.to_vec())
                .await
                .map_err(Self::error_to_status)?
            {
                if grpc_message_limiter.fits::<Certificate>(certificate.clone().into())? {
                    certificates.push(linera_chain::types::Certificate::from(certificate));
                } else {
                    break 'outer;
                }
            }
        }

        Ok(Response::new(CertificatesBatchResponse::try_from(
            certificates,
        )?))
    }

    #[instrument(skip_all, err(level = Level::WARN))]
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
            .map_err(Self::error_to_status)?;
        Ok(Response::new(blob_state.last_used_by.into()))
    }

    #[instrument(skip_all, err(level = Level::WARN))]
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
            .map_err(Self::error_to_status)?;
        Ok(Response::new(missing_blob_ids.try_into()?))
    }
}

#[async_trait]
impl<S> NotifierService for GrpcProxy<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[instrument(skip_all, err(Display))]
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        let chain_id = notification
            .chain_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("Missing field: chain_id."))?
            .try_into()?;
        self.0.notifier.notify_chain(&chain_id, &Ok(notification));
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
        if required > self.remaining {
            return Ok(false);
        }
        self.remaining -= required;
        Ok(true)
    }
}

#[cfg(test)]
mod proto_message_cap {
    use linera_base::{
        crypto::{KeyPair, Signature},
        hashed::Hashed,
    };
    use linera_chain::{
        data_types::{BlockExecutionOutcome, ExecutedBlock},
        types::{Certificate, ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_execution::committee::ValidatorName;
    use linera_sdk::base::{ChainId, TestString};

    use super::{CertificatesBatchResponse, GrpcMessageLimiter};

    fn test_certificate() -> Certificate {
        let keypair = KeyPair::generate();
        let validator = ValidatorName(keypair.public());
        let signature = Signature::new(&TestString::new("Test"), &keypair);
        let executed_block = ExecutedBlock {
            block: linera_chain::test::make_first_block(ChainId::root(0)),
            outcome: BlockExecutionOutcome::default(),
        };
        let signatures = vec![(validator, signature)];
        Certificate::Confirmed(ConfirmedBlockCertificate::new(
            Hashed::new(ConfirmedBlock::new(executed_block)),
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
