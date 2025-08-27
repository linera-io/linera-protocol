// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use futures::{stream, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{
        Amount, Blob, BlockHeight, ChainDescription, ChainOrigin, Epoch, InitialChainConfig,
        OracleResponse, Round, Timestamp,
    },
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::BlockExecutionOutcome,
    manager::ChainManagerInfo,
    test::{make_child_block, make_first_block, BlockTestExt},
    types::{CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate},
};
use linera_core::{
    data_types::{ChainInfo, ChainInfoResponse},
    node::NodeError,
};
use linera_execution::{Operation, SystemOperation};
use linera_rpc::{
    grpc::api::{
        validator_node_server::{ValidatorNode, ValidatorNodeServer},
        BlobIds,
    },
    HandleConfirmedCertificateRequest,
};
use linera_service::config::DestinationKind;
use linera_storage::Storage;
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use crate::{
    common::{get_address, BlockId, CanonicalBlock},
    runloops::indexer_api::{
        element::Payload,
        indexer_server::{Indexer, IndexerServer},
        Element,
    },
};

#[derive(Clone, Default)]
pub(crate) struct DummyIndexer {
    pub(crate) fault_guard: Arc<AtomicBool>,
    pub(crate) blobs: Arc<DashSet<BlobId>>,
    pub(crate) state: Arc<DashSet<CryptoHash>>,
}

impl DummyIndexer {
    pub(crate) async fn start(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        let endpoint = get_address(port);
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<IndexerServer<Self>>().await;

        Server::builder()
            .add_service(health_service)
            .add_service(IndexerServer::new(self))
            .serve_with_shutdown(endpoint, cancellation_token.cancelled_owned())
            .await
            .expect("a running indexer");

        Ok(())
    }
}

#[async_trait]
impl Indexer for DummyIndexer {
    type IndexBatchStream = Pin<Box<dyn Stream<Item = Result<(), Status>> + Send + 'static>>;

    async fn index_batch(
        &self,
        request: Request<Streaming<Element>>,
    ) -> Result<Response<Self::IndexBatchStream>, Status> {
        let stream = request.into_inner();
        let is_faulty = self.fault_guard.clone();
        let state_moved = self.state.clone();
        let blobs_state_moved = self.blobs.clone();

        let output = stream::unfold(stream, move |mut stream| {
            let is_faulty_moved = is_faulty.clone();
            let moved_state = state_moved.clone();
            let moved_blobs_state = blobs_state_moved.clone();
            async move {
                while let Some(result) = stream.next().await {
                    if is_faulty_moved.load(Ordering::Acquire) {
                        return Some((Err(Status::from_error("err".into())), stream));
                    }

                    match result {
                        Ok(Element {
                            payload: Some(Payload::Block(indexer_block)),
                        }) => match TryInto::<ConfirmedBlockCertificate>::try_into(indexer_block) {
                            Ok(block) => {
                                moved_state.insert(block.hash());
                                return Some((Ok(()), stream));
                            }
                            Err(e) => return Some((Err(Status::from_error(e)), stream)),
                        },
                        Ok(Element {
                            payload: Some(Payload::Blob(indexer_blob)),
                        }) => {
                            let blob = Blob::try_from(indexer_blob).unwrap();
                            moved_blobs_state.insert(blob.id());
                        }
                        Ok(_) => continue,
                        Err(e) => return Some((Err(e), stream)),
                    }
                }
                None
            }
        });

        Ok(Response::new(Box::pin(output)))
    }
}

#[derive(Clone, Default)]
pub(crate) struct DummyValidator {
    pub(crate) validator_port: u16,
    pub(crate) fault_guard: Arc<AtomicBool>,
    pub(crate) blobs: Arc<DashSet<BlobId>>,
    pub(crate) state: Arc<DashSet<CryptoHash>>,
    // Tracks whether a block has been received multiple times.
    pub(crate) duplicate_blocks: Arc<DashMap<CryptoHash, u64>>,
}

impl DummyValidator {
    pub fn new(port: u16) -> Self {
        Self {
            validator_port: port,
            fault_guard: Arc::new(AtomicBool::new(false)),
            blobs: Arc::new(DashSet::new()),
            state: Arc::new(DashSet::new()),
            duplicate_blocks: Arc::new(DashMap::new()),
        }
    }

    pub(crate) async fn start(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        let endpoint = get_address(port);
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ValidatorNodeServer<Self>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(ValidatorNodeServer::new(self))
            .serve_with_shutdown(endpoint, cancellation_token.cancelled_owned())
            .await
            .expect("a running validator");

        Ok(())
    }
}

#[async_trait]
impl ValidatorNode for DummyValidator {
    type SubscribeStream =
        UnboundedReceiverStream<Result<linera_rpc::grpc::api::Notification, Status>>;

    async fn handle_confirmed_certificate(
        &self,
        request: Request<linera_rpc::grpc::api::HandleConfirmedCertificateRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        if self.fault_guard.load(Ordering::Acquire) {
            return Err(Status::from_error("err".into()));
        }

        let req = HandleConfirmedCertificateRequest::try_from(request.into_inner())
            .map_err(Status::from)?;

        let mut missing_blobs = Vec::new();
        let created_blobs = req.certificate.inner().block().created_blob_ids();
        for blob in req.certificate.inner().required_blob_ids() {
            if !self.blobs.contains(&blob) && !created_blobs.contains(&blob) {
                missing_blobs.push(blob);
            }
        }

        let chain_info = ChainInfo {
            chain_id: ChainId(CryptoHash::test_hash("test")),
            epoch: Epoch::ZERO,
            description: None,
            manager: ChainManagerInfo::default().into(),
            chain_balance: Amount::ONE,
            block_hash: None,
            timestamp: Timestamp::now(),
            next_block_height: BlockHeight::ZERO,
            state_hash: None,
            requested_owner_balance: None,
            requested_committees: None,
            requested_pending_message_bundles: vec![],
            requested_sent_certificate_hashes: vec![],
            count_received_log: 0,
            requested_received_log: vec![],
        };

        let response = if missing_blobs.is_empty() {
            let response = ChainInfoResponse::new(chain_info, None).try_into()?;
            for blob in created_blobs {
                self.blobs.insert(blob);
            }

            if !self.state.insert(req.certificate.hash()) {
                tracing::warn!(validator=?self.validator_port, certificate=?req.certificate.hash(), "duplicate block received");
                self.duplicate_blocks
                    .entry(req.certificate.hash())
                    .and_modify(|count| *count += 1)
                    .or_insert(2);
            }

            response
        } else {
            NodeError::BlobsNotFound(missing_blobs).try_into()?
        };

        Ok(Response::new(response))
    }

    async fn upload_blob(
        &self,
        request: Request<linera_rpc::grpc::api::BlobContent>,
    ) -> Result<Response<linera_rpc::grpc::api::BlobId>, Status> {
        if self.fault_guard.load(Ordering::Acquire) {
            return Err(Status::from_error("err".into()));
        }

        let content: linera_sdk::linera_base_types::BlobContent =
            request.into_inner().try_into()?;
        let blob = Blob::new(content);
        let id = blob.id();
        self.blobs.insert(id);
        Ok(Response::new(id.try_into()?))
    }

    async fn handle_block_proposal(
        &self,
        _request: Request<linera_rpc::grpc::api::BlockProposal>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn handle_lite_certificate(
        &self,
        _request: Request<linera_rpc::grpc::api::LiteCertificate>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn handle_validated_certificate(
        &self,
        _request: Request<linera_rpc::grpc::api::HandleValidatedCertificateRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn handle_timeout_certificate(
        &self,
        _request: Request<linera_rpc::grpc::api::HandleTimeoutCertificateRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn handle_chain_info_query(
        &self,
        _request: Request<linera_rpc::grpc::api::ChainInfoQuery>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn subscribe(
        &self,
        _request: Request<linera_rpc::grpc::api::SubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        unimplemented!()
    }

    async fn get_version_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<linera_rpc::grpc::api::VersionInfo>, Status> {
        unimplemented!()
    }

    async fn get_network_description(
        &self,
        _request: Request<()>,
    ) -> Result<Response<linera_rpc::grpc::api::NetworkDescription>, Status> {
        unimplemented!()
    }

    async fn download_blob(
        &self,
        _request: Request<linera_rpc::grpc::api::BlobId>,
    ) -> Result<Response<linera_rpc::grpc::api::BlobContent>, Status> {
        unimplemented!()
    }

    async fn download_pending_blob(
        &self,
        _request: Request<linera_rpc::grpc::api::PendingBlobRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::PendingBlobResult>, Status> {
        unimplemented!()
    }

    async fn handle_pending_blob(
        &self,
        _request: Request<linera_rpc::grpc::api::HandlePendingBlobRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::ChainInfoResult>, Status> {
        unimplemented!()
    }

    async fn download_certificate(
        &self,
        _request: Request<linera_rpc::grpc::api::CryptoHash>,
    ) -> Result<Response<linera_rpc::grpc::api::Certificate>, Status> {
        unimplemented!()
    }

    async fn download_certificates(
        &self,
        _request: Request<linera_rpc::grpc::api::CertificatesBatchRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::CertificatesBatchResponse>, Status> {
        unimplemented!()
    }

    async fn download_certificates_by_heights(
        &self,
        _request: Request<linera_rpc::grpc::api::DownloadCertificatesByHeightsRequest>,
    ) -> Result<Response<linera_rpc::grpc::api::CertificatesBatchResponse>, Status> {
        unimplemented!()
    }

    async fn blob_last_used_by(
        &self,
        _request: Request<linera_rpc::grpc::api::BlobId>,
    ) -> Result<Response<linera_rpc::grpc::api::CryptoHash>, Status> {
        unimplemented!()
    }

    async fn blob_last_used_by_certificate(
        &self,
        _request: Request<linera_rpc::grpc::api::BlobId>,
    ) -> Result<Response<linera_rpc::grpc::api::Certificate>, Status> {
        unimplemented!()
    }

    async fn missing_blob_ids(
        &self,
        _request: Request<BlobIds>,
    ) -> Result<Response<BlobIds>, Status> {
        unimplemented!()
    }
}

#[async_trait]
pub trait TestDestination {
    fn kind(&self) -> DestinationKind;
    fn blobs(&self) -> &DashSet<BlobId>;
    fn set_faulty(&self);
    fn unset_faulty(&self);
    fn state(&self) -> &DashSet<CryptoHash>;
    async fn start(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait]
impl TestDestination for DummyIndexer {
    fn kind(&self) -> DestinationKind {
        DestinationKind::Indexer
    }

    fn blobs(&self) -> &DashSet<BlobId> {
        self.blobs.as_ref()
    }

    fn set_faulty(&self) {
        self.fault_guard.store(true, Ordering::Release);
    }

    fn unset_faulty(&self) {
        self.fault_guard.store(false, Ordering::Release);
    }

    fn state(&self) -> &DashSet<CryptoHash> {
        self.state.as_ref()
    }

    async fn start(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        self.start(port, cancellation_token).await
    }
}

#[async_trait]
impl TestDestination for DummyValidator {
    fn kind(&self) -> DestinationKind {
        DestinationKind::Validator
    }

    fn blobs(&self) -> &DashSet<BlobId> {
        self.blobs.as_ref()
    }

    fn set_faulty(&self) {
        self.fault_guard.store(true, Ordering::Release);
    }

    fn unset_faulty(&self) {
        self.fault_guard.store(false, Ordering::Release);
    }

    fn state(&self) -> &DashSet<CryptoHash> {
        self.state.as_ref()
    }

    async fn start(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        self.start(port, cancellation_token).await
    }
}

/// Creates a chain state with two blocks, each containing blobs.
pub(crate) async fn make_simple_state_with_blobs<S: Storage>(
    storage: &S,
) -> (BlockId, Vec<CanonicalBlock>) {
    let chain_description = ChainDescription::new(
        ChainOrigin::Root(0),
        InitialChainConfig {
            ownership: Default::default(),
            epoch: Default::default(),
            balance: Default::default(),
            application_permissions: Default::default(),
            min_active_epoch: Default::default(),
            max_active_epoch: Default::default(),
        },
        Timestamp::now(),
    );

    let blob_1 = Blob::new_data("1".as_bytes());
    let blob_2 = Blob::new_data("2".as_bytes());
    let blob_3 = Blob::new_data("3".as_bytes());

    let chain_id = chain_description.id();
    let chain_blob = Blob::new_chain_description(&chain_description);

    let block_1 = ConfirmedBlock::new(
        BlockExecutionOutcome {
            blobs: vec![vec![blob_1.clone()]],
            ..Default::default()
        }
        .with(make_first_block(chain_id).with_operation(Operation::system(
            SystemOperation::PublishDataBlob {
                blob_hash: CryptoHash::new(blob_2.content()),
            },
        ))),
    );

    let block_2 = ConfirmedBlock::new(
        BlockExecutionOutcome {
            oracle_responses: vec![vec![OracleResponse::Blob(blob_3.id())]],
            ..Default::default()
        }
        .with(
            make_child_block(&block_1)
                .with_operation(Operation::system(SystemOperation::PublishDataBlob {
                    blob_hash: CryptoHash::new(blob_2.content()),
                }))
                .with_operation(Operation::system(SystemOperation::PublishDataBlob {
                    blob_hash: CryptoHash::new(blob_1.content()),
                })),
        ),
    );

    storage
        .write_blobs(&[chain_blob.clone(), blob_1, blob_2.clone(), blob_3.clone()])
        .await
        .unwrap();
    storage
        .write_blobs_and_certificate(
            &[],
            &ConfirmedBlockCertificate::new(block_1.clone(), Round::Fast, vec![]),
        )
        .await
        .unwrap();
    storage
        .write_blobs_and_certificate(
            &[],
            &ConfirmedBlockCertificate::new(block_2.clone(), Round::Fast, vec![]),
        )
        .await
        .unwrap();

    let notification = BlockId::from_confirmed_block(&block_2);

    let state = vec![
        CanonicalBlock::new(block_1.inner().hash(), &[blob_2.id(), chain_blob.id()]),
        CanonicalBlock::new(block_2.inner().hash(), &[blob_3.id()]),
    ];

    (notification, state)
}
