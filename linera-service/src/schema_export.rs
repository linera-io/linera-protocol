// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{Blob, HashedBlob, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::data_types::{
    BlockProposal, Certificate, HashedCertificateValue, LiteCertificate,
};
use linera_client::{
    chain_listener::{ChainListenerConfig, ClientContext},
    wallet::Wallet,
};
use linera_core::{
    client::{ChainClient, Client},
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{
        CrossChainMessageDelivery, LocalValidatorNodeProvider, NodeError, NotificationStream,
        ValidatorNode, ValidatorNodeProvider,
    },
};
use linera_execution::committee::Committee;
use linera_service::node_service::NodeService;
use linera_storage::{MemoryStorage, WallClock};
use linera_version::VersionInfo;

#[derive(Clone)]
struct DummyValidatorNode;

impl ValidatorNode for DummyValidatorNode {
    type NotificationStream = NotificationStream;

    async fn handle_block_proposal(
        &mut self,
        _: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_lite_certificate(
        &mut self,
        _: LiteCertificate<'_>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_certificate(
        &mut self,
        _: Certificate,
        _: Vec<HashedCertificateValue>,
        _: Vec<HashedBlob>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_chain_info_query(
        &mut self,
        _: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn subscribe(&mut self, _: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn get_version_info(&mut self) -> Result<VersionInfo, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_blob(&mut self, _: BlobId) -> Result<Blob, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_certificate_value(
        &mut self,
        _: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_certificate(&mut self, _: CryptoHash) -> Result<Certificate, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn blob_last_used_by(&mut self, _: BlobId) -> Result<CryptoHash, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }
}

struct DummyValidatorNodeProvider;

impl LocalValidatorNodeProvider for DummyValidatorNodeProvider {
    type Node = DummyValidatorNode;

    fn make_node(&self, _: &str) -> Result<Self::Node, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    fn make_nodes<I>(&self, _: &Committee) -> Result<I, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }
}

#[derive(clap::Parser)]
#[command(
    name = "linera-schema-export",
    about = "Export the GraphQL schema for the core data in a Linera chain",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct Options {}

#[derive(Clone)]
struct DummyContext;

type DummyStorage = MemoryStorage<WallClock>;

#[async_trait]
impl ClientContext for DummyContext {
    type ValidatorNodeProvider = DummyValidatorNodeProvider;
    type Storage = DummyStorage;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn make_chain_client(
        &self,
        _: ChainId,
    ) -> ChainClient<Self::ValidatorNodeProvider, Self::Storage> {
        unimplemented!()
    }

    fn update_wallet_for_new_chain(&mut self, _: ChainId, _: Option<KeyPair>, _: Timestamp) {}

    async fn update_wallet(&mut self, _: &ChainClient<Self::ValidatorNodeProvider, Self::Storage>) {
    }

    fn client(&self) -> std::sync::Arc<Client<Self::ValidatorNodeProvider, Self::Storage>> {
        unimplemented!()
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _options = <Options as clap::Parser>::parse();

    let namespace = "schema_export";
    let service = NodeService::new(
        ChainListenerConfig {
            delay_before_ms: 0,
            delay_after_ms: 0,
        },
        std::num::NonZeroU16::new(8080).unwrap(),
        None,
        DummyContext,
    );
    let schema = service.schema().sdl();
    print!("{}", schema);
    Ok(())
}
