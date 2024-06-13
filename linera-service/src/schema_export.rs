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
use linera_core::{
    client::ChainClient,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{
        CrossChainMessageDelivery, LocalValidatorNodeProvider, NodeError, NotificationStream,
        ValidatorNode,
    },
};
use linera_execution::committee::Committee;
use linera_service::{
    chain_listener::{ChainListenerConfig, ClientContext},
    node_service::NodeService,
    wallet::Wallet,
};
use linera_storage::{MemoryStorage, Storage};
use linera_version::VersionInfo;
use linera_views::{
    memory::{MemoryStoreConfig, TEST_MEMORY_MAX_STREAM_QUERIES},
    views::ViewError,
};

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

struct DummyContext<P, S> {
    _phantom: std::marker::PhantomData<(P, S)>,
}

#[async_trait]
impl<P: LocalValidatorNodeProvider + Send, S: Storage + Send + Sync> ClientContext
    for DummyContext<P, S>
{
    type ValidatorNodeProvider = P;
    type Storage = S;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn make_chain_client(&self, _: ChainId) -> ChainClient<P, S>
    where
        ViewError: From<S::ContextError>,
    {
        unimplemented!()
    }

    fn update_wallet_for_new_chain(&mut self, _: ChainId, _: Option<KeyPair>, _: Timestamp) {}

    async fn update_wallet<'a>(&'a mut self, _: &'a mut ChainClient<P, S>)
    where
        ViewError: From<S::ContextError>,
    {
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _options = <Options as clap::Parser>::parse();

    let store_config = MemoryStoreConfig::new(TEST_MEMORY_MAX_STREAM_QUERIES);
    let namespace = "schema_export";
    let storage = MemoryStorage::new(store_config, namespace, None)
        .await
        .expect("storage");
    let config = ChainListenerConfig {
        delay_before_ms: 0,
        delay_after_ms: 0,
    };
    let context = DummyContext {
        _phantom: std::marker::PhantomData,
    };
    let service = NodeService::<DummyValidatorNodeProvider, _, _>::new(
        config,
        std::num::NonZeroU16::new(8080).unwrap(),
        None,
        storage,
        context,
    );
    let schema = service.schema().sdl();
    print!("{}", schema);
    Ok(())
}
