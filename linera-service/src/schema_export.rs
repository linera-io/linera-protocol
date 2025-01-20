// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{BlobContent, Timestamp},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate, LiteCertificate, Timeout,
        ValidatedBlock,
    },
};
use linera_client::{
    chain_listener::{ChainListenerConfig, ClientContext},
    wallet::Wallet,
    Error,
};
use linera_core::{
    client::ChainClient,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider,
    },
};
use linera_execution::committee::{Committee, ValidatorName};
use linera_service::node_service::NodeService;
use linera_storage::{DbStorage, Storage};
use linera_version::VersionInfo;
use linera_views::memory::{MemoryStore, MemoryStoreConfig, TEST_MEMORY_MAX_STREAM_QUERIES};

#[derive(Clone)]
struct DummyValidatorNode;

impl ValidatorNode for DummyValidatorNode {
    type NotificationStream = NotificationStream;

    async fn handle_block_proposal(
        &self,
        _: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_lite_certificate(
        &self,
        _: LiteCertificate<'_>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_timeout_certificate(
        &self,
        _: GenericCertificate<Timeout>,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_confirmed_certificate(
        &self,
        _: GenericCertificate<ConfirmedBlock>,
        _delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_validated_certificate(
        &self,
        _: GenericCertificate<ValidatedBlock>,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_chain_info_query(
        &self,
        _: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_pending_blob(&self, _: ChainId, _: BlobId) -> Result<BlobContent, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_pending_blob(
        &self,
        _: ChainId,
        _: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn subscribe(&self, _: Vec<ChainId>) -> Result<NotificationStream, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn upload_blob(&self, _: BlobContent) -> Result<BlobId, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_blob(&self, _: BlobId) -> Result<BlobContent, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_certificate(
        &self,
        _: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn download_certificates(
        &self,
        _: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn blob_last_used_by(&self, _: BlobId) -> Result<CryptoHash, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn missing_blob_ids(&self, _: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }
}

struct DummyValidatorNodeProvider;

impl ValidatorNodeProvider for DummyValidatorNodeProvider {
    type Node = DummyValidatorNode;

    fn make_node(&self, _address: &str) -> Result<Self::Node, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    fn make_nodes(
        &self,
        _committee: &Committee,
    ) -> Result<impl Iterator<Item = (ValidatorName, Self::Node)> + '_, NodeError> {
        Err::<std::iter::Empty<_>, _>(NodeError::UnexpectedMessage)
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
impl<P: ValidatorNodeProvider + Send, S: Storage + Clone + Send + Sync + 'static> ClientContext
    for DummyContext<P, S>
{
    type ValidatorNodeProvider = P;
    type Storage = S;

    fn wallet(&self) -> &Wallet {
        unimplemented!()
    }

    fn make_chain_client(&self, _: ChainId) -> Result<ChainClient<P, S>, Error> {
        unimplemented!()
    }

    async fn update_wallet_for_new_chain(
        &mut self,
        _: ChainId,
        _: Option<KeyPair>,
        _: Timestamp,
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn update_wallet(&mut self, _: &ChainClient<P, S>) -> Result<(), Error> {
        Ok(())
    }

    fn clients(
        &self,
    ) -> Result<Vec<ChainClient<Self::ValidatorNodeProvider, Self::Storage>>, Error> {
        Ok(vec![])
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _options = <Options as clap::Parser>::parse();

    let store_config = MemoryStoreConfig::new(TEST_MEMORY_MAX_STREAM_QUERIES);
    let namespace = "schema_export";
    let root_key = &[];
    let storage = DbStorage::<MemoryStore, _>::initialize(store_config, namespace, root_key, None)
        .await
        .expect("storage");
    let config = ChainListenerConfig::default();
    let context = DummyContext::<DummyValidatorNodeProvider, _> {
        _phantom: std::marker::PhantomData,
    };
    let service = NodeService::new(
        config,
        std::num::NonZeroU16::new(8080).unwrap(),
        None,
        storage,
        context,
    )
    .await;
    let schema = service.schema().sdl();
    print!("{}", schema);
    Ok(())
}
