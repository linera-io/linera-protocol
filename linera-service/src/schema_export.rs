// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_base::{crypto::KeyPair, data_types::Timestamp, identifiers::ChainId};
use linera_chain::data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate};
use linera_core::{
    client::ChainClient,
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{
        CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
        ValidatorNodeProvider,
    },
};
use linera_execution::committee::Committee;
use linera_service::{
    chain_listener::{ChainListenerConfig, ClientContext},
    config::WalletState,
    node_service::NodeService,
};
use linera_storage::{MemoryStorage, Storage};
use linera_version::VersionInfo;
use linera_views::{memory::TEST_MEMORY_MAX_STREAM_QUERIES, views::ViewError};

#[derive(Clone)]
struct DummyValidatorNode;

#[async_trait]
impl ValidatorNode for DummyValidatorNode {
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
        _: Vec<HashedValue>,
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
}

struct DummyValidatorNodeProvider;

impl ValidatorNodeProvider for DummyValidatorNodeProvider {
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

struct DummyContext;

#[async_trait]
impl ClientContext<DummyValidatorNodeProvider> for DummyContext {
    fn wallet_state(&self) -> &WalletState {
        unimplemented!()
    }

    fn make_chain_client<S>(&self, _: S, _: ChainId) -> ChainClient<DummyValidatorNodeProvider, S> {
        unimplemented!()
    }

    fn update_wallet_for_new_chain(&mut self, _: ChainId, _: Option<KeyPair>, _: Timestamp) {}

    async fn update_wallet<'a, S>(
        &'a mut self,
        _: &'a mut ChainClient<DummyValidatorNodeProvider, S>,
    ) where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let _options = <Options as clap::Parser>::parse();

    let storage = MemoryStorage::new(None, TEST_MEMORY_MAX_STREAM_QUERIES)
        .await
        .expect("storage");
    let config = ChainListenerConfig {
        delay_before_ms: 0,
        delay_after_ms: 0,
    };
    let context = DummyContext;
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
