// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::ChainId;
use linera_chain::data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate};
use linera_views::memory::TEST_MEMORY_MAX_STREAM_QUERIES;

use async_trait::async_trait;
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, NotificationStream, ValidatorNode, ValidatorNodeProvider},
};
use linera_execution::committee::Committee;
use linera_service::{chain_listener::ChainListenerConfig, node_service::NodeService};
use linera_storage::{MemoryStoreClient, WallClock};

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
    ) -> Result<ChainInfoResponse, NodeError> {
        Err(NodeError::UnexpectedMessage)
    }

    async fn handle_certificate(
        &mut self,
        _: Certificate,
        _: Vec<HashedValue>,
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

fn main() -> std::io::Result<()> {
    let store = MemoryStoreClient::new(None, TEST_MEMORY_MAX_STREAM_QUERIES, WallClock);
    let config = ChainListenerConfig {
        delay_before_ms: 0,
        delay_after_ms: 0,
    };
    let service = NodeService::<DummyValidatorNodeProvider, _>::new(
        config,
        std::num::NonZeroU16::new(8080).unwrap(),
        None,
        store,
    );
    let schema = service.schema().sdl();
    print!("{}", schema);
    Ok(())
}
