// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    data_types::{BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_chain::data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate};

use async_trait::async_trait;
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{NodeError, NotificationStream, ValidatorNode},
};
use linera_execution::committee::Committee;
use linera_service::{
    chain_listener::ChainListenerConfig,
    node_service::{Chains, NodeService},
};
use linera_storage::MemoryStoreClient;

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
    let chain_id = ChainId::from(ChainDescription::Root(0));
    let store = MemoryStoreClient::new(None);
    let chain_client = ChainClient::new(
        chain_id,
        Vec::new(),
        DummyValidatorNodeProvider,
        store,
        chain_id,
        0,
        None,
        Timestamp::now(),
        BlockHeight(0),
        std::time::Duration::from_micros(0),
        0,
    );
    let chains = Chains {
        list: Vec::new(),
        default: chain_id,
    };
    let config = ChainListenerConfig {
        delay_before_ms: 0,
        delay_after_ms: 0,
    };
    let service = NodeService::new(
        chain_client,
        config,
        std::num::NonZeroU16::new(8080).unwrap(),
        chains,
    );
    let schema = service.schema().sdl();
    print!("{}", schema);
    Ok(())
}
