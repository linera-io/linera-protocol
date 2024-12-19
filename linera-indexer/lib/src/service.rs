// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the service client for the indexer.

use async_tungstenite::{
    tokio::connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue},
};
use futures::{
    task::{FutureObj, Spawn, SpawnError},
    StreamExt,
};
use graphql_client::reqwest::post_graphql;
use graphql_ws_client::{graphql::StreamingOperation, GraphQLClientClientBuilder};
use linera_base::{
    crypto::CryptoHash, data_types::BlockHeight, hashed::Hashed, identifiers::ChainId,
    time::Duration,
};
use linera_chain::types::ConfirmedBlock;
use linera_core::worker::Reason;
use linera_service_graphql_client::{block, chains, notifications, Block, Chains, Notifications};
use linera_views::store::KeyValueStore;
use tokio::runtime::Handle;
use tracing::error;

use crate::{common::IndexerError, indexer::Indexer};

struct TokioSpawner(Handle);

impl Spawn for TokioSpawner {
    fn spawn_obj(&self, obj: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(obj);
        Ok(())
    }
}

pub enum Protocol {
    Http,
    WebSocket,
}

fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

#[derive(clap::Parser, Debug, Clone)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
pub struct Service {
    /// The port of the node service
    #[arg(long, default_value = "8080")]
    pub service_port: u16,
    /// The address of the node service
    #[arg(long, default_value = "localhost")]
    pub service_address: String,
    /// Use SSL/TLS
    #[arg(long)]
    pub tls: bool,
}

impl Service {
    pub fn with_protocol(&self, protocol: Protocol) -> String {
        let tls = if self.tls { "s" } else { "" };
        let (protocol, suffix) = match protocol {
            Protocol::Http => ("http", ""),
            Protocol::WebSocket => ("ws", "/ws"),
        };
        format!(
            "{}{}://{}:{}{}",
            protocol, tls, self.service_address, self.service_port, suffix
        )
    }

    pub fn websocket(&self) -> String {
        self.with_protocol(Protocol::WebSocket)
    }

    pub fn http(&self) -> String {
        self.with_protocol(Protocol::Http)
    }

    /// Gets one hashed value from the node service
    pub async fn get_value(
        &self,
        chain_id: ChainId,
        hash: Option<CryptoHash>,
    ) -> Result<Hashed<ConfirmedBlock>, IndexerError> {
        let client = reqwest_client();
        let variables = block::Variables { hash, chain_id };
        let response = post_graphql::<Block, _>(&client, &self.http(), variables).await?;
        response
            .data
            .ok_or_else(|| IndexerError::NullData(response.errors))?
            .block
            .ok_or_else(|| IndexerError::NotFound(hash))?
            .try_into()
            .map_err(IndexerError::UnknownCertificateStatus)
    }

    /// Gets chains
    pub async fn get_chains(&self) -> Result<Vec<ChainId>, IndexerError> {
        let client = reqwest_client();
        let variables = chains::Variables;
        let result = post_graphql::<Chains, _>(&client, &self.http(), variables).await?;
        Ok(result
            .data
            .ok_or(IndexerError::NullData(result.errors))?
            .chains
            .list)
    }
}

#[derive(clap::Parser, Debug, Clone)]
pub struct Listener {
    #[command(flatten)]
    pub service: Service,
    /// The height at which the indexer should start
    #[arg(long = "start", default_value = "0")]
    pub start: BlockHeight,
}

impl Listener {
    /// Connects to the websocket of the service node for a particular chain
    pub async fn listen<S>(
        &self,
        indexer: &Indexer<S>,
        chain_id: ChainId,
    ) -> Result<ChainId, IndexerError>
    where
        S: KeyValueStore + Clone + Send + Sync + 'static,
        S::Error: Send + Sync + std::error::Error + 'static,
    {
        let mut request = self.service.websocket().into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str("graphql-transport-ws")?,
        );
        let (connection, _) = connect_async(request).await?;
        let (sink, stream) = connection.split();
        let mut client = GraphQLClientClientBuilder::new()
            .build(stream, sink, TokioSpawner(Handle::current()))
            .await?;
        let operation: StreamingOperation<Notifications> =
            StreamingOperation::new(notifications::Variables { chain_id });
        let mut stream = client.streaming_operation(operation).await?;
        while let Some(item) = stream.next().await {
            match item {
                Ok(response) => {
                    if let Some(data) = response.data {
                        if let Reason::NewBlock { hash, .. } = data.notifications.reason {
                            if let Ok(value) = self.service.get_value(chain_id, Some(hash)).await {
                                indexer.process(self, &value).await?;
                            }
                        }
                    } else {
                        error!("null data from GraphQL WebSocket")
                    }
                }
                Err(error) => error!("error in WebSocket stream: {}", error),
            }
        }
        Ok(chain_id)
    }
}
