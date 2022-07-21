use super::{
    CrossChainConfig, RunningServerState, Server, ShardConfig, ValidatorInternalNetworkConfig,
};
use crate::{
    chain_guards::ChainGuards,
    transport::{MessageHandler, NetworkProtocol},
};
use async_trait::async_trait;
use futures::channel::mpsc;
use linera_base::{
    chain::ChainState,
    crypto::HashValue,
    error::Error,
    messages::{Certificate, ChainId, ChainInfoQuery},
};
use linera_core::worker::WorkerState;
use linera_storage::Storage;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Test if messages for different chains are handled concurrently.
///
/// Check if handling two messages for two different chains using the [`DelayedEmptyStorage`] (which
/// forces each request to last for at least the specified delay) takes _less_ than twice the delay.
/// If that's the case, then the messages were handled concurrently.
#[tokio::test(start_paused = true)]
async fn messages_for_different_chains_are_handled_concurrently() {
    let delay = Duration::from_secs(10);
    let server = create_dummy_server(delay);

    let result = timeout(
        delay * 2,
        send_two_concurrent_requests_for(ChainId::root(0), ChainId::root(1), server),
    )
    .await;

    assert!(result.is_ok());
}

/// Create a dummy instance of [`RunningServerState`] using a dummy [`Storage`] implementation that
/// just delays chain read requests by `delay`.
fn create_dummy_server(delay: Duration) -> RunningServerState<DelayedEmptyStorage> {
    let shard = ShardConfig {
        host: "0.0.0.0".to_owned(),
        port: 65_001,
    };
    let shard_id = 0;
    let network = ValidatorInternalNetworkConfig {
        protocol: NetworkProtocol::Udp,
        shards: vec![shard],
    };

    let key_pair = None;
    let dummy_storage = DelayedEmptyStorage { delay };
    let state = WorkerState::new("worker".to_owned(), key_pair, dummy_storage);

    let cross_chain_config = CrossChainConfig {
        queue_size: 1,
        max_retries: 1,
        retry_delay_ms: 1,
    };

    let host = "0.0.0.0".to_owned();
    let port = 65000;
    let server = Server::new(network, host, port, state, shard_id, cross_chain_config);

    let (cross_chain_sender, _) = mpsc::channel(1);

    RunningServerState {
        server,
        cross_chain_sender,
        chain_guards: ChainGuards::default(),
    }
}

/// Concurrently send two dummy [`ChainInfoQuery`] messages to `server`, one for the `first_chain`,
/// and one for the `second_chain`, and wait until they both finish.
async fn send_two_concurrent_requests_for(
    first_chain: ChainId,
    second_chain: ChainId,
    server: RunningServerState<DelayedEmptyStorage>,
) {
    let mut server_for_first_task = server.clone();
    let mut server_for_second_task = server;

    let first_task = tokio::spawn(async move {
        server_for_first_task
            .handle_message(ChainInfoQuery::new(first_chain).into())
            .await
    });
    let second_task = tokio::spawn(async move {
        server_for_second_task
            .handle_message(ChainInfoQuery::new(second_chain).into())
            .await
    });

    first_task
        .await
        .expect("Handling of fake request for the first chain failed");
    second_task
        .await
        .expect("Handling of fake request for the second chain failed");
}

/// A dummy implementation of [`DelayedEmptyStorage`] that has stub implementations of all its
/// methods, in addition to forcing the `read_chain_or_default` method to always sleep for a
/// preconfigured amount.
#[derive(Clone)]
pub struct DelayedEmptyStorage {
    delay: Duration,
}

#[async_trait]
impl Storage for DelayedEmptyStorage {
    async fn read_chain_or_default(&mut self, chain_id: ChainId) -> Result<ChainState, Error> {
        sleep(self.delay).await;
        Ok(ChainState::new(chain_id))
    }

    async fn write_chain(&mut self, _: ChainState) -> Result<(), Error> {
        Ok(())
    }

    async fn remove_chain(&mut self, _: ChainId) -> Result<(), Error> {
        Ok(())
    }

    async fn read_certificate(&mut self, hash: HashValue) -> Result<Certificate, Error> {
        Err(Error::MissingCertificate { hash })
    }

    async fn write_certificate(&mut self, _: Certificate) -> Result<(), Error> {
        Ok(())
    }
}
