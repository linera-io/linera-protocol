// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::time::timeout;

async fn get_new_local_address() -> Result<String, std::io::Error> {
    let builder = net2::TcpBuilder::new_v4()?;
    builder.reuse_address(true)?;
    builder.bind("127.0.0.1:0")?;
    Ok(format!("{}", builder.local_addr()?))
}

struct TestService {
    counter: Arc<AtomicUsize>,
}

impl TestService {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        TestService { counter }
    }
}

impl MessageHandler for TestService {
    fn handle_message<'a>(
        &'a mut self,
        buffer: &'a [u8],
    ) -> future::BoxFuture<'a, Option<Vec<u8>>> {
        self.counter.fetch_add(buffer.len(), Ordering::Relaxed);
        Box::pin(async move { Some(Vec::from(buffer)) })
    }
}

async fn test_server(protocol: NetworkProtocol) -> Result<(usize, usize), std::io::Error> {
    let address = get_new_local_address().await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let mut received = 0;

    let server = protocol
        .spawn_server(&address, TestService::new(counter.clone()), 100)
        .await?;

    let mut client = protocol.connect(address.clone(), 1000).await?;
    client.write_data(b"abcdef").await?;
    received += client.read_data().await?.len();
    client.write_data(b"abcd").await?;
    received += client.read_data().await?.len();

    // Use a second connection (here pooled).
    let mut pool = protocol.make_outgoing_connection_pool().await?;
    pool.send_data_to(b"abc", &address).await?;

    // Try to read data on the first connection (should fail).
    received += timeout(Duration::from_millis(500), client.read_data())
        .await
        .unwrap_or_else(|_| Ok(Vec::new()))?
        .len();

    // Attempt to gracefully kill server.
    server.kill().await?;

    timeout(Duration::from_millis(500), client.write_data(b"abcd"))
        .await
        .unwrap_or(Ok(()))?;
    received += timeout(Duration::from_millis(500), client.read_data())
        .await
        .unwrap_or_else(|_| Ok(Vec::new()))?
        .len();

    Ok((counter.load(Ordering::Relaxed), received))
}

#[tokio::test]
async fn udp_server() {
    let (processed, received) = test_server(NetworkProtocol::Udp).await.unwrap();
    assert_eq!(processed, 13);
    assert_eq!(received, 10);
}

#[tokio::test]
async fn tcp_server() {
    let (processed, received) = test_server(NetworkProtocol::Tcp).await.unwrap();
    // Active TCP connections are allowed to finish before the server is gracefully killed.
    assert_eq!(processed, 17);
    assert_eq!(received, 14);
}
