// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use linera_rpc::{
    grpc::{transport::Options, GrpcError, GRPC_MAX_MESSAGE_SIZE},
    NodeOptions,
};
use tokio::{sync::mpsc::Sender, time::sleep};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Streaming,
};

use super::indexer_api::{indexer_client::IndexerClient as IndexerClientInner, Element};
use crate::ExporterError;

pub(super) struct IndexerClient {
    max_retries: u32,
    retry_delay: Duration,
    client: IndexerClientInner<Channel>,
}

impl IndexerClient {
    pub(super) fn new(address: &str, options: NodeOptions) -> Result<Self, GrpcError> {
        let channel = create_channel(address, (&options).into())?;
        let client = IndexerClientInner::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        Ok(Self {
            client,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
        })
    }

    pub(super) async fn synchronize_long_lived_stream(
        &mut self,
        queue_size: usize,
    ) -> Result<(Sender<Element>, Streaming<()>), ExporterError> {
        let mut retry_count = 0;
        let streams = loop {
            let (stream, sink) = tokio::sync::mpsc::channel(queue_size);
            let request = Request::new(ReceiverStream::new(sink));
            match self.client.index_batch(request).await {
                Ok(res) => break (stream, res.into_inner()),
                Err(e) => {
                    if retry_count > self.max_retries {
                        return Err(ExporterError::SynchronizationFailed(e.into()));
                    }

                    let delay = self.retry_delay.saturating_mul(retry_count);
                    tracing::error!("recieved error:{:?} when in attempt to establish a stream, re-trying in {:?} seconds", e, delay);
                    sleep(delay).await;
                    retry_count += 1;
                }
            }
        };

        Ok(streams)
    }
}

fn create_channel(address: &str, options: Options) -> Result<Channel, tonic::transport::Error> {
    let mut endpoint = Endpoint::from_shared(address.to_string())?
        .tls_config(tonic::transport::channel::ClientTlsConfig::default().with_webpki_roots())?;

    if let Some(timeout) = options.connect_timeout {
        endpoint = endpoint.connect_timeout(timeout);
    }

    if let Some(timeout) = options.timeout {
        endpoint = endpoint.timeout(timeout);
    }

    endpoint = endpoint
        .http2_keep_alive_interval(Duration::from_secs(20))
        .keep_alive_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(20)))
        .keep_alive_while_idle(true)
        .tcp_nodelay(false);

    Ok(endpoint.connect_lazy())
}
