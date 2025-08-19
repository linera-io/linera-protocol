// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use futures::StreamExt;
use linera_base::time::Duration;
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

#[cfg(with_metrics)]
use super::indexer_api::element::Payload;
use super::indexer_api::{indexer_client::IndexerClient as IndexerClientInner, Element};
use crate::ExporterError;

pub(super) struct IndexerClient {
    max_retries: u32,
    retry_delay: Duration,
    client: IndexerClientInner<Channel>,
    #[cfg(with_metrics)]
    // Tracks timestamps of when we start exporting a Block to an indexer.
    sent_latency: Arc<Mutex<VecDeque<linera_base::time::Instant>>>,
    #[cfg(with_metrics)]
    address: String,
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
            #[cfg(with_metrics)]
            sent_latency: Arc::from(Mutex::new(VecDeque::new())),
            #[cfg(with_metrics)]
            address: address.to_string(),
        })
    }

    // try to make a streaming connection with the destination
    pub(super) async fn setup_indexer_client(
        &mut self,
        queue_size: usize,
    ) -> Result<(Sender<Element>, Streaming<()>), ExporterError> {
        let mut retry_count = 0;
        loop {
            let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
            #[cfg(with_metrics)]
            let request = {
                let stream = ReceiverStream::new(receiver).map(|element: Element| {
                    if let Some(Payload::Block(_)) = &element.payload {
                        use linera_base::time::Instant;
                        self.sent_latency.lock().unwrap().push_back(Instant::now());
                    }
                    element
                });
                Request::new(stream.into_inner())
            };
            #[cfg(not(with_metrics))]
            let request = Request::new(ReceiverStream::new(receiver));
            match self.client.index_batch(request).await {
                Ok(res) => {
                    let ack_stream = res.into_inner().map(|response: Result<(), tonic::Status>| {
                        #[cfg(with_metrics)]
                        {
                            // We assume that indexer responds with ACKs only after storing a block
                            // and that it doesn't ACK blocks out of order.
                            let start_time = self
                                .sent_latency
                                .lock()
                                .unwrap()
                                .pop_front()
                                .expect("have timer waiting");
                            crate::metrics::DISPATCH_BLOCK_HISTOGRAM
                                .with_label_values(&[&self.address])
                                .observe(start_time.elapsed().as_secs_f64() * 1000.0);
                        }
                        response
                    });
                    return Ok((sender, ack_stream.into_inner()));
                }
                Err(e) => {
                    if retry_count > self.max_retries {
                        return Err(ExporterError::SynchronizationFailed(e.into()));
                    }

                    let delay = self.retry_delay.saturating_mul(retry_count);
                    sleep(delay).await;
                    retry_count += 1;
                }
            }
        }
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
