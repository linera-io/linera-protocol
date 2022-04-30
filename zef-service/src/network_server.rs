use crate::transport::{MessageHandler, SpawnedServer};
use bytes::Bytes;
use futures::{stream::StreamExt as _, SinkExt as _};
use log::{debug, info, warn};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A simple network server used for benchmarking.
pub struct BenchmarkServer {
    /// The network address of the server.
    address: String,
    /// Keeps a channel to each client connection.
    handles: HashMap<SocketAddr, Sender<Bytes>>,
}

impl BenchmarkServer {
    pub fn spawn<H>(address: String, handler: H) -> Result<SpawnedServer, std::io::Error>
    where
        H: MessageHandler + Send + 'static,
    {
        let (complete, receiver) = futures::channel::oneshot::channel();
        let handle = tokio::spawn(async move {
            Self {
                address,
                handles: HashMap::new(),
            }
            .run(handler, receiver)
            .await
        });
        Ok(SpawnedServer { complete, handle })
    }

    async fn run<H>(
        &mut self,
        mut handler: H,
        _exit_future: futures::channel::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error>
    where
        H: MessageHandler + Send + 'static,
    {
        let (tx_block, mut rx_block) = channel(1_000);
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            tokio::select! {
                result = listener.accept() => match result {
                    Ok((socket, peer)) => {
                        info!("Incoming connection established with {}", peer);
                        let (tx_reply, rx_reply) = channel(1_000);
                        self.handles.insert(peer, tx_reply);

                        // TODO: cleanup the hashmap self.handles when this task ends.
                        Self::spawn_connection(socket, peer, tx_block.clone(), rx_reply);
                    },
                    Err(e) => {
                        warn!("Failed to listen to client block: {}", e);
                    }
                },
                Some((peer, bytes)) = rx_block.recv() => {
                    if let Some(sender) = self.handles.get(&peer) {
                        if let Some(reply) = handler.handle_message(&bytes).await {
                            if let Err(e) = sender
                                .send(Bytes::from(reply))
                                .await
                            {
                                warn!("Failed to send reply to connection task: {}", e);
                                break;
                            }
                        }
                    }
                },
                else => break
            }
        }
        Ok(())
    }

    fn spawn_connection(
        socket: TcpStream,
        peer: SocketAddr,
        tx_block: Sender<(SocketAddr, Bytes)>,
        mut rx_reply: Receiver<Bytes>,
    ) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            loop {
                tokio::select! {
                    Some(frame) = reader.next() => match frame {
                        Ok(message) => {
                            if let Err(e) = tx_block
                            .send((peer, message.freeze()))
                            .await {
                                warn!("Failed to send message to main network task: {}", e);
                                break;
                            }
                        },
                        Err(e) => {
                            warn!("Failed to read TCP stream: {}", e);
                            break;
                        }
                    },
                    Some(reply) = rx_reply.recv() => {
                        if let Err(e) = writer.send(reply).await {
                            warn!("Failed to send reply to client: {}", e);
                            break;
                        }
                    },
                    else => break
                }
            }
            info!("Connection closed");
        });
    }
}
