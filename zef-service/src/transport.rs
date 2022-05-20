// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::codec::{self, Codec};
use clap::arg_enum;
use futures::{future, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use log::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_util::{codec::Framed, udp::UdpFramed};
use zef_base::serialize::SerializedMessage;

/// Suggested buffer size
pub const DEFAULT_MAX_DATAGRAM_SIZE: &str = "65507";

// Supported transport protocols.
arg_enum! {
    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub enum NetworkProtocol {
        Udp,
        Tcp,
    }
}

/// A pool of (outgoing) data streams.
pub trait DataStreamPool: Send {
    fn send_message_to<'a>(
        &'a mut self,
        message: SerializedMessage,
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), codec::Error>>;
}

/// The handler required to create a service.
pub trait MessageHandler {
    fn handle_message(
        &mut self,
        message: SerializedMessage,
    ) -> future::BoxFuture<Option<SerializedMessage>>;
}

/// The result of spawning a server is oneshot channel to kill it and a handle to track completion.
pub struct SpawnedServer {
    pub complete: futures::channel::oneshot::Sender<()>,
    pub handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl SpawnedServer {
    pub async fn join(self) -> Result<(), std::io::Error> {
        // Note that dropping `self.complete` would terminate the server.
        self.handle.await??;
        Ok(())
    }

    pub async fn kill(self) -> Result<(), std::io::Error> {
        self.complete.send(()).unwrap();
        self.handle.await??;
        Ok(())
    }
}

/// A trait alias for a protocol transport.
///
/// A transport is an active connection that can be used to send and receive
/// [`SerializedMessages`]s.
pub trait Transport:
    Stream<Item = Result<SerializedMessage, codec::Error>>
    + Sink<SerializedMessage, Error = codec::Error>
{
}

impl<T> Transport for T where
    T: Stream<Item = Result<SerializedMessage, codec::Error>>
        + Sink<SerializedMessage, Error = codec::Error>
{
}

impl NetworkProtocol {
    /// Create a transport for this protocol.
    pub async fn connect(self, address: String) -> Result<impl Transport, std::io::Error> {
        let address: SocketAddr = address.parse().expect("Invalid address to connect to");

        let stream: futures::future::Either<_, _> = match self {
            NetworkProtocol::Udp => {
                let socket = UdpSocket::bind(&"0.0.0.0:0").await?;

                UdpFramed::new(socket, Codec)
                    .with(move |message| future::ready(Ok((message, address))))
                    .map_ok(|(message, _address)| message)
                    .left_stream()
            }
            NetworkProtocol::Tcp => {
                let stream = TcpStream::connect(address).await?;

                Framed::new(stream, Codec).right_stream()
            }
        };

        Ok(stream)
    }

    /// Create a DataStreamPool for this protocol.
    pub async fn make_outgoing_connection_pool(
        self,
    ) -> Result<Box<dyn DataStreamPool>, std::io::Error> {
        let pool: Box<dyn DataStreamPool> = match self {
            Self::Udp => Box::new(UdpDataStreamPool::new().await?),
            Self::Tcp => Box::new(TcpDataStreamPool::new().await?),
        };
        Ok(pool)
    }

    /// Run a server for this protocol and the given message handler.
    pub async fn spawn_server<S>(
        self,
        address: &str,
        state: S,
    ) -> Result<SpawnedServer, std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let (complete, receiver) = futures::channel::oneshot::channel();
        let handle = match self {
            Self::Udp => {
                let socket = UdpSocket::bind(&address).await?;
                tokio::spawn(Self::run_udp_server(socket, state, receiver))
            }
            Self::Tcp => {
                let listener = TcpListener::bind(address).await?;
                tokio::spawn(Self::run_tcp_server(listener, state, receiver))
            }
        };
        Ok(SpawnedServer { complete, handle })
    }
}

/// An implementation of DataStreamPool based on UDP.
struct UdpDataStreamPool {
    transport: UdpFramed<Codec>,
}

impl UdpDataStreamPool {
    async fn new() -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(&"0.0.0.0:0").await?;
        let transport = UdpFramed::new(socket, Codec);
        Ok(Self { transport })
    }
}

impl DataStreamPool for UdpDataStreamPool {
    fn send_message_to<'a>(
        &'a mut self,
        message: SerializedMessage,
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), codec::Error>> {
        Box::pin(async move {
            let address = address
                .parse()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))?;
            self.transport.send((message, address)).await
        })
    }
}

// Server implementation for UDP.
impl NetworkProtocol {
    async fn run_udp_server<S>(
        socket: UdpSocket,
        mut state: S,
        mut exit_future: futures::channel::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let mut transport = UdpFramed::new(socket, Codec);
        loop {
            let (message, peer) = match future::select(exit_future, transport.next()).await {
                future::Either::Left(_) => break,
                future::Either::Right((value, new_exit_future)) => {
                    exit_future = new_exit_future;
                    match value {
                        Some(Ok(value)) => value,
                        Some(Err(codec::Error::Io(io_error))) => return Err(io_error),
                        Some(Err(other_error)) => {
                            warn!("Received an invalid message: {other_error}");
                            continue;
                        }
                        None => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                    }
                }
            };
            if let Some(reply) = state.handle_message(message).await {
                let status = transport.send((reply, peer)).await;
                if let Err(error) = status {
                    error!("Failed to send query response: {}", error);
                }
            }
        }
        Ok(())
    }
}

/// An implementation of DataStreamPool based on TCP.
struct TcpDataStreamPool {
    streams: HashMap<String, Framed<TcpStream, Codec>>,
}

impl TcpDataStreamPool {
    async fn new() -> Result<Self, std::io::Error> {
        let streams = HashMap::new();
        Ok(Self { streams })
    }

    async fn get_stream(
        &mut self,
        address: &str,
    ) -> Result<&mut Framed<TcpStream, Codec>, io::Error> {
        if !self.streams.contains_key(address) {
            match TcpStream::connect(address).await {
                Ok(s) => {
                    self.streams
                        .insert(address.to_string(), Framed::new(s, Codec));
                }
                Err(error) => {
                    error!("Failed to open connection to {}: {}", address, error);
                    return Err(error);
                }
            };
        };
        Ok(self.streams.get_mut(address).unwrap())
    }
}

impl DataStreamPool for TcpDataStreamPool {
    fn send_message_to<'a>(
        &'a mut self,
        message: SerializedMessage,
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), codec::Error>> {
        Box::pin(async move {
            let stream = self.get_stream(address).await?;
            let result = stream.send(message).await;
            if result.is_err() {
                self.streams.remove(address);
            }
            result
        })
    }
}

// Server implementation for TCP.
impl NetworkProtocol {
    async fn run_tcp_server<S>(
        listener: TcpListener,
        state: S,
        mut exit_future: futures::channel::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let guarded_state = Arc::new(futures::lock::Mutex::new(state));
        loop {
            let (socket, _) = match future::select(exit_future, Box::pin(listener.accept())).await {
                future::Either::Left(_) => break,
                future::Either::Right((value, new_exit_future)) => {
                    exit_future = new_exit_future;
                    value?
                }
            };
            let guarded_state = guarded_state.clone();
            tokio::spawn(async move {
                let mut transport = Framed::new(socket, Codec);
                while let Some(maybe_message) = transport.next().await {
                    let message = match maybe_message {
                        Ok(message) => message,
                        Err(error) => {
                            // We expect some EOF or disconnect error at the end.
                            if !matches!(
                                &error,
                                codec::Error::Io(error)
                                    if error.kind() == io::ErrorKind::UnexpectedEof
                                    || error.kind() == io::ErrorKind::ConnectionReset
                            ) {
                                error!("Error while reading TCP stream: {}", error);
                            }

                            break;
                        }
                    };

                    if let Some(reply) = guarded_state.lock().await.handle_message(message).await {
                        if let Err(error) = transport.send(reply).await {
                            error!("Failed to send query response: {}", error);
                        }
                    }
                }
            });
        }
        Ok(())
    }
}
