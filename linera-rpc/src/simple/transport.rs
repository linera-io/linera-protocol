// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, io, sync::Arc};

use async_trait::async_trait;
use futures::{
    future,
    stream::{self, AbortHandle, AbortRegistration, Abortable},
    Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
    sync::Mutex,
};
use tokio_util::{codec::Framed, udp::UdpFramed};
use tracing::{error, warn};

use crate::{
    simple::{codec, codec::Codec},
    RpcMessage,
};

/// Suggested buffer size
pub const DEFAULT_MAX_DATAGRAM_SIZE: &str = "65507";

// Supported transport protocols.
#[derive(clap::ValueEnum, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransportProtocol {
    Udp,
    Tcp,
}

impl std::str::FromStr for TransportProtocol {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

impl std::fmt::Display for TransportProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl TransportProtocol {
    pub fn scheme(&self) -> &'static str {
        match self {
            TransportProtocol::Udp => "udp",
            TransportProtocol::Tcp => "tcp",
        }
    }
}

/// A pool of (outgoing) data streams.
pub trait ConnectionPool: Send {
    fn send_message_to<'a>(
        &'a mut self,
        message: RpcMessage,
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), codec::Error>>;
}

/// The handler required to create a service.
///
/// The implementation needs to implement [`Clone`] because a seed instance is used to generate
/// cloned instances, where each cloned instance handles a single request. Multiple cloned instances
/// may exist at the same time and handle separate requests concurrently.
#[async_trait]
pub trait MessageHandler: Clone {
    async fn handle_message(&mut self, message: RpcMessage) -> Option<RpcMessage>;
}

/// The result of spawning a server is a handle to track completion.
pub struct ServerHandle {
    pub handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl ServerHandle {
    pub async fn join(self) -> Result<(), std::io::Error> {
        // Note that dropping `self.complete` would terminate the server.
        self.handle.await??;
        Ok(())
    }
}

/// A trait alias for a protocol transport.
///
/// A transport is an active connection that can be used to send and receive
/// [`RpcMessage`]s.
pub trait Transport:
    Stream<Item = Result<RpcMessage, codec::Error>> + Sink<RpcMessage, Error = codec::Error>
{
}

impl<T> Transport for T where
    T: Stream<Item = Result<RpcMessage, codec::Error>> + Sink<RpcMessage, Error = codec::Error>
{
}

impl TransportProtocol {
    /// Creates a transport for this protocol.
    pub async fn connect(
        self,
        address: impl ToSocketAddrs,
    ) -> Result<impl Transport, std::io::Error> {
        let mut addresses = lookup_host(address)
            .await
            .expect("Invalid address to connect to");
        let address = addresses
            .next()
            .expect("Couldn't resolve address to connect to");

        let stream: futures::future::Either<_, _> = match self {
            TransportProtocol::Udp => {
                let socket = UdpSocket::bind(&"0.0.0.0:0").await?;

                UdpFramed::new(socket, Codec)
                    .with(move |message| future::ready(Ok((message, address))))
                    .map_ok(|(message, _address)| message)
                    .left_stream()
            }
            TransportProtocol::Tcp => {
                let stream = TcpStream::connect(address).await?;

                Framed::new(stream, Codec).right_stream()
            }
        };

        Ok(stream)
    }

    /// Creates a [`ConnectionPool`] for this protocol.
    pub async fn make_outgoing_connection_pool(
        self,
    ) -> Result<Box<dyn ConnectionPool>, std::io::Error> {
        let pool: Box<dyn ConnectionPool> = match self {
            Self::Udp => Box::new(UdpConnectionPool::new().await?),
            Self::Tcp => Box::new(TcpConnectionPool::new().await?),
        };
        Ok(pool)
    }

    /// Runs a server for this protocol and the given message handler.
    pub async fn spawn_server<S>(
        self,
        address: impl ToSocketAddrs,
        state: S,
    ) -> Result<ServerHandle, std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let (abort, registration) = AbortHandle::new_pair();
        let handle = match self {
            Self::Udp => {
                let socket = UdpSocket::bind(address).await?;
                tokio::spawn(Self::run_udp_server(socket, state, registration))
            }
            Self::Tcp => {
                let listener = TcpListener::bind(address).await?;
                tokio::spawn(Self::run_tcp_server(listener, state, registration))
            }
        };
        Ok(ServerHandle { handle })
    }
}

/// An implementation of [`ConnectionPool`] based on UDP.
struct UdpConnectionPool {
    transport: UdpFramed<Codec>,
}

impl UdpConnectionPool {
    async fn new() -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(&"0.0.0.0:0").await?;
        let transport = UdpFramed::new(socket, Codec);
        Ok(Self { transport })
    }
}

impl ConnectionPool for UdpConnectionPool {
    fn send_message_to<'a>(
        &'a mut self,
        message: RpcMessage,
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
impl TransportProtocol {
    async fn run_udp_server<S>(
        socket: UdpSocket,
        state: S,
        registration: AbortRegistration,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let (udp_sink, udp_stream) = UdpFramed::new(socket, Codec).split();
        let mut udp_stream = Abortable::new(udp_stream, registration);
        let udp_sink = Arc::new(Mutex::new(udp_sink));
        // Track the latest tasks for a given peer. This is used to return answers in the
        // same order as the queries.
        let mut previous_tasks = HashMap::new();

        while let Some(value) = udp_stream.next().await {
            let (message, peer) = match value {
                Ok(value) => value,
                Err(codec::Error::Io(io_error)) => return Err(io_error),
                Err(other_error) => {
                    warn!("Received an invalid message: {other_error}");
                    continue;
                }
            };

            let previous_task = previous_tasks.remove(&peer);
            let mut state = state.clone();
            let udp_sink = udp_sink.clone();
            let new_task = tokio::spawn(async move {
                if let Some(reply) = state.handle_message(message).await {
                    if let Some(task) = previous_task {
                        if let Err(error) = task.await {
                            warn!("Previous task cannot be joined: {}", error);
                        }
                    }
                    let status = udp_sink.lock().await.send((reply, peer)).await;
                    if let Err(error) = status {
                        error!("Failed to send query response: {}", error);
                    }
                }
            });
            previous_tasks.insert(peer, new_task);
            if previous_tasks.len() >= 100 {
                // Collect finished tasks to avoid leaking memory.
                previous_tasks.retain(|_, task| !task.is_finished());
            }
        }
        Ok(())
    }
}

/// An implementation of [`ConnectionPool`] based on TCP.
struct TcpConnectionPool {
    streams: HashMap<String, Framed<TcpStream, Codec>>,
}

impl TcpConnectionPool {
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

impl ConnectionPool for TcpConnectionPool {
    fn send_message_to<'a>(
        &'a mut self,
        message: RpcMessage,
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
impl TransportProtocol {
    async fn run_tcp_server<S>(
        listener: TcpListener,
        state: S,
        registration: AbortRegistration,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let accept_stream = stream::try_unfold(listener, |listener| async move {
            let (socket, _) = listener.accept().await?;
            Ok::<_, io::Error>(Some((socket, listener)))
        });
        let mut accept_stream = Box::pin(Abortable::new(accept_stream, registration));
        while let Some(value) = accept_stream.next().await {
            let socket = value?;
            let mut handler = state.clone();
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

                    if let Some(reply) = handler.handle_message(message).await {
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
