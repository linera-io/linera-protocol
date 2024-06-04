// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, io, mem, net::SocketAddr, pin::pin, sync::Arc};

use async_trait::async_trait;
use futures::{
    future,
    stream::{self, FuturesUnordered, SplitSink, SplitStream},
    Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
    sync::Mutex,
    task::JoinHandle,
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
    pub fn spawn_server<S>(
        self,
        address: impl ToSocketAddrs + Send + 'static,
        state: S,
    ) -> ServerHandle
    where
        S: MessageHandler + Send + 'static,
    {
        let handle = match self {
            Self::Udp => tokio::spawn(UdpServer::run(address, state)),
            Self::Tcp => tokio::spawn(Self::run_tcp_server(address, state)),
        };
        ServerHandle { handle }
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

/// Server implementation for UDP.
pub struct UdpServer<State> {
    handler: State,
    udp_sink: SharedUdpSink,
    udp_stream: SplitStream<UdpFramed<Codec>>,
    active_handlers: HashMap<SocketAddr, JoinHandle<()>>,
}

/// Type alias for the outgoing endpoint of UDP messages.
type SharedUdpSink = Arc<Mutex<SplitSink<UdpFramed<Codec>, (RpcMessage, SocketAddr)>>>;

impl<State> UdpServer<State>
where
    State: MessageHandler + Send + 'static,
{
    /// Runs the UDP server implementation.
    pub async fn run(address: impl ToSocketAddrs, state: State) -> Result<(), std::io::Error> {
        let mut server = Self::bind(address, state).await?;

        loop {
            match server.udp_stream.next().await {
                Some(Ok((message, peer))) => server.handle_message(message, peer),
                Some(Err(error)) => server.handle_error(error).await?,
                None => unreachable!("`UdpFramed` should never return `None`"),
            }
        }
    }

    /// Creates a [`UpdServer`] bound to the provided `address`, handling messages using the
    /// provided `handler`.
    async fn bind(address: impl ToSocketAddrs, handler: State) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(address).await?;
        let (udp_sink, udp_stream) = UdpFramed::new(socket, Codec).split();

        Ok(UdpServer {
            handler,
            udp_sink: Arc::new(Mutex::new(udp_sink)),
            udp_stream,
            active_handlers: HashMap::new(),
        })
    }

    /// Spawns a task to handle a single incoming message.
    fn handle_message(&mut self, message: RpcMessage, peer: SocketAddr) {
        let previous_task = self.active_handlers.remove(&peer);
        let mut state = self.handler.clone();
        let udp_sink = self.udp_sink.clone();

        let new_task = tokio::spawn(async move {
            if let Some(reply) = state.handle_message(message).await {
                if let Some(task) = previous_task {
                    if let Err(error) = task.await {
                        warn!("Message handler task panicked: {}", error);
                    }
                }
                let status = udp_sink.lock().await.send((reply, peer)).await;
                if let Err(error) = status {
                    error!("Failed to send query response: {}", error);
                }
            }
        });

        self.active_handlers.insert(peer, new_task);

        if self.active_handlers.len() >= 100 {
            // Collect finished tasks to avoid leaking memory.
            self.active_handlers.retain(|_, task| !task.is_finished());
        }
    }

    /// Handles an error while receiving a message.
    async fn handle_error(&mut self, error: codec::Error) -> Result<(), std::io::Error> {
        match error {
            codec::Error::Io(io_error) => {
                self.shutdown().await;
                Err(io_error)
            }
            other_error => {
                warn!("Received an invalid message: {other_error}");
                Ok(())
            }
        }
    }

    /// Gracefully shuts down the server, waiting for existing tasks to finish.
    async fn shutdown(&mut self) {
        let handlers = mem::take(&mut self.active_handlers);
        let mut handler_results = FuturesUnordered::from_iter(handlers.into_values());

        while let Some(result) = handler_results.next().await {
            if let Err(error) = result {
                warn!("Message handler panicked: {}", error);
            }
        }
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
    async fn run_tcp_server<S>(address: impl ToSocketAddrs, state: S) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let listener = TcpListener::bind(address).await?;
        let mut accept_stream = stream::try_unfold(listener, |listener| async move {
            let (socket, _) = listener.accept().await?;
            Ok::<_, io::Error>(Some((socket, listener)))
        });
        let mut accept_stream = pin!(accept_stream);
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
