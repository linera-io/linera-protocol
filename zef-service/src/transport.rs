// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use clap::arg_enum;
use futures::future;
use log::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryInto, io, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};

#[cfg(test)]
#[path = "unit_tests/transport_tests.rs"]
mod transport_tests;

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

/// How to send and obtain data packets over an "active socket".
pub trait DataStream: Send {
    fn write_data<'a>(
        &'a mut self,
        buffer: &'a [u8],
    ) -> future::BoxFuture<'a, Result<(), std::io::Error>>;
    fn read_data(&mut self) -> future::BoxFuture<Result<Vec<u8>, std::io::Error>>;
}

/// A pool of (outgoing) data streams.
pub trait DataStreamPool: Send {
    fn send_data_to<'a>(
        &'a mut self,
        buffer: &'a [u8],
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), io::Error>>;
}

/// The handler required to create a service.
pub trait MessageHandler {
    fn handle_message<'a>(&'a mut self, buffer: &'a [u8])
        -> future::BoxFuture<'a, Option<Vec<u8>>>;
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

impl NetworkProtocol {
    /// Create a DataStream for this protocol.
    pub async fn connect(
        self,
        address: String,
        max_data_size: usize,
    ) -> Result<Box<dyn DataStream>, std::io::Error> {
        let stream: Box<dyn DataStream> = match self {
            NetworkProtocol::Udp => Box::new(UdpDataStream::connect(address, max_data_size).await?),
            NetworkProtocol::Tcp => Box::new(TcpDataStream::connect(address, max_data_size).await?),
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
        buffer_size: usize,
    ) -> Result<SpawnedServer, std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let (complete, receiver) = futures::channel::oneshot::channel();
        let handle = match self {
            Self::Udp => {
                let socket = UdpSocket::bind(&address).await?;
                tokio::spawn(Self::run_udp_server(socket, state, receiver, buffer_size))
            }
            Self::Tcp => {
                let listener = TcpListener::bind(address).await?;
                tokio::spawn(Self::run_tcp_server(listener, state, receiver, buffer_size))
            }
        };
        Ok(SpawnedServer { complete, handle })
    }
}

/// An implementation of DataStream based on UDP.
struct UdpDataStream {
    socket: UdpSocket,
    address: String,
    buffer: Vec<u8>,
}

impl UdpDataStream {
    async fn connect(address: String, max_data_size: usize) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(&"0.0.0.0:0").await?;
        let buffer = vec![0u8; max_data_size];
        Ok(Self {
            socket,
            address,
            buffer,
        })
    }
}

impl DataStream for UdpDataStream {
    fn write_data<'a>(
        &'a mut self,
        buffer: &'a [u8],
    ) -> future::BoxFuture<'a, Result<(), std::io::Error>> {
        Box::pin(async move {
            self.socket.send_to(buffer, &*self.address).await?;
            Ok(())
        })
    }

    fn read_data(&mut self) -> future::BoxFuture<Result<Vec<u8>, std::io::Error>> {
        Box::pin(async move {
            let size = self.socket.recv(&mut self.buffer).await?;
            Ok(self.buffer[..size].into())
        })
    }
}

/// An implementation of DataStreamPool based on UDP.
struct UdpDataStreamPool {
    socket: UdpSocket,
}

impl UdpDataStreamPool {
    async fn new() -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(&"0.0.0.0:0").await?;
        Ok(Self { socket })
    }
}

impl DataStreamPool for UdpDataStreamPool {
    fn send_data_to<'a>(
        &'a mut self,
        buffer: &'a [u8],
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), std::io::Error>> {
        Box::pin(async move {
            self.socket.send_to(buffer, address).await?;
            Ok(())
        })
    }
}

// Server implementation for UDP.
impl NetworkProtocol {
    async fn run_udp_server<S>(
        socket: UdpSocket,
        mut state: S,
        mut exit_future: futures::channel::oneshot::Receiver<()>,
        buffer_size: usize,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let mut buffer = vec![0; buffer_size];
        loop {
            let (size, peer) =
                match future::select(exit_future, Box::pin(socket.recv_from(&mut buffer))).await {
                    future::Either::Left(_) => break,
                    future::Either::Right((value, new_exit_future)) => {
                        exit_future = new_exit_future;
                        value?
                    }
                };
            if let Some(reply) = state.handle_message(&buffer[..size]).await {
                let status = socket.send_to(&reply[..], &peer).await;
                if let Err(error) = status {
                    error!("Failed to send query response: {}", error);
                }
            }
        }
        Ok(())
    }
}

/// An implementation of DataStream based on TCP.
struct TcpDataStream {
    stream: TcpStream,
    max_data_size: usize,
}

impl TcpDataStream {
    async fn connect(address: String, max_data_size: usize) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self {
            stream,
            max_data_size,
        })
    }

    async fn tcp_write_data<S>(stream: &mut S, buffer: &[u8]) -> Result<(), std::io::Error>
    where
        S: AsyncWrite + Unpin,
    {
        stream
            .write_all(&u32::to_le_bytes(
                buffer
                    .len()
                    .try_into()
                    .expect("length must not exceed u32::MAX"),
            ))
            .await?;
        stream.write_all(buffer).await
    }

    async fn tcp_read_data<S>(stream: &mut S, max_size: usize) -> Result<Vec<u8>, std::io::Error>
    where
        S: AsyncRead + Unpin,
    {
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await?;
        let size = u32::from_le_bytes(size_buf);
        if size as usize > max_size {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Message size exceeds buffer size",
            ));
        }
        let mut buf = vec![0u8; size as usize];
        stream.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

impl DataStream for TcpDataStream {
    fn write_data<'a>(
        &'a mut self,
        buffer: &'a [u8],
    ) -> future::BoxFuture<'a, Result<(), std::io::Error>> {
        Box::pin(Self::tcp_write_data(&mut self.stream, buffer))
    }

    fn read_data(&mut self) -> future::BoxFuture<Result<Vec<u8>, std::io::Error>> {
        Box::pin(Self::tcp_read_data(&mut self.stream, self.max_data_size))
    }
}

/// An implementation of DataStreamPool based on TCP.
struct TcpDataStreamPool {
    streams: HashMap<String, TcpStream>,
}

impl TcpDataStreamPool {
    async fn new() -> Result<Self, std::io::Error> {
        let streams = HashMap::new();
        Ok(Self { streams })
    }

    async fn get_stream(&mut self, address: &str) -> Result<&mut TcpStream, io::Error> {
        if !self.streams.contains_key(address) {
            match TcpStream::connect(address).await {
                Ok(s) => {
                    self.streams.insert(address.to_string(), s);
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
    fn send_data_to<'a>(
        &'a mut self,
        buffer: &'a [u8],
        address: &'a str,
    ) -> future::BoxFuture<'a, Result<(), std::io::Error>> {
        Box::pin(async move {
            let stream = self.get_stream(address).await?;
            let result = TcpDataStream::tcp_write_data(stream, buffer).await;
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
        buffer_size: usize,
    ) -> Result<(), std::io::Error>
    where
        S: MessageHandler + Send + 'static,
    {
        let guarded_state = Arc::new(futures::lock::Mutex::new(state));
        loop {
            let (mut socket, _) =
                match future::select(exit_future, Box::pin(listener.accept())).await {
                    future::Either::Left(_) => break,
                    future::Either::Right((value, new_exit_future)) => {
                        exit_future = new_exit_future;
                        value?
                    }
                };
            let guarded_state = guarded_state.clone();
            tokio::spawn(async move {
                loop {
                    let buffer = match TcpDataStream::tcp_read_data(&mut socket, buffer_size).await
                    {
                        Ok(buffer) => buffer,
                        Err(err) => {
                            // We expect some EOF or disconnect error at the end.
                            if err.kind() != io::ErrorKind::UnexpectedEof
                                && err.kind() != io::ErrorKind::ConnectionReset
                            {
                                error!("Error while reading TCP stream: {}", err);
                            }
                            break;
                        }
                    };

                    if let Some(reply) =
                        guarded_state.lock().await.handle_message(&buffer[..]).await
                    {
                        let status = TcpDataStream::tcp_write_data(&mut socket, &reply[..]).await;
                        if let Err(error) = status {
                            error!("Failed to send query response: {}", error);
                        }
                    };
                }
            });
        }
        Ok(())
    }
}
