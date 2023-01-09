// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::grpc_network::{
    grpc::{
        notifier_service_client::NotifierServiceClient, validator_node_client::ValidatorNodeClient,
        validator_worker_client::ValidatorWorkerClient,
    },
    GrpcError,
};
use async_trait::async_trait;
use dashmap::{mapref::one::RefMut, DashMap};
use std::hash::Hash;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct ConnectionPool<C: Connect>(DashMap<C::Address, C>);

#[async_trait]
pub trait Connect: Sized + Clone {
    type Address: Clone + Hash + Eq;

    async fn connect(address: Self::Address) -> Result<Self, GrpcError>;

    fn pool() -> ConnectionPool<Self> {
        ConnectionPool::new()
    }
}

#[async_trait]
impl Connect for ValidatorWorkerClient<Channel> {
    type Address = String;

    async fn connect(address: Self::Address) -> Result<Self, GrpcError> {
        Ok(ValidatorWorkerClient::connect(address).await?)
    }
}

#[async_trait]
impl Connect for ValidatorNodeClient<Channel> {
    type Address = String;

    async fn connect(address: Self::Address) -> Result<Self, GrpcError> {
        Ok(ValidatorNodeClient::connect(address).await?)
    }
}

#[async_trait]
impl Connect for NotifierServiceClient<Channel> {
    type Address = String;

    async fn connect(address: Self::Address) -> Result<Self, GrpcError> {
        Ok(NotifierServiceClient::connect(address).await?)
    }
}

impl<C: Connect> Default for ConnectionPool<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Connect> ConnectionPool<C> {
    pub fn new() -> Self {
        Self(DashMap::new())
    }

    /// Get a mutable reference to a client if it exists - if not creates a new one.
    ///
    /// If a single thread is going to have sporadic access to an underlying client,
    /// this method is more efficient as there is no cloning overhead.
    pub async fn client_for_address_mut(
        &self,
        remote_address: C::Address,
    ) -> Result<RefMut<C::Address, C>, GrpcError> {
        Ok(self
            .0
            .entry(remote_address.clone())
            .or_insert(C::connect(remote_address).await?))
    }

    /// Clone's a client for the given address if it exists - if not, creates a new one.
    /// Cloning a gRPC client will re-use the underlying transport without needing to
    /// establish a new connection.
    ///
    /// For applications with a lot of contention, that is, threads accessing the same client
    /// concurrently,this option is going to be faster than getting a mutable reference.
    pub async fn cloned_client_for_address(
        &self,
        remote_address: C::Address,
    ) -> Result<C, GrpcError> {
        Ok(self
            .0
            .entry(remote_address.clone())
            .or_insert(C::connect(remote_address).await?)
            .value()
            .clone())
    }
}
