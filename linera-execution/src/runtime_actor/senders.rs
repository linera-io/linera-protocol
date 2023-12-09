// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::runtime_actor::{ContractRequest, ServiceRequest};
use futures::channel::mpsc;

/// Gives access to the contract system API.
#[derive(Clone)]
pub struct ContractRuntimeSender {
    pub inner: mpsc::UnboundedSender<ContractRequest>,
}

impl ContractRuntimeSender {
    /// Creates a new [`ContractRuntimeSender`] instance.
    pub fn new(inner: mpsc::UnboundedSender<ContractRequest>) -> Self {
        Self { inner }
    }
}

/// Gives access to the service system API.
#[derive(Clone)]
pub struct ServiceRuntimeSender {
    pub inner: mpsc::UnboundedSender<ServiceRequest>,
}

impl ServiceRuntimeSender {
    /// Creates a new [`ServiceRuntimeSender`] instance.
    pub fn new(inner: mpsc::UnboundedSender<ServiceRequest>) -> Self {
        Self { inner }
    }
}
