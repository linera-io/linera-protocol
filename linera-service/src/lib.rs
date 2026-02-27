// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module provides the executables needed to operate a Linera service, including a placeholder wallet acting as a GraphQL service for user interfaces.

#![recursion_limit = "256"]

pub mod cli;
pub mod cli_wrappers;
pub mod config;
pub mod controller;
pub mod node_service;
pub mod project;
pub mod storage;
pub mod task_processor;
pub mod tracing;
pub mod util;
pub mod wallet;
pub use wallet::Wallet;
