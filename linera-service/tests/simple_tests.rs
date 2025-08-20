// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "storage-service")]

mod guard;

use std::env;

use anyhow::Result;
use async_graphql::InputType;
use futures::{
    channel::mpsc,
    future::{self, Either},
    SinkExt, StreamExt,
};
use guard::INTEGRATION_TEST_GUARD;
use linera_base::{
    crypto::{CryptoHash, Secp256k1SecretKey},
    data_types::Amount,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId},
    time::{Duration, Instant},
    vm::VmRuntime,
};
use linera_core::worker::{Notification, Reason};
use linera_sdk::{
    abis::fungible::NativeFungibleTokenAbi,
    linera_base_types::{AccountSecretKey, BlobContent, BlockHeight, DataBlobHash},
};

use linera_service::cli_wrappers::local_net::{Database, LocalNetConfig};
use linera_service::cli_wrappers::Network;

use linera_service::{
    cli_wrappers::{
        local_net::{get_node_port, ProcessInbox},
        ApplicationWrapper, ClientWrapper, LineraNet, LineraNetConfig,
    },
    test_name,
    util::eventually,
};
use serde_json::{json, Value};
use test_case::test_case;

