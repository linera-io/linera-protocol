// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Crowd-funding Example Application
//!
//! This example application implements crowd-funding campaigns using fungible tokens in
//! the `fungible` application. This demonstrates how to compose applications together and
//! how to instantiate applications where one chain has a special role.
//!
//! Once this application is built and its bytecode published on a Linera chain, the
//! published bytecode can be used to create different instances, where each
//! instance represents a different campaign.
//!
//! # How It Works
//!
//! The chain that created the campaign is called the "campaign chain". It is owned by the
//! creator (and beneficiary) of the campaign.
//!
//! The goal of a crowd-funding campaign is to let people pledge any number of tokens from
//! their own chain(s). If enough tokens are pledged before the campaign expires, the
//! campaign is *successful* and the creator can receive all the funds. Otherwise, the
//! campaign is *unsuccessful* and contributors should be refunded.
//!
//! # Caveat
//!
//! Currently, only the owner of the campaign can create blocks that contain the `Cancel`
//! operation. In the future, campaign chains will not be single-owner chains and should
//! instead allow contributors (users with a pledge) to cancel the campaign if
//! appropriate (even without the owner's cooperation).
//!
//! Optionally, contributors may also be able to create a block to accept a new epoch
//! (i.e. a change of validators).
//!
//! # Usage
//!
//! ## Setting Up
//!
//! The WebAssembly binaries for the bytecode can be built and published using [steps from the
//! book](https://linera-io.github.io/linera-documentation/getting_started/first_app.html),
//! summarized below.
//!
//! First, setup a local network with two wallets, and keep it running in a separate terminal:
//!
//! ```bash
//! ./scripts/run_local.sh
//! ```
//!
//! Compile the Wasm binaries for the two applications `fungible` and `crowd-funding`
//! application, and publish them as an application bytecode:
//!
//! ```bash
//! alias linera="$PWD/target/debug/linera"
//! export LINERA_WALLET="$PWD/target/debug/wallet.json"
//! export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"
//! export LINERA_WALLET2="$PWD/target/debug/wallet_2.json"
//! export LINERA_STORAGE2="rocksdb:$(dirname "$LINERA_WALLET2")/linera_2.db"
//!
//! (cargo build && cd examples && cargo build --release)
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-bytecode \
//!   examples/target/wasm32-unknown-unknown/release/fungible_{contract,service}.wasm
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-bytecode \
//!   examples/target/wasm32-unknown-unknown/release/crowd-funding_{contract,service}.wasm
//! ```
//!
//! This will output two new bytecode IDs, for instance:
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
//! ```
//!
//! We will refer to them as `$BYTECODE_ID1` and `$BYTECODE_ID2`.
//!
//! ## Creating a Token
//!
//! In order to use the published bytecode to create a token application, the initial state must be
//! specified. This initial state is where the tokens are minted. After the token is created, no
//! additional tokens can be minted and added to the application. The initial state is a JSON string
//! that specifies the accounts that start with tokens.
//!
//! In order to select the accounts to have initial tokens, the command below can be used to list
//! the chains created for the test as known by each wallet:
//!
//! ```bash
//! linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" wallet show
//! linera --storage "$LINERA_STORAGE2" --wallet "$LINERA_WALLET2" wallet show
//! ```
//!
//! A table will be shown with the chains registered in the wallet and their meta-data.
//! The default chain of each wallet should be highlighted in green. Each chain has an
//! `Owner` field, and that is what is used for the account. Let's pick the owners of the
//! default chain of each wallet chain. Remember the corresponding chain IDs as
//! `$CHAIN_ID1` (the chain where we just published the bytecode) and `$CHAIN_ID2` (some
//! user chain).
//!
//! The example below creates a token application where two accounts start with the minted tokens,
//! one with 100 of them and another with 200 of them:
//!
//! ```bash
//! linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" create-application $BYTECODE_ID1 \
//!     --json-argument '{ "accounts": { "User:445991f46ae490fe207e60c95d0ed95bf4e7ed9c270d4fd4fa555587c2604fe1": "100.", "User:c2f98d76c332bf809d7f91671eb76e5839c02d5896209881368da5838d85c83f": "200." } }'
//! ```
//!
//! This will output the application ID for the newly created token, e.g.:
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
//! ```
//!
//! Let remember it as `$APP_ID1`.
//!
//! ## Creating a crowd-funding campaign
//!
//! Similarly, we're going to create a crowd-funding campaign on the default chain.
//! ```bash
//! linera --storage "$LINERA_STORAGE" --wallet "$LINERA_WALLET" create-application $BYTECODE_ID2 \
//!    --json-argument '{ "owner": "User:504e41bc8a35ebf92f248009fccb1c55e2e59473f30d4249a2b88815fba48ef4", "deadline": 4102473600000000, "target": "100." }'
//! ```
//!
//! Let remember the application ID as `$APP_ID1`.
//!
//! ## Interacting with the campaign
//!
//! First, a node service has to be started for each wallet, using two different ports. The
//! `$SOURCE_CHAIN_ID` and `$TARGET_CHAIN_ID` can be left blank to use the default chains from each
//! wallet:
//!
//! ```bash
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" service --port 8080 $SOURCE_CHAIN_ID &
//! linera --wallet "$LINERA_WALLET2" --storage "$LINERA_STORAGE2" service --port 8081 $TARGET_CHAIN_ID &
//! ```
//!
//! Then the web frontend has to be started
//!
//! ```bash
//! cd examples/fungible/web-frontend
//! npm install
//! npm start
//! ```
//!
//! The web UI can then be opened by navigating to
//! `http://localhost:3000/$APPLICATION_ID?owner=$SOURCE_ACCOUNT&port=$PORT`, where:
//!
//! - `$APPLICATION_ID` is the token application ID obtained when creating the token
//! - `$SOURCE_ACCOUNT` is the owner of the chosen sender account
//! - `$PORT` is the port the sender wallet service is listening to (`8080` for the sender wallet
//!   and `8081` for the receiver wallet as per the previous commands)
//!
//! Two browser instances can be opened, one for the sender account and one for the receiver
//! account. In the sender account browser, the target chain ID and account can be specified, as
//! well as the amount to send. Once sent, the balance on the receiver account browser should
//! automatically update.

use async_graphql::{Request, Response, SimpleObject};
use fungible::AccountOwner;
use linera_sdk::base::{Amount, ApplicationId, ContractAbi, ServiceAbi, Timestamp};
use serde::{Deserialize, Serialize};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct CrowdFundingAbi;

impl ContractAbi for CrowdFundingAbi {
    type InitializationArgument = InitializationArgument;
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;
    type Operation = Operation;
    type ApplicationCall = ApplicationCall;
    type Message = Message;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for CrowdFundingAbi {
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;
    type Query = Request;
    type QueryResponse = Response;
}

/// The initialization data required to create a crowd-funding campaign.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, SimpleObject)]
pub struct InitializationArgument {
    /// The receiver of the pledges of a successful campaign.
    pub owner: AccountOwner,
    /// The deadline of the campaign, after which it can be cancelled if it hasn't met its target.
    pub deadline: Timestamp,
    /// The funding target of the campaign.
    pub target: Amount,
}

impl std::fmt::Display for InitializationArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(self).expect("Serialization failed")
        )
    }
}

/// Operations that can be executed by the application.
#[derive(Debug, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Operation {
    /// Pledge some tokens to the campaign (from an account on the current chain to the campaign chain).
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}

/// Messages that can be exchanged across chains from the same application instance.
#[derive(Debug, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    /// Pledge some tokens to the campaign (from an account on the receiver chain).
    PledgeWithAccount { owner: AccountOwner, amount: Amount },
}

/// A cross-application call. This is meant to mimic operations, except triggered by another contract.
#[derive(Debug, Deserialize, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplicationCall {
    /// Pledge some tokens to the campaign (from an account on the current chain).
    PledgeWithTransfer { owner: AccountOwner, amount: Amount },
    /// Pledge some tokens to the campaign from a session (for now, campaign chain only).
    PledgeWithSessions { source: AccountOwner },
    /// Collect the pledges after the campaign has reached its target (campaign chain only).
    Collect,
    /// Cancel the campaign and refund all pledges after the campaign has reached its deadline (campaign chain only).
    Cancel,
}
