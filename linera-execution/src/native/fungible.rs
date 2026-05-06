// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native runtime implementation of a fungible token application.
//!
//! Operations and queries follow the same wire-format as the SDK
//! `linera_sdk::abis::fungible::FungibleTokenAbi` so this app is a drop-in
//! replacement for a Wasm fungible application.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_graphql::{EmptySubscription, Object, Request, Schema};
use linera_base::{
    data_types::{Amount, Resources, SendMessageRequest, StreamUpdate},
    identifiers::{Account, AccountOwner, ChainId, OwnerSpender},
};
use serde::{Deserialize, Serialize};

use crate::{
    runtime::{ContractSyncRuntimeHandle, ServiceSyncRuntimeHandle},
    BaseRuntime, ContractRuntime, ExecutionError, ServiceRuntime, UserContract,
    UserContractInstance, UserContractModule, UserService, UserServiceInstance, UserServiceModule,
};

/// The ticker symbol enforced for the native fungible application.
pub const TICKER_SYMBOL: &str = "NAT";

/// An operation accepted by the native fungible application. The serde shape matches
/// `linera_sdk::abis::fungible::FungibleOperation`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FungibleOperation {
    Balance {
        owner: AccountOwner,
    },
    TickerSymbol,
    Approve {
        owner: AccountOwner,
        spender: AccountOwner,
        allowance: Amount,
    },
    Transfer {
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
    TransferFrom {
        owner: AccountOwner,
        spender: AccountOwner,
        amount: Amount,
        target_account: Account,
    },
    Claim {
        source_account: Account,
        amount: Amount,
        target_account: Account,
    },
}

/// A response from the native fungible application. The serde shape matches
/// `linera_sdk::abis::fungible::FungibleResponse`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub enum FungibleResponse {
    #[default]
    Ok,
    Balance(Amount),
    TickerSymbol(String),
}

/// The instantiation argument: an initial set of balances credited from the chain.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InitialState {
    pub accounts: BTreeMap<AccountOwner, Amount>,
}

/// Application parameters: only the ticker symbol.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Parameters {
    pub ticker_symbol: String,
}

/// Cross-chain message used to wake up subscribers when a remote transfer happens.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum NativeFungibleMessage {
    Notify,
}

/// The runtime-native fungible contract.
pub struct NativeFungibleContract {
    runtime: ContractSyncRuntimeHandle,
}

impl NativeFungibleContract {
    fn notify(&mut self, destination: ChainId) -> Result<(), ExecutionError> {
        let message = bcs::to_bytes(&NativeFungibleMessage::Notify).map_err(|error| {
            ExecutionError::UserError(format!("Failed to serialize Notify message: {error}"))
        })?;
        self.runtime.send_message(SendMessageRequest {
            destination,
            authenticated: true,
            is_tracked: false,
            grant: Resources::default(),
            message,
        })
    }

    fn maybe_notify(&mut self, destination: ChainId) -> Result<(), ExecutionError> {
        let chain_id = self.runtime.chain_id()?;
        if destination != chain_id {
            self.notify(destination)?;
        }
        Ok(())
    }
}

impl UserContract for NativeFungibleContract {
    fn instantiate(&mut self, argument: Vec<u8>) -> Result<(), ExecutionError> {
        let parameters_bytes = self.runtime.application_parameters()?;
        let parameters: Parameters =
            serde_json::from_slice(&parameters_bytes).map_err(|error| {
                ExecutionError::UserError(format!("Invalid native fungible parameters: {error}"))
            })?;
        if parameters.ticker_symbol != TICKER_SYMBOL {
            return Err(ExecutionError::UserError(format!(
                "Only {TICKER_SYMBOL} is accepted as ticker symbol"
            )));
        }
        let state: InitialState = serde_json::from_slice(&argument).map_err(|error| {
            ExecutionError::UserError(format!(
                "Invalid native fungible instantiation argument: {error}"
            ))
        })?;
        let chain_id = self.runtime.chain_id()?;
        for (owner, amount) in state.accounts {
            let account = Account { chain_id, owner };
            self.runtime
                .transfer(AccountOwner::CHAIN, account, amount)?;
        }
        Ok(())
    }

    fn execute_operation(&mut self, operation: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        let operation: FungibleOperation = bcs::from_bytes(&operation).map_err(|error| {
            ExecutionError::UserError(format!("Invalid native fungible operation: {error}"))
        })?;
        let response = match operation {
            FungibleOperation::Balance { owner } => {
                FungibleResponse::Balance(self.runtime.read_owner_balance(owner)?)
            }
            FungibleOperation::TickerSymbol => {
                FungibleResponse::TickerSymbol(TICKER_SYMBOL.to_string())
            }
            FungibleOperation::Approve {
                owner,
                spender,
                allowance,
            } => {
                self.runtime.approve(owner, spender, allowance)?;
                FungibleResponse::Ok
            }
            FungibleOperation::Transfer {
                owner,
                amount,
                target_account,
            } => {
                // When invoked via `call_application`, authenticate the transfer using the
                // caller (depth 1) so that transfers from `AccountOwner::Application(caller)`
                // are authorized. When invoked directly, fall back to the current application.
                if self.runtime.authenticated_caller_id()?.is_some() {
                    self.runtime
                        .transfer_auth_depth(owner, target_account, amount, 1)?;
                } else {
                    self.runtime.transfer(owner, target_account, amount)?;
                }
                self.maybe_notify(target_account.chain_id)?;
                FungibleResponse::Ok
            }
            FungibleOperation::TransferFrom {
                owner,
                spender,
                amount,
                target_account,
            } => {
                self.runtime
                    .transfer_from(owner, spender, target_account, amount)?;
                self.maybe_notify(target_account.chain_id)?;
                FungibleResponse::Ok
            }
            FungibleOperation::Claim {
                source_account,
                amount,
                target_account,
            } => {
                if self.runtime.authenticated_caller_id()?.is_some() {
                    self.runtime
                        .claim_auth_depth(source_account, target_account, amount, 1)?;
                } else {
                    self.runtime.claim(source_account, target_account, amount)?;
                }
                let chain_id = self.runtime.chain_id()?;
                if source_account.chain_id == chain_id {
                    self.maybe_notify(target_account.chain_id)?;
                } else {
                    self.notify(source_account.chain_id)?;
                }
                FungibleResponse::Ok
            }
        };
        bcs::to_bytes(&response).map_err(|error| {
            ExecutionError::UserError(format!(
                "Failed to serialize native fungible response: {error}"
            ))
        })
    }

    fn execute_message(&mut self, message: Vec<u8>) -> Result<(), ExecutionError> {
        // The only message type the native fungible application sends is `Notify`,
        // which is intentionally a no-op (it just wakes up the destination chain).
        let _: NativeFungibleMessage = bcs::from_bytes(&message).map_err(|error| {
            ExecutionError::UserError(format!("Invalid native fungible message: {error}"))
        })?;
        Ok(())
    }

    fn process_streams(&mut self, _updates: Vec<StreamUpdate>) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}

/// Module factory for the native fungible contract.
#[derive(Clone)]
pub struct NativeFungibleContractModule;

impl UserContractModule for NativeFungibleContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
        Ok(Box::new(NativeFungibleContract { runtime }))
    }
}

/// The runtime-native fungible service.
///
/// Each query takes a snapshot of the chain's balances and allowances and answers
/// from that snapshot, so the GraphQL resolvers don't need to hold a runtime
/// reference. Mutations are appended to a `pending_operations` collector during
/// resolution and scheduled in a single batch after the schema executes.
pub struct NativeFungibleService {
    runtime: ServiceSyncRuntimeHandle,
}

#[derive(async_graphql::SimpleObject)]
struct AccountEntry {
    key: AccountOwner,
    value: Amount,
}

#[derive(async_graphql::SimpleObject)]
struct AllowanceEntry {
    key: OwnerSpender,
    value: Amount,
}

struct Accounts {
    balances: BTreeMap<AccountOwner, Amount>,
}

#[Object]
impl Accounts {
    async fn entry(&self, key: AccountOwner) -> AccountEntry {
        let value = self.balances.get(&key).copied().unwrap_or(Amount::ZERO);
        AccountEntry { key, value }
    }

    async fn entries(&self) -> Vec<AccountEntry> {
        self.balances
            .iter()
            .map(|(owner, amount)| AccountEntry {
                key: *owner,
                value: *amount,
            })
            .collect()
    }

    async fn keys(&self) -> Vec<AccountOwner> {
        self.balances.keys().copied().collect()
    }
}

struct Allowances {
    allowances: Vec<(AccountOwner, AccountOwner, Amount)>,
}

#[Object]
impl Allowances {
    async fn entry(&self, key: OwnerSpender) -> AllowanceEntry {
        let value = self
            .allowances
            .iter()
            .find_map(|(owner, spender, amount)| {
                (*owner == key.owner && *spender == key.spender).then_some(*amount)
            })
            .unwrap_or(Amount::ZERO);
        AllowanceEntry { key, value }
    }

    async fn entries(&self) -> Vec<AllowanceEntry> {
        self.allowances
            .iter()
            .map(|(owner, spender, amount)| AllowanceEntry {
                key: OwnerSpender {
                    owner: *owner,
                    spender: *spender,
                },
                value: *amount,
            })
            .collect()
    }
}

struct QueryRoot {
    balances: BTreeMap<AccountOwner, Amount>,
    allowances: Vec<(AccountOwner, AccountOwner, Amount)>,
}

#[Object]
impl QueryRoot {
    async fn ticker_symbol(&self) -> &str {
        TICKER_SYMBOL
    }

    async fn accounts(&self) -> Accounts {
        Accounts {
            balances: self.balances.clone(),
        }
    }

    async fn allowances(&self) -> Allowances {
        Allowances {
            allowances: self.allowances.clone(),
        }
    }
}

struct MutationRoot {
    pending: Arc<Mutex<Vec<FungibleOperation>>>,
}

impl MutationRoot {
    fn push(&self, operation: FungibleOperation) -> [u8; 0] {
        self.pending
            .lock()
            .expect("MutationRoot pending lock poisoned")
            .push(operation);
        []
    }
}

#[Object]
impl MutationRoot {
    async fn balance(&self, owner: AccountOwner) -> [u8; 0] {
        self.push(FungibleOperation::Balance { owner })
    }

    async fn ticker_symbol(&self) -> [u8; 0] {
        self.push(FungibleOperation::TickerSymbol)
    }

    async fn approve(
        &self,
        owner: AccountOwner,
        spender: AccountOwner,
        allowance: Amount,
    ) -> [u8; 0] {
        self.push(FungibleOperation::Approve {
            owner,
            spender,
            allowance,
        })
    }

    async fn transfer(
        &self,
        owner: AccountOwner,
        amount: Amount,
        target_account: Account,
    ) -> [u8; 0] {
        self.push(FungibleOperation::Transfer {
            owner,
            amount,
            target_account,
        })
    }

    async fn transfer_from(
        &self,
        owner: AccountOwner,
        spender: AccountOwner,
        amount: Amount,
        target_account: Account,
    ) -> [u8; 0] {
        self.push(FungibleOperation::TransferFrom {
            owner,
            spender,
            amount,
            target_account,
        })
    }

    async fn claim(
        &self,
        source_account: Account,
        amount: Amount,
        target_account: Account,
    ) -> [u8; 0] {
        self.push(FungibleOperation::Claim {
            source_account,
            amount,
            target_account,
        })
    }
}

impl UserService for NativeFungibleService {
    fn handle_query(&mut self, argument: Vec<u8>) -> Result<Vec<u8>, ExecutionError> {
        let request: Request = serde_json::from_slice(&argument).map_err(|error| {
            ExecutionError::UserError(format!("Invalid GraphQL request: {error}"))
        })?;

        let balances: BTreeMap<AccountOwner, Amount> =
            self.runtime.read_owner_balances()?.into_iter().collect();
        let allowances = self.runtime.read_allowances()?;

        let pending: Arc<Mutex<Vec<FungibleOperation>>> = Arc::new(Mutex::new(Vec::new()));
        // The service runtime is already driven by a `futures::executor::block_on` higher
        // up the stack, and `block_on`-inside-`block_on` panics. Run the GraphQL schema on a
        // fresh OS thread so it gets its own executor context.
        let pending_clone = pending.clone();
        let response = std::thread::spawn(move || {
            let schema = Schema::build(
                QueryRoot {
                    balances,
                    allowances,
                },
                MutationRoot {
                    pending: pending_clone,
                },
                EmptySubscription,
            )
            .finish();
            futures::executor::block_on(schema.execute(request))
        })
        .join()
        .map_err(|_| {
            ExecutionError::UserError(
                "Native fungible GraphQL execution thread panicked".to_string(),
            )
        })?;
        let collected = pending
            .lock()
            .expect("MutationRoot pending lock poisoned")
            .split_off(0);
        for operation in collected {
            let bytes = bcs::to_bytes(&operation).map_err(|error| {
                ExecutionError::UserError(format!(
                    "Failed to serialize scheduled operation: {error}"
                ))
            })?;
            self.runtime.schedule_operation(bytes)?;
        }
        serde_json::to_vec(&response).map_err(|error| {
            ExecutionError::UserError(format!("Failed to serialize GraphQL response: {error}"))
        })
    }
}

/// Module factory for the native fungible service.
#[derive(Clone)]
pub struct NativeFungibleServiceModule;

impl UserServiceModule for NativeFungibleServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        Ok(Box::new(NativeFungibleService { runtime }))
    }
}
