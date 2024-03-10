// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::NonFungibleToken;
use async_trait::async_trait;
use fungible::Account;
use linera_sdk::{
    base::{AccountOwner, ApplicationId, Owner, WithContractAbi},
    contract::system_api,
    ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, ViewStateStorage,
};
use non_fungible::{Message, Nft, Operation, TokenId};
use std::collections::BTreeSet;
use thiserror::Error;

pub struct NonFungibleTokenContract {
    state: NonFungibleToken,
    runtime: ContractRuntime,
}

linera_sdk::contract!(NonFungibleTokenContract);

impl WithContractAbi for NonFungibleTokenContract {
    type Abi = non_fungible::NonFungibleTokenAbi;
}

#[async_trait]
impl Contract for NonFungibleTokenContract {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = NonFungibleToken;

    async fn new(state: NonFungibleToken, runtime: ContractRuntime) -> Result<Self, Self::Error> {
        Ok(NonFungibleTokenContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn initialize(
        &mut self,
        _runtime: &mut ContractRuntime,
        _state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());
        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        _runtime: &mut ContractRuntime,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::Mint { name, payload } => {
                let signer = self.runtime.authenticated_signer().unwrap();
                Ok(self.mint(AccountOwner::User(signer), name, payload).await)
            }

            Operation::Transfer {
                source_owner,
                token_id,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    self.runtime.authenticated_signer(),
                    source_owner,
                )?;

                let nft = self.get_nft(&token_id).await;
                Self::check_account_authentication(
                    None,
                    self.runtime.authenticated_signer(),
                    nft.owner,
                )?;

                Ok(self.transfer(nft, target_account).await)
            }

            Operation::Claim {
                source_account,
                token_id,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    self.runtime.authenticated_signer(),
                    source_account.owner,
                )?;

                if source_account.chain_id == system_api::current_chain_id() {
                    let nft = self.get_nft(&token_id).await;
                    Self::check_account_authentication(
                        None,
                        self.runtime.authenticated_signer(),
                        nft.owner,
                    )?;

                    Ok(self.transfer(nft, target_account).await)
                } else {
                    Ok(self.remote_claim(source_account, token_id, target_account))
                }
            }
        }
    }

    async fn execute_message(
        &mut self,
        _runtime: &mut ContractRuntime,
        message: Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match message {
            Message::Transfer {
                mut nft,
                target_account,
            } => {
                let is_bouncing = self
                    .runtime
                    .message_is_bouncing()
                    .expect("Message delivery status has to be available when executing a message");
                if !is_bouncing {
                    nft.owner = target_account.owner;
                }

                self.add_nft(nft).await;
                Ok(ExecutionOutcome::default())
            }

            Message::Claim {
                source_account,
                token_id,
                target_account,
            } => {
                Self::check_account_authentication(
                    None,
                    self.runtime.authenticated_signer(),
                    source_account.owner,
                )?;

                let nft = self.get_nft(&token_id).await;
                Self::check_account_authentication(
                    None,
                    self.runtime.authenticated_signer(),
                    nft.owner,
                )?;

                Ok(self.transfer(nft, target_account).await)
            }
        }
    }

    async fn handle_application_call(
        &mut self,
        _runtime: &mut ContractRuntime,
        call: Self::ApplicationCall,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
        match call {
            Self::ApplicationCall::Mint { name, payload } => {
                let signer = self.runtime.authenticated_caller_id().unwrap();
                let execution_outcome = self
                    .mint(AccountOwner::Application(signer), name, payload)
                    .await;

                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }

            Self::ApplicationCall::Transfer {
                source_owner,
                token_id,
                target_account,
            } => {
                Self::check_account_authentication(
                    self.runtime.authenticated_caller_id(),
                    self.runtime.authenticated_signer(),
                    source_owner,
                )?;

                let nft = self.get_nft(&token_id).await;
                Self::check_account_authentication(
                    self.runtime.authenticated_caller_id(),
                    self.runtime.authenticated_signer(),
                    nft.owner,
                )?;

                let execution_outcome = self.transfer(nft, target_account).await;
                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }

            Self::ApplicationCall::Claim {
                source_account,
                token_id,
                target_account,
            } => {
                Self::check_account_authentication(
                    self.runtime.authenticated_caller_id(),
                    self.runtime.authenticated_signer(),
                    source_account.owner,
                )?;

                let execution_outcome = if source_account.chain_id == system_api::current_chain_id()
                {
                    let nft = self.get_nft(&token_id).await;
                    Self::check_account_authentication(
                        self.runtime.authenticated_caller_id(),
                        self.runtime.authenticated_signer(),
                        nft.owner,
                    )?;

                    self.transfer(nft, target_account).await
                } else {
                    self.remote_claim(source_account, token_id, target_account)
                };

                Ok(ApplicationCallOutcome {
                    execution_outcome,
                    ..Default::default()
                })
            }
        }
    }
}

impl NonFungibleTokenContract {
    /// Verifies that a transfer is authenticated for this local account.
    fn check_account_authentication(
        authenticated_application_id: Option<ApplicationId>,
        authenticated_signer: Option<Owner>,
        owner: AccountOwner,
    ) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) if authenticated_signer == Some(address) => Ok(()),
            AccountOwner::Application(id) if authenticated_application_id == Some(id) => Ok(()),
            _ => Err(Error::IncorrectAuthentication),
        }
    }

    /// Transfers the specified NFT to another account.
    /// Authentication needs to have happened already.
    async fn transfer(
        &mut self,
        mut nft: Nft,
        target_account: Account,
    ) -> ExecutionOutcome<Message> {
        self.remove_nft(&nft).await;
        if target_account.chain_id == system_api::current_chain_id() {
            nft.owner = target_account.owner;
            self.add_nft(nft).await;
            ExecutionOutcome::default()
        } else {
            let message = Message::Transfer {
                nft,
                target_account,
            };

            ExecutionOutcome::default().with_tracked_message(target_account.chain_id, message)
        }
    }

    async fn get_nft(&self, token_id: &TokenId) -> Nft {
        self.state
            .nfts
            .get(token_id)
            .await
            .expect("Failure in retrieving NFT")
            .expect("NFT should not be None")
    }

    async fn mint(
        &mut self,
        owner: AccountOwner,
        name: String,
        payload: Vec<u8>,
    ) -> ExecutionOutcome<Message> {
        let token_id = Nft::create_token_id(
            &system_api::current_chain_id(),
            &system_api::current_application_id(),
            &name,
            &owner,
            &payload,
        );

        self.add_nft(Nft {
            token_id,
            owner,
            name,
            minter: owner,
            payload,
        })
        .await;

        ExecutionOutcome::default()
    }

    fn remote_claim(
        &mut self,
        source_account: Account,
        token_id: TokenId,
        target_account: Account,
    ) -> ExecutionOutcome<Message> {
        let message = Message::Claim {
            source_account,
            token_id,
            target_account,
        };
        ExecutionOutcome::default().with_authenticated_message(source_account.chain_id, message)
    }

    async fn add_nft(&mut self, nft: Nft) {
        let token_id = nft.token_id.clone();
        let owner = nft.owner;

        self.state
            .nfts
            .insert(&token_id, nft)
            .expect("Error in insert statement");
        if let Some(owned_token_ids) = self
            .state
            .owned_token_ids
            .get_mut(&owner)
            .await
            .expect("Error in get_mut statement")
        {
            owned_token_ids.insert(token_id);
        } else {
            let mut owned_token_ids = BTreeSet::new();
            owned_token_ids.insert(token_id);
            self.state
                .owned_token_ids
                .insert(&owner, owned_token_ids)
                .expect("Error in insert statement");
        }
    }

    async fn remove_nft(&mut self, nft: &Nft) {
        self.state
            .nfts
            .remove(&nft.token_id)
            .expect("Failure removing NFT");
        let owned_token_ids = self
            .state
            .owned_token_ids
            .get_mut(&nft.owner)
            .await
            .expect("Error in get_mut statement")
            .expect("NFT set should be there!");

        owned_token_ids.remove(&nft.token_id);
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Requested transfer does not have permission on this account.
    #[error("The requested transfer is not correctly authenticated.")]
    IncorrectAuthentication,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
