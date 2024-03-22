// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::collections::BTreeSet;

use async_trait::async_trait;
use fungible::Account;
use linera_sdk::{
    base::{AccountOwner, WithContractAbi},
    ensure, ApplicationCallOutcome, Contract, ContractRuntime, ExecutionOutcome, ViewStateStorage,
};
use non_fungible::{Message, Nft, NonFungibleTokenAbi, Operation, TokenId};
use thiserror::Error;

use self::state::NonFungibleToken;

pub struct NonFungibleTokenContract {
    state: NonFungibleToken,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(NonFungibleTokenContract);

impl WithContractAbi for NonFungibleTokenContract {
    type Abi = NonFungibleTokenAbi;
}

#[async_trait]
impl Contract for NonFungibleTokenContract {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = NonFungibleToken;

    async fn new(
        state: NonFungibleToken,
        runtime: ContractRuntime<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(NonFungibleTokenContract { state, runtime })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn initialize(
        &mut self,
        _state: Self::InitializationArgument,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
        self.state.num_minted_nfts.set(0);
        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        operation: Self::Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::Mint {
                minter,
                name,
                payload,
            } => {
                self.check_account_authentication(minter)?;
                self.mint(minter, name, payload).await
            }

            Operation::Transfer {
                source_owner,
                token_id,
                target_account,
            } => {
                self.check_account_authentication(source_owner)?;

                let nft = self.get_nft(&token_id).await?;
                self.check_account_authentication(nft.owner)?;

                Ok(self.transfer(nft, target_account).await)
            }

            Operation::Claim {
                source_account,
                token_id,
                target_account,
            } => {
                self.check_account_authentication(source_account.owner)?;

                if source_account.chain_id == self.runtime.chain_id() {
                    let nft = self.get_nft(&token_id).await?;
                    self.check_account_authentication(nft.owner)?;

                    Ok(self.transfer(nft, target_account).await)
                } else {
                    Ok(self.remote_claim(source_account, token_id, target_account))
                }
            }
        }
    }

    async fn execute_message(
        &mut self,
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
                self.check_account_authentication(source_account.owner)?;

                let nft = self.get_nft(&token_id).await?;
                self.check_account_authentication(nft.owner)?;

                Ok(self.transfer(nft, target_account).await)
            }
        }
    }

    async fn handle_application_call(
        &mut self,
        call: Self::ApplicationCall,
    ) -> Result<ApplicationCallOutcome<Self::Message, Self::Response>, Self::Error> {
        match call {
            Self::ApplicationCall::Mint {
                minter,
                name,
                payload,
            } => {
                self.check_account_authentication(minter)?;

                let execution_outcome = self.mint(minter, name, payload).await?;
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
                self.check_account_authentication(source_owner)?;

                let nft = self.get_nft(&token_id).await?;
                self.check_account_authentication(nft.owner)?;

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
                self.check_account_authentication(source_account.owner)?;

                let execution_outcome = if source_account.chain_id == self.runtime.chain_id() {
                    let nft = self.get_nft(&token_id).await?;
                    self.check_account_authentication(nft.owner)?;

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
    fn check_account_authentication(&mut self, owner: AccountOwner) -> Result<(), Error> {
        match owner {
            AccountOwner::User(address) => {
                ensure!(
                    self.runtime.authenticated_signer() == Some(address),
                    Error::IncorrectAuthentication
                )
            }
            AccountOwner::Application(id) => {
                ensure!(
                    self.runtime.authenticated_caller_id() == Some(id),
                    Error::IncorrectAuthentication
                )
            }
        }

        Ok(())
    }

    /// Transfers the specified NFT to another account.
    /// Authentication needs to have happened already.
    async fn transfer(
        &mut self,
        mut nft: Nft,
        target_account: Account,
    ) -> ExecutionOutcome<Message> {
        self.remove_nft(&nft).await;
        if target_account.chain_id == self.runtime.chain_id() {
            nft.owner = target_account.owner;
            self.add_nft(nft).await;
            ExecutionOutcome::default()
        } else {
            let message = Message::Transfer {
                nft,
                target_account,
            };

            self.runtime
                .send_message(target_account.chain_id, message)
                .with_tracking();
            ExecutionOutcome::default()
        }
    }

    async fn get_nft(&self, token_id: &TokenId) -> Result<Nft, Error> {
        self.state
            .nfts
            .get(token_id)
            .await
            .expect("Failure in retrieving NFT")
            .ok_or_else(|| Error::NftNotFound {
                token_id: token_id.clone(),
            })
    }

    async fn mint(
        &mut self,
        owner: AccountOwner,
        name: String,
        payload: Vec<u8>,
    ) -> Result<ExecutionOutcome<Message>, Error> {
        let token_id = Nft::create_token_id(
            &self.runtime.chain_id(),
            &self.runtime.application_id().forget_abi(),
            &name,
            &owner,
            &payload,
            *self.state.num_minted_nfts.get(),
        )?;

        self.add_nft(Nft {
            token_id,
            owner,
            name,
            minter: owner,
            payload,
        })
        .await;

        let num_minted_nfts = self.state.num_minted_nfts.get_mut();
        *num_minted_nfts += 1;
        Ok(ExecutionOutcome::default())
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
        self.runtime
            .send_message(source_account.chain_id, message)
            .with_authentication();
        ExecutionOutcome::default()
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

    #[error("NFT {token_id} not found")]
    NftNotFound { token_id: TokenId },
}
