// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use crowd_funding::{CrowdFundingAbi, InstantiationArgument, Message, Operation};
use fungible::{Account, FungibleResponse, FungibleTokenAbi};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, WithContractAbi},
    ensure,
    views::View,
    Contract, ContractRuntime, StoreOnDrop,
};
use state::{CrowdFunding, Status};
use thiserror::Error;

pub struct CrowdFundingContract {
    state: StoreOnDrop<CrowdFunding>,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CrowdFundingContract);

impl WithContractAbi for CrowdFundingContract {
    type Abi = CrowdFundingAbi;
}

impl Contract for CrowdFundingContract {
    type Error = Error;
    type State = CrowdFunding;
    type Message = Message;
    type InstantiationArgument = InstantiationArgument;
    type Parameters = ApplicationId<fungible::FungibleTokenAbi>;

    async fn new(state: CrowdFunding, runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(CrowdFundingContract {
            state: StoreOnDrop(state),
            runtime,
        })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(&mut self, argument: InstantiationArgument) -> Result<(), Self::Error> {
        // Validate that the application parameters were configured correctly.
        let _ = self.runtime.application_parameters();

        self.state.instantiation_argument.set(Some(argument));

        let deadline = self.instantiation_argument().deadline;
        ensure!(
            deadline > self.runtime.system_time(),
            Error::DeadlineInThePast
        );

        Ok(())
    }

    async fn execute_operation(&mut self, operation: Operation) -> Result<(), Self::Error> {
        match operation {
            Operation::Pledge { owner, amount } => {
                if self.runtime.chain_id() == self.runtime.application_id().creation.chain_id {
                    self.execute_pledge_with_account(owner, amount).await?;
                } else {
                    self.execute_pledge_with_transfer(owner, amount)?;
                }
            }
            Operation::Collect => self.collect_pledges()?,
            Operation::Cancel => self.cancel_campaign().await?,
        }

        Ok(())
    }

    async fn execute_message(&mut self, message: Message) -> Result<(), Self::Error> {
        match message {
            Message::PledgeWithAccount { owner, amount } => {
                ensure!(
                    self.runtime.chain_id() == self.runtime.application_id().creation.chain_id,
                    Error::CampaignChainOnly
                );
                self.execute_pledge_with_account(owner, amount).await?;
            }
        }
        Ok(())
    }
}

impl CrowdFundingContract {
    fn fungible_id(&mut self) -> ApplicationId<FungibleTokenAbi> {
        // TODO(#723): We should be able to pull the fungible ID from the
        // `required_application_ids` of the application description.
        self.runtime.application_parameters()
    }

    /// Adds a pledge from a local account to the remote campaign chain.
    fn execute_pledge_with_transfer(
        &mut self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<(), Error> {
        ensure!(amount > Amount::ZERO, Error::EmptyPledge);
        // The campaign chain.
        let chain_id = self.runtime.application_id().creation.chain_id;
        // First, move the funds to the campaign chain (under the same owner).
        // TODO(#589): Simplify this when the messaging system guarantees atomic delivery
        // of all messages created in the same operation/message.
        let target_account = Account { chain_id, owner };
        let call = fungible::Operation::Transfer {
            owner,
            amount,
            target_account,
        };
        let fungible_id = self.fungible_id();
        self.runtime
            .call_application(/* authenticated by owner */ true, fungible_id, &call);
        // Second, schedule the attribution of the funds to the (remote) campaign.
        self.runtime
            .prepare_message(Message::PledgeWithAccount { owner, amount })
            .with_authentication()
            .send_to(chain_id);
        Ok(())
    }

    /// Adds a pledge from a local account to the campaign chain.
    async fn execute_pledge_with_account(
        &mut self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Result<(), Error> {
        ensure!(amount > Amount::ZERO, Error::EmptyPledge);
        self.receive_from_account(owner, amount);
        self.finish_pledge(owner, amount).await
    }

    /// Marks a pledge in the application state, so that it can be returned if the campaign is
    /// cancelled.
    async fn finish_pledge(&mut self, source: AccountOwner, amount: Amount) -> Result<(), Error> {
        match self.state.status.get() {
            Status::Active => {
                self.state
                    .pledges
                    .get_mut_or_default(&source)
                    .await
                    .expect("view access should not fail")
                    .saturating_add_assign(amount);
                Ok(())
            }
            Status::Complete => {
                self.send_to(amount, self.instantiation_argument().owner);
                Ok(())
            }
            Status::Cancelled => Err(Error::Cancelled),
        }
    }

    /// Collects all pledges and completes the campaign if the target has been reached.
    fn collect_pledges(&mut self) -> Result<(), Error> {
        let total = self.balance()?;

        match self.state.status.get() {
            Status::Active => {
                ensure!(
                    total >= self.instantiation_argument().target,
                    Error::TargetNotReached
                );
            }
            Status::Complete => (),
            Status::Cancelled => return Err(Error::Cancelled),
        }

        self.send_to(total, self.instantiation_argument().owner);
        self.state.pledges.clear();
        self.state.status.set(Status::Complete);

        Ok(())
    }

    /// Cancels the campaign if the deadline has passed, refunding all pledges.
    async fn cancel_campaign(&mut self) -> Result<(), Error> {
        ensure!(!self.state.status.get().is_complete(), Error::Completed);

        // TODO(#728): Remove this.
        #[cfg(not(any(test, feature = "test")))]
        ensure!(
            self.runtime.system_time() >= self.instantiation_argument().deadline,
            Error::DeadlineNotReached
        );

        let mut pledges = Vec::new();
        self.state
            .pledges
            .for_each_index_value(|pledger, amount| {
                pledges.push((pledger, amount));
                Ok(())
            })
            .await
            .expect("view iteration should not fail");
        for (pledger, amount) in pledges {
            self.send_to(amount, pledger);
        }

        let balance = self.balance()?;
        self.send_to(balance, self.instantiation_argument().owner);
        self.state.status.set(Status::Cancelled);

        Ok(())
    }

    /// Queries the token application to determine the total amount of tokens in custody.
    fn balance(&mut self) -> Result<Amount, Error> {
        let owner = AccountOwner::Application(self.runtime.application_id().forget_abi());
        let fungible_id = self.fungible_id();
        let response = self.runtime.call_application(
            true,
            fungible_id,
            &fungible::Operation::Balance { owner },
        );
        match response {
            fungible::FungibleResponse::Balance(balance) => Ok(balance),
            response => Err(Error::UnexpectedFungibleResponse(response)),
        }
    }

    /// Transfers `amount` tokens from the funds in custody to the `owner`'s account.
    fn send_to(&mut self, amount: Amount, owner: AccountOwner) {
        let target_account = Account {
            chain_id: self.runtime.chain_id(),
            owner,
        };
        let transfer = fungible::Operation::Transfer {
            owner: AccountOwner::Application(self.runtime.application_id().forget_abi()),
            amount,
            target_account,
        };
        let fungible_id = self.fungible_id();
        self.runtime.call_application(true, fungible_id, &transfer);
    }

    /// Calls into the Fungible Token application to receive tokens from the given account.
    fn receive_from_account(&mut self, owner: AccountOwner, amount: Amount) {
        let target_account = Account {
            chain_id: self.runtime.chain_id(),
            owner: AccountOwner::Application(self.runtime.application_id().forget_abi()),
        };
        let transfer = fungible::Operation::Transfer {
            owner,
            amount,
            target_account,
        };
        let fungible_id = self.fungible_id();
        self.runtime.call_application(true, fungible_id, &transfer);
    }

    pub fn instantiation_argument(&self) -> &InstantiationArgument {
        self.state
            .instantiation_argument
            .get()
            .as_ref()
            .expect("Application is not running on the host chain or was not instantiated yet")
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Action can only be executed on the chain that created the crowd-funding campaign
    #[error("Action can only be executed on the chain that created the crowd-funding campaign")]
    CampaignChainOnly,

    /// Crowd-funding campaign cannot start after its deadline.
    #[error("Crowd-funding campaign cannot start after its deadline")]
    DeadlineInThePast,

    /// A pledge can not be empty.
    #[error("Pledge is empty")]
    EmptyPledge,

    /// Pledge used a token that's not the same as the one in the campaign's [`InstantiationArgument`].
    #[error("Pledge uses the incorrect token")]
    IncorrectToken,

    /// Cross-application call without a source application ID.
    #[error("Applications must identify themselves to perform transfers")]
    MissingSourceApplication,

    /// Can't collect pledges before the campaign target has been reached.
    #[error("Crowd-funding campaign has not reached its target yet")]
    TargetNotReached,

    /// Can't cancel a campaign before its deadline.
    #[error("Crowd-funding campaign has not reached its deadline yet")]
    DeadlineNotReached,

    /// Can't cancel a campaign after it has been completed.
    #[error("Crowd-funding campaign has already been completed")]
    Completed,

    /// Can't pledge to or collect pledges from a cancelled campaign.
    #[error("Crowd-funding campaign has been cancelled")]
    Cancelled,

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),

    /// Unexpected response from fungible token application.
    #[error("Unexpected response from fungible token application: {0:?}")]
    UnexpectedFungibleResponse(FungibleResponse),
}
