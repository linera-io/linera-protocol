#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Amm;
use amm::{AmmError, ApplicationCall, Message, Operation, OperationType};
use async_trait::async_trait;
use fungible::{Account, AccountOwner, Destination, FungibleTokenAbi};
use linera_sdk::{
    base::{Amount, ApplicationId, SessionId, WithContractAbi},
    contract::system_api,
    ensure, ApplicationCallResult, CalleeContext, Contract, ExecutionResult, MessageContext,
    OperationContext, SessionCallResult, ViewStateStorage,
};

#[macro_use]
extern crate approx;

linera_sdk::contract!(Amm);

impl WithContractAbi for Amm {
    type Abi = amm::AmmAbi;
}

#[async_trait]
impl Contract for Amm {
    type Error = AmmError;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionResult<Self::Message>, AmmError> {
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: Self::Operation,
    ) -> Result<ExecutionResult<Self::Message>, AmmError> {
        let mut result = ExecutionResult::default();
        match operation {
            Operation::ExecuteOperation { operation } => {
                if context.chain_id == system_api::current_application_id().creation.chain_id {
                    self.execute_order_local(operation).await?;
                } else {
                    self.execute_order_remote(&mut result, operation).await?;
                }
            }
        }

        Ok(result)
    }

    async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: Self::Message,
    ) -> Result<ExecutionResult<Self::Message>, AmmError> {
        ensure!(
            context.chain_id == system_api::current_application_id().creation.chain_id,
            AmmError::AmmChainOnly
        );
        match message {
            Message::ExecuteOperation { operation } => {
                self.execute_order_local(operation).await?;
            }
        }
        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        argument: Self::ApplicationCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Message, Self::Response, Self::SessionState>, AmmError>
    {
        let mut result = ApplicationCallResult::default();
        match argument {
            ApplicationCall::ExecuteOperation { operation } => {
                if context.chain_id == system_api::current_application_id().creation.chain_id {
                    self.execute_order_local(operation).await?;
                } else {
                    self.execute_order_remote(&mut result.execution_result, operation)
                        .await?;
                }
            }
        }

        Ok(result)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _session: (),
        _argument: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult<Self::Message, Self::Response, Self::SessionState>, AmmError>
    {
        Err(AmmError::SessionsNotSupported)
    }
}

impl Amm {
    async fn execute_order_local(&mut self, operation: OperationType) -> Result<(), AmmError> {
        match operation {
            OperationType::Swap {
                owner,
                input_token_idx,
                output_token_idx,
                input_amount,
            } => {
                if input_amount == 0 {
                    return Err(AmmError::NoZeroAmounts);
                }

                if input_token_idx > 1 || output_token_idx > 1 {
                    return Err(AmmError::InvalidTokenIdx);
                }

                if input_token_idx == output_token_idx {
                    return Err(AmmError::EqualTokensError);
                }

                let input_pool_balance = self.get_pool_balance(input_token_idx).await?;
                let output_pool_balance = self.get_pool_balance(output_token_idx).await?;

                let output_amount = amm::calculate_output_amount(
                    input_amount,
                    input_pool_balance,
                    output_pool_balance,
                )?;

                self.receive_from_account(&owner, input_token_idx, input_amount)
                    .await?;
                self.send_to(&owner, output_token_idx, output_amount).await
            }
            OperationType::AddLiquidity {
                owner,
                token0_amount,
                token1_amount,
            } => {
                if token0_amount == 0 || token1_amount == 0 {
                    return Err(AmmError::NoZeroAmounts);
                }

                let balance0 = self.get_pool_balance(0).await?;
                let balance1 = self.get_pool_balance(1).await?;

                if balance1 > 0
                    && abs_diff_ne!(
                        balance0 as f64 / balance1 as f64,
                        token0_amount as f64 / token1_amount as f64,
                        epsilon = 0.01
                    )
                {
                    return Err(AmmError::BalanceRatioAlteredError);
                }

                self.receive_from_account(&owner, 0, token0_amount).await?;
                self.receive_from_account(&owner, 1, token1_amount).await?;

                Ok(())
            }
            // When removing liquidity, you'll specify one of the tokens you want to
            // remove and the amount, and we'll calculate the amount for the other token that
            // we'll remove based on the current ratio, and remove them.
            OperationType::RemoveLiquidity {
                owner,
                input_token_idx,
                other_token_idx,
                input_amount,
            } => {
                if input_token_idx > 1 || other_token_idx > 1 {
                    return Err(AmmError::InvalidTokenIdx);
                }

                if input_token_idx == other_token_idx {
                    return Err(AmmError::EqualTokensError);
                }

                let balance0 = self.get_pool_balance(0).await?;
                let balance1 = self.get_pool_balance(1).await?;

                if (input_token_idx == 0 && input_amount > balance0)
                    || (input_token_idx == 1 && input_amount > balance1)
                {
                    return Err(AmmError::InsufficientLiquidityError);
                }

                let other_amount_float = if input_token_idx == 0 {
                    (input_amount as f64) * ((balance1 as f64) / (balance0 as f64))
                } else {
                    (input_amount as f64) * ((balance0 as f64) / (balance1 as f64))
                };

                let other_amount = other_amount_float as u64;
                if (other_token_idx == 1 && other_amount > balance1)
                    || (other_token_idx == 0 && other_amount > balance0)
                {
                    return Err(AmmError::InsufficientLiquidityError);
                }

                self.send_to(&owner, input_token_idx, input_amount).await?;
                self.send_to(&owner, other_token_idx, other_amount).await?;
                Ok(())
            }
        }
    }

    async fn execute_order_remote(
        &mut self,
        result: &mut ExecutionResult<Message>,
        operation: OperationType,
    ) -> Result<(), AmmError> {
        let chain_id = system_api::current_application_id().creation.chain_id;
        let message = Message::ExecuteOperation {
            operation: operation.clone(),
        };
        result.messages.push((chain_id.into(), true, message));
        Ok(())
    }

    async fn get_pool_balance(&mut self, token_idx: u32) -> Result<u64, AmmError> {
        let pool_owner = AccountOwner::Application(system_api::current_application_id());
        self.balance(&pool_owner, token_idx).await
    }

    fn fungible_id(token_idx: u32) -> Result<ApplicationId<FungibleTokenAbi>, AmmError> {
        let parameter = Self::parameters()?;
        Ok(parameter.tokens[token_idx as usize])
    }

    async fn transfer(
        &mut self,
        owner: &AccountOwner,
        amount: u64,
        destination: Destination,
        token_idx: u32,
    ) -> Result<(), AmmError> {
        let transfer = fungible::ApplicationCall::Transfer {
            owner: *owner,
            amount: Amount::from_tokens(amount as u128),
            destination,
        };
        let token = Self::fungible_id(token_idx).expect("failed to get the token");
        self.call_application(true, token, &transfer, vec![])
            .await?;
        Ok(())
    }

    async fn balance(&mut self, owner: &AccountOwner, token_idx: u32) -> Result<u64, AmmError> {
        let balance = fungible::ApplicationCall::Balance { owner: *owner };
        let token = Self::fungible_id(token_idx).expect("failed to get the token");
        let amount = self
            .call_application(true, token, &balance, vec![])
            .await?
            .0;
        Ok(amount.saturating_div(Amount::ONE) as u64)
    }

    async fn receive_from_account(
        &mut self,
        owner: &AccountOwner,
        token_idx: u32,
        amount: u64,
    ) -> Result<(), AmmError> {
        let account = Account {
            chain_id: system_api::current_chain_id(),
            owner: AccountOwner::Application(system_api::current_application_id()),
        };
        let destination = Destination::Account(account);
        self.transfer(owner, amount, destination, token_idx).await
    }

    async fn send_to(
        &mut self,
        owner: &AccountOwner,
        token_idx: u32,
        amount: u64,
    ) -> Result<(), AmmError> {
        let account = Account {
            chain_id: system_api::current_chain_id(),
            owner: *owner,
        };
        let destination = Destination::Account(account);
        let owner_app = AccountOwner::Application(system_api::current_application_id());
        self.transfer(&owner_app, amount, destination, token_idx)
            .await
    }
}
