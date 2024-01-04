// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Amm;
use amm::{AmmError, ApplicationCall, Message, Operation};
use async_trait::async_trait;
use fungible::{Account, AccountOwner, Destination, FungibleTokenAbi};
use linera_sdk::{
    base::{Amount, ApplicationId, Owner, SessionId, WithContractAbi},
    contract::system_api,
    ensure, ApplicationCallResult, CalleeContext, Contract, ExecutionResult, MessageContext,
    OperationContext, OutgoingMessage, SessionCallResult, ViewStateStorage,
};
use num_bigint::BigUint;
use num_traits::{cast::FromPrimitive, ToPrimitive};

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
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: Self::Operation,
    ) -> Result<ExecutionResult<Self::Message>, AmmError> {
        let mut result = ExecutionResult::default();
        if context.chain_id == system_api::current_application_id().creation.chain_id {
            self.execute_order_local(operation).await?;
        } else {
            self.execute_order_remote(&mut result, operation).await?;
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
            Message::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                Self::check_account_authentication(None, context.authenticated_signer, owner)?;
                self.execute_swap(owner, input_token_idx, input_amount)
                    .await?;
            }
        }

        Ok(ExecutionResult::default())
    }

    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        application_call: ApplicationCall,
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult<Self::Message, Self::Response, Self::SessionState>, AmmError>
    {
        let mut result = ApplicationCallResult::default();
        match application_call {
            ApplicationCall::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                Self::check_account_authentication(
                    context.authenticated_caller_id,
                    context.authenticated_signer,
                    owner,
                )?;
                if context.chain_id == system_api::current_application_id().creation.chain_id {
                    self.execute_swap(owner, input_token_idx, input_amount)
                        .await?;
                } else {
                    self.execute_application_call_remote(
                        &mut result.execution_result,
                        application_call,
                    )
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
    /// authenticate the originator of the message
    fn check_account_authentication(
        authenticated_application_id: Option<ApplicationId>,
        authenticated_signer: Option<Owner>,
        owner: AccountOwner,
    ) -> Result<(), AmmError> {
        match owner {
            AccountOwner::User(address) if authenticated_signer == Some(address) => Ok(()),
            AccountOwner::Application(id) if authenticated_application_id == Some(id) => Ok(()),
            _ => Err(AmmError::IncorrectAuthentication),
        }
    }

    async fn execute_order_local(&mut self, operation: Operation) -> Result<(), AmmError> {
        match operation {
            Operation::Swap {
                owner: _,
                input_token_idx: _,
                input_amount: _,
            } => Err(AmmError::SwappingLocally),
            Operation::AddLiquidity {
                owner,
                max_token0_amount,
                max_token1_amount,
            } => {
                if max_token0_amount == Amount::ZERO || max_token1_amount == Amount::ZERO {
                    return Err(AmmError::NoZeroAmounts);
                }

                let balance0 = self.get_pool_balance(0).await?;
                let balance1 = self.get_pool_balance(1).await?;

                let token0_amount;
                let token1_amount;
                if balance0 > Amount::ZERO && balance1 > Amount::ZERO {
                    let balance0_bigint = BigUint::from_u128(u128::from(balance0))
                        .expect("Couldn't generate balance0 in bigint");
                    let balance1_bigint = BigUint::from_u128(u128::from(balance1))
                        .expect("Couldn't generate balance1 in bigint");
                    let max_token0_amount_bigint =
                        BigUint::from_u128(u128::from(max_token0_amount))
                            .expect("Couldn't generate max_token0_amount in bigint");
                    let max_token1_amount_bigint =
                        BigUint::from_u128(u128::from(max_token1_amount))
                            .expect("Couldn't generate max_token1_amount in bigint");

                    // This is the formula to maintain the ratio:
                    //      balance0 / balance1 = (balance0 + max_token0_amount) / (balance1 + token1_amount)
                    //      balance0 * (balance1 + token1_amount) = balance1 * (balance0 + max_token0_amount)
                    //      balance0 * balance1 + balance0 * token1_amount = balance1 * balance0 + balance1 * max_token0_amount
                    //      balance0 * token1_amount = balance1 * max_token0_amount
                    //      token1_amount = (balance1 * max_token0_amount) / balance0
                    //
                    // For token0_amount, it would be this:
                    //      token0_amount = (balance0 * max_token1_amount) / balance1

                    if &max_token0_amount_bigint * &balance1_bigint
                        > &max_token1_amount_bigint * &balance0_bigint
                    {
                        let token0_amount_bigint =
                            (&balance0_bigint * &max_token1_amount_bigint) / &balance1_bigint;
                        token0_amount = Amount::from_atto(
                            token0_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert token0_amount_bigint to u128"),
                        );
                        token1_amount = Amount::from_atto(
                            max_token1_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert max_token1_amount_bigint to u128"),
                        );
                    } else {
                        let token1_amount_bigint =
                            (&balance1_bigint * &max_token0_amount_bigint) / &balance0_bigint;
                        token0_amount = Amount::from_atto(
                            max_token0_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert max_token0_amount_bigint to u128"),
                        );
                        token1_amount = Amount::from_atto(
                            token1_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert token1_amount_bigint to u128"),
                        );
                    }
                } else {
                    // This means we're on the first liquidity addition
                    token0_amount = max_token0_amount;
                    token1_amount = max_token1_amount;
                }

                self.receive_from_account(&owner, 0, token0_amount).await?;
                self.receive_from_account(&owner, 1, token1_amount).await?;

                Ok(())
            }
            // When removing liquidity, you'll specify one of the tokens you want to
            // remove and the amount, and we'll calculate the amount for the other token that
            // we'll remove based on the current ratio, and remove them.
            Operation::RemoveLiquidity {
                owner,
                token_to_remove_idx,
                mut token_to_remove_amount,
            } => {
                if token_to_remove_idx > 1 {
                    return Err(AmmError::InvalidTokenIdx);
                }

                let other_token_to_remove_idx = 1 - token_to_remove_idx;
                let balance0 = self.get_pool_balance(0).await?;
                let balance1 = self.get_pool_balance(1).await?;

                if token_to_remove_idx == 0 && token_to_remove_amount > balance0 {
                    token_to_remove_amount = balance0;
                } else if token_to_remove_idx == 1 && token_to_remove_amount > balance1 {
                    token_to_remove_amount = balance1;
                }

                let token_to_remove_amount_bigint =
                    BigUint::from_u128(u128::from(token_to_remove_amount))
                        .expect("Couldn't generate token_to_remove_amount in bigint");

                let balance0_bigint = BigUint::from_u128(u128::from(balance0))
                    .expect("Couldn't generate balance0 in bigint");
                let balance1_bigint = BigUint::from_u128(u128::from(balance1))
                    .expect("Couldn't generate balance1 in bigint");

                let other_amount = if token_to_remove_idx == 0 {
                    Amount::from_atto(
                        ((token_to_remove_amount_bigint * balance1_bigint) / balance0_bigint)
                            .to_u128()
                            .expect("Couldn't convert other_amount to u128"),
                    )
                } else {
                    Amount::from_atto(
                        ((token_to_remove_amount_bigint * balance0_bigint) / balance1_bigint)
                            .to_u128()
                            .expect("Couldn't convert other_amount to u128"),
                    )
                };

                self.send_to(&owner, token_to_remove_idx, token_to_remove_amount)
                    .await?;
                self.send_to(&owner, other_token_to_remove_idx, other_amount)
                    .await?;
                Ok(())
            }
        }
    }

    async fn execute_swap(
        &mut self,
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    ) -> Result<(), AmmError> {
        if input_amount == Amount::ZERO {
            return Err(AmmError::NoZeroAmounts);
        }

        if input_token_idx > 1 {
            return Err(AmmError::InvalidTokenIdx);
        }

        let output_token_idx = 1 - input_token_idx;
        let input_pool_balance = self.get_pool_balance(input_token_idx).await?;
        let output_pool_balance = self.get_pool_balance(output_token_idx).await?;

        let output_amount =
            self.calculate_output_amount(input_amount, input_pool_balance, output_pool_balance)?;

        self.receive_from_account(&owner, input_token_idx, input_amount)
            .await?;
        self.send_to(&owner, output_token_idx, output_amount)
            .await?;

        Ok(())
    }

    async fn execute_order_remote(
        &mut self,
        result: &mut ExecutionResult<Message>,
        operation: Operation,
    ) -> Result<(), AmmError> {
        match operation {
            Operation::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                let chain_id = system_api::current_application_id().creation.chain_id;
                let message = Message::Swap {
                    owner,
                    input_token_idx,
                    input_amount,
                };
                result.messages.push(OutgoingMessage {
                    destination: chain_id.into(),
                    authenticated: true,
                    is_tracked: false,
                    message,
                });
            }
            Operation::AddLiquidity {
                owner: _,
                max_token0_amount: _,
                max_token1_amount: _,
            } => {
                return Err(AmmError::AddingLiquidityFromRemoteChain);
            }
            Operation::RemoveLiquidity {
                owner: _,
                token_to_remove_idx: _,
                token_to_remove_amount: _,
            } => {
                return Err(AmmError::RemovingLiquidityFromRemoteChain);
            }
        }

        Ok(())
    }

    async fn execute_application_call_remote(
        &mut self,
        result: &mut ExecutionResult<Message>,
        application_call: ApplicationCall,
    ) -> Result<(), AmmError> {
        match application_call {
            ApplicationCall::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                let chain_id = system_api::current_application_id().creation.chain_id;
                let message = Message::Swap {
                    owner,
                    input_token_idx,
                    input_amount,
                };
                result.messages.push(OutgoingMessage {
                    destination: chain_id.into(),
                    authenticated: true,
                    is_tracked: false,
                    message,
                });
            }
        }

        Ok(())
    }

    fn calculate_output_amount(
        &mut self,
        input_amount: Amount,
        input_pool_balance: Amount,
        output_pool_balance: Amount,
    ) -> Result<Amount, AmmError> {
        if input_pool_balance == Amount::ZERO || output_pool_balance == Amount::ZERO {
            return Err(AmmError::InvalidPoolBalanceError);
        }

        let input_amount_bigint = BigUint::from_u128(u128::from(input_amount))
            .expect("Couldn't generate input_amount in bigint");
        let output_pool_balance_bigint = BigUint::from_u128(u128::from(output_pool_balance))
            .expect("Couldn't generate output_pool_balance in bigint");
        let input_pool_balance_bigint = BigUint::from_u128(u128::from(input_pool_balance))
            .expect("Couldn't generate input_pool_balance in bigint");

        // Logic for this is the following:
        // This is a Constant Product Automated Market Maker, or CPAMM, so we want
        // the product to remain constant.
        // That means that this is the equation we need to solve to find output_amount:
        //      (input_pool_balance + input_amount) * (output_pool_balance - output_amount) = input_pool_balance * output_pool_balance
        //      output_pool_balance - output_amount = (input_pool_balance * output_pool_balance) / (input_pool_balance + input_amount)
        //      output_amount = output_pool_balance - (input_pool_balance * output_pool_balance) / (input_pool_balance + input_amount)
        //      output_amount = (output_pool_balance * (input_pool_balance + input_amount) - (input_pool_balance * output_pool_balance)) / (input_pool_balance + input_amount)
        //      output_amount = (input_pool_balance * output_pool_balance + input_amount * output_pool_balance - input_pool_balance * output_pool_balance) / (input_pool_balance + input_amount)
        //      output_amount = (input_amount * output_pool_balance) / (input_pool_balance + input_amount)

        // Numerator will be a number with 36 decimal points here
        let numerator_bigint = &input_amount_bigint * output_pool_balance_bigint;
        // Denominator will have 18 decimal points
        let denominator_bigint = input_pool_balance_bigint + input_amount_bigint;

        // Dividing 36 decimal points with 18 decimal points = 18 decimal points
        let output_amount_bigint = numerator_bigint / denominator_bigint;
        let output_amount = Amount::from_atto(
            output_amount_bigint
                .to_u128()
                .expect("Couldn't convert output_amount_bigint to u128"),
        );
        Ok(output_amount)
    }

    async fn get_pool_balance(&mut self, token_idx: u32) -> Result<Amount, AmmError> {
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
        amount: Amount,
        destination: Destination,
        token_idx: u32,
    ) -> Result<(), AmmError> {
        let transfer = fungible::ApplicationCall::Transfer {
            owner: *owner,
            amount,
            destination,
        };
        let token = Self::fungible_id(token_idx).expect("failed to get the token");
        self.call_application(true, token, &transfer, vec![])?;
        Ok(())
    }

    async fn balance(&mut self, owner: &AccountOwner, token_idx: u32) -> Result<Amount, AmmError> {
        let balance = fungible::ApplicationCall::Balance { owner: *owner };
        let token = Self::fungible_id(token_idx).expect("failed to get the token");
        match self.call_application(true, token, &balance, vec![])?.0 {
            fungible::FungibleResponse::Balance(balance) => Ok(balance),
            response => Err(AmmError::UnexpectedFungibleResponse(response)),
        }
    }

    async fn receive_from_account(
        &mut self,
        owner: &AccountOwner,
        token_idx: u32,
        amount: Amount,
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
        amount: Amount,
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
