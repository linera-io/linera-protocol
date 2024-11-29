// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use amm::{AmmAbi, Message, Operation, Parameters};
use fungible::{Account, FungibleTokenAbi};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, ChainId, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use num_bigint::BigUint;
use num_traits::{cast::FromPrimitive, ToPrimitive};

use self::state::AmmState;

pub struct AmmContract {
    state: AmmState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(AmmContract);

impl WithContractAbi for AmmContract {
    type Abi = AmmAbi;
}

impl Contract for AmmContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = Parameters;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = AmmState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        AmmContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Self::Operation) -> Self::Response {
        if self.runtime.chain_id() == self.runtime.application_creator_chain_id() {
            self.execute_order_local(operation).await;
        } else {
            self.execute_order_remote(operation).await;
        }
    }

    async fn execute_message(&mut self, message: Self::Message) {
        assert_eq!(
            self.runtime.chain_id(),
            self.runtime.application_creator_chain_id(),
            "Action can only be executed on the chain that created the AMM"
        );

        match message {
            Message::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                self.check_account_authentication(owner);
                // It's assumed that the tokens have already been transferred here at this point
                assert!(
                    input_amount > Amount::ZERO,
                    "You can't add liquidity with zero tokens"
                );

                assert!(input_token_idx < 2, "Invalid token index");

                let output_token_idx = 1 - input_token_idx;
                let input_pool_balance = self.get_pool_balance(input_token_idx);
                let output_pool_balance = self.get_pool_balance(output_token_idx);

                let output_amount = self.calculate_output_amount(
                    input_amount,
                    input_pool_balance,
                    output_pool_balance,
                );

                let amm_account = self.get_amm_account();
                self.transfer(owner, input_amount, amm_account, input_token_idx);

                let amm_app_owner = self.get_amm_app_owner();
                let message_origin_account = self.get_message_origin_account(owner);
                self.transfer(
                    amm_app_owner,
                    output_amount,
                    message_origin_account,
                    output_token_idx,
                );
            }

            Message::AddLiquidity {
                owner,
                max_token0_amount,
                max_token1_amount,
            } => {
                self.check_account_authentication(owner);

                assert!(
                    max_token0_amount > Amount::ZERO && max_token1_amount > Amount::ZERO,
                    "You can't add liquidity with zero tokens"
                );

                let balance0 = self.get_pool_balance(0);
                let balance1 = self.get_pool_balance(1);

                let balance0_bigint = BigUint::from_u128(u128::from(balance0))
                    .expect("Couldn't generate balance0 in bigint");
                let balance1_bigint = BigUint::from_u128(u128::from(balance1))
                    .expect("Couldn't generate balance1 in bigint");

                let token0_amount;
                let token1_amount;
                if balance0 > Amount::ZERO && balance1 > Amount::ZERO {
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
                        token0_amount = Amount::from_attos(
                            token0_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert token0_amount_bigint to u128"),
                        );
                        token1_amount = Amount::from_attos(
                            max_token1_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert max_token1_amount_bigint to u128"),
                        );
                    } else {
                        let token1_amount_bigint =
                            (&balance1_bigint * &max_token0_amount_bigint) / &balance0_bigint;
                        token0_amount = Amount::from_attos(
                            max_token0_amount_bigint
                                .to_u128()
                                .expect("Couldn't convert max_token0_amount_bigint to u128"),
                        );
                        token1_amount = Amount::from_attos(
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

                let amm_account = self.get_amm_account();
                let message_origin_account = self.get_message_origin_account(owner);
                // See if we'll need to send refunds
                if token0_amount < max_token0_amount {
                    self.transfer(
                        owner,
                        max_token0_amount.saturating_sub(token0_amount),
                        message_origin_account,
                        0,
                    );
                }
                // Transfer tokens to AMM owner
                self.transfer(owner, token0_amount, amm_account, 0);

                // See if we'll need to send refunds
                if token1_amount < max_token1_amount {
                    self.transfer(
                        owner,
                        max_token1_amount.saturating_sub(token1_amount),
                        message_origin_account,
                        1,
                    );
                }
                // Transfer tokens to AMM owner
                self.transfer(owner, token1_amount, amm_account, 1);

                let shares_to_mint =
                    self.get_shares(token0_amount, token1_amount, &balance0_bigint);

                let mut current_shares = self
                    .current_shares_or_default(&message_origin_account)
                    .await;
                current_shares.saturating_add_assign(shares_to_mint);
                self.state
                    .shares
                    .insert(&message_origin_account, current_shares)
                    .expect("Failed insert statement");

                let total_shares_supply = self.state.total_shares_supply.get_mut();
                *total_shares_supply = total_shares_supply.saturating_add(shares_to_mint);
            }

            Message::RemoveLiquidity {
                owner,
                token_to_remove_idx,
                mut token_to_remove_amount,
            } => {
                self.check_account_authentication(owner);

                assert!(token_to_remove_idx < 2, "Invalid token index");

                let balance0 = self.get_pool_balance(0);
                let balance1 = self.get_pool_balance(1);

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
                    Amount::from_attos(
                        ((token_to_remove_amount_bigint * balance1_bigint.clone())
                            / balance0_bigint.clone())
                        .to_u128()
                        .expect("Couldn't convert other_amount to u128"),
                    )
                } else {
                    Amount::from_attos(
                        ((token_to_remove_amount_bigint * balance0_bigint.clone())
                            / balance1_bigint.clone())
                        .to_u128()
                        .expect("Couldn't convert other_amount to u128"),
                    )
                };

                let shares_to_return = if token_to_remove_idx == 0 {
                    self.get_shares(token_to_remove_amount, other_amount, &balance0_bigint)
                } else {
                    self.get_shares(other_amount, token_to_remove_amount, &balance0_bigint)
                };

                let message_origin_account = self.get_message_origin_account(owner);
                let current_shares = self
                    .current_shares_or_default(&message_origin_account)
                    .await;
                assert!(
                    shares_to_return <= current_shares,
                    "Can't remove more liquidity than you added"
                );
                self.return_shares(
                    message_origin_account,
                    current_shares,
                    shares_to_return,
                    token_to_remove_idx,
                    token_to_remove_amount,
                    other_amount,
                )
            }

            Message::RemoveAllAddedLiquidity { owner } => {
                self.check_account_authentication(owner);

                let message_origin_account = self.get_message_origin_account(owner);
                let current_shares = self
                    .current_shares_or_default(&message_origin_account)
                    .await;

                let (amount_token0, amount_token1) = self.get_amounts_from_shares(current_shares);
                self.return_shares(
                    message_origin_account,
                    current_shares,
                    current_shares,
                    0,
                    amount_token0,
                    amount_token1,
                );
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl AmmContract {
    /// authenticate the originator of the message
    fn check_account_authentication(&mut self, owner: AccountOwner) {
        match owner {
            AccountOwner::User(address) => {
                assert_eq!(
                    self.runtime.authenticated_signer(),
                    Some(address),
                    "Unauthorized"
                )
            }
            AccountOwner::Application(id) => {
                assert_eq!(
                    self.runtime.authenticated_caller_id(),
                    Some(id),
                    "Unauthorized"
                )
            }
        }
    }

    /// Obtains the current shares for an `account`.
    async fn current_shares_or_default(&self, account: &Account) -> Amount {
        self.state
            .shares
            .get(account)
            .await
            .expect("Failure in the retrieval")
            .unwrap_or_default()
    }

    fn return_shares(
        &mut self,
        account: Account,
        mut current_shares: Amount,
        shares_to_return: Amount,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
        other_token_to_remove_amount: Amount,
    ) {
        let amm_app_owner = self.get_amm_app_owner();
        self.transfer(
            amm_app_owner,
            token_to_remove_amount,
            account,
            token_to_remove_idx,
        );
        self.transfer(
            amm_app_owner,
            other_token_to_remove_amount,
            account,
            1 - token_to_remove_idx,
        );

        current_shares = current_shares.saturating_sub(shares_to_return);

        if current_shares == Amount::ZERO {
            self.state
                .shares
                .remove(&account)
                .expect("Failed remove statement");
        } else {
            self.state
                .shares
                .insert(&account, current_shares)
                .expect("Failed insert statement");
        }

        let total_shares_supply = self.state.total_shares_supply.get_mut();
        *total_shares_supply = total_shares_supply.saturating_sub(shares_to_return);
    }

    fn get_shares(
        &self,
        token0_amount: Amount,
        token1_amount: Amount,
        balance0_bigint: &BigUint,
    ) -> Amount {
        let token0_amount_bigint = BigUint::from_u128(u128::from(token0_amount))
            .expect("Converting token0_amount to BigUint should not fail!");
        let token1_amount_bigint = BigUint::from_u128(u128::from(token1_amount))
            .expect("Converting token1_amount to BigUint should not fail!");

        if *self.state.total_shares_supply.get() == Amount::ZERO {
            let tokens_mul_bigint = token0_amount_bigint * token1_amount_bigint;
            Amount::from_attos(
                BigUint::sqrt(&tokens_mul_bigint)
                    .to_u128()
                    .expect("Couldn't convert BigUint shares to u128"),
            )
        } else {
            let total_shares_supply_bigint =
                BigUint::from_u128(u128::from(*self.state.total_shares_supply.get()))
                    .expect("Converting total_shares_supply to BigUint should not fail!");
            Amount::from_attos(
                ((token0_amount_bigint * total_shares_supply_bigint.clone()) / balance0_bigint)
                    .to_u128()
                    .expect("Couldn't convert BigUint shares to u128"),
            )
        }
    }

    fn get_amounts_from_shares(&mut self, current_shares: Amount) -> (Amount, Amount) {
        let total_shares_supply = *self.state.total_shares_supply.get();
        let balance0 = self.get_pool_balance(0);
        let balance1 = self.get_pool_balance(1);

        let total_shares_supply_bigint = BigUint::from_u128(u128::from(total_shares_supply))
            .expect("Couldn't generate total_shares_supply in bigint");
        let current_shares_bigint = BigUint::from_u128(u128::from(current_shares))
            .expect("Couldn't generate current_shares in bigint");
        let balance0_bigint =
            BigUint::from_u128(u128::from(balance0)).expect("Couldn't generate balance0 in bigint");
        let balance1_bigint =
            BigUint::from_u128(u128::from(balance1)).expect("Couldn't generate balance1 in bigint");

        (
            Amount::from_attos(
                ((current_shares_bigint.clone() * balance0_bigint)
                    / total_shares_supply_bigint.clone())
                .to_u128()
                .expect("Couldn't convert amount_token0 to u128"),
            ),
            Amount::from_attos(
                ((current_shares_bigint * balance1_bigint) / total_shares_supply_bigint)
                    .to_u128()
                    .expect("Couldn't convert amount_token1 to u128"),
            ),
        )
    }

    fn get_amm_app_owner(&mut self) -> AccountOwner {
        AccountOwner::Application(self.runtime.application_id().forget_abi())
    }

    fn get_amm_chain_id(&mut self) -> ChainId {
        self.runtime.application_creator_chain_id()
    }

    fn get_amm_account(&mut self) -> Account {
        Account {
            chain_id: self.get_amm_chain_id(),
            owner: self.get_amm_app_owner(),
        }
    }

    fn get_message_creation_chain_id(&mut self) -> ChainId {
        self.runtime
            .message_id()
            .expect("Getting message id should not fail")
            .chain_id
    }

    fn get_message_origin_account(&mut self, owner: AccountOwner) -> Account {
        Account {
            chain_id: self.get_message_creation_chain_id(),
            owner,
        }
    }

    fn get_account_on_amm_chain(&mut self, owner: AccountOwner) -> Account {
        Account {
            chain_id: self.get_amm_chain_id(),
            owner,
        }
    }

    async fn execute_order_local(&mut self, operation: Operation) {
        match operation {
            Operation::Swap {
                owner: _,
                input_token_idx: _,
                input_amount: _,
            } => panic!("Can't swap locally"),

            Operation::AddLiquidity {
                owner: _,
                max_token0_amount: _,
                max_token1_amount: _,
            } => panic!("Can't add liquidity locally"),

            Operation::RemoveLiquidity {
                owner: _,
                token_to_remove_idx: _,
                token_to_remove_amount: _,
            } => panic!("Can't remove liquidity locally"),

            Operation::RemoveAllAddedLiquidity { owner: _ } => {
                panic!("Can't remove liquidity locally")
            }

            Operation::CloseChain => {
                let accounts = self
                    .state
                    .shares
                    .indices()
                    .await
                    .expect("Failed to load list of share owners");

                for account in accounts {
                    let current_shares = self.current_shares_or_default(&account).await;
                    let (amount_token0, amount_token1) =
                        self.get_amounts_from_shares(current_shares);

                    self.return_shares(
                        account,
                        current_shares,
                        current_shares,
                        0,
                        amount_token0,
                        amount_token1,
                    );
                }

                assert_eq!(
                    *self.state.total_shares_supply.get(),
                    Amount::ZERO,
                    "Untracked liquidity was found"
                );

                self.runtime
                    .close_chain()
                    .expect("Application is not authorized to close the chain");
            }
        }
    }

    async fn execute_order_remote(&mut self, operation: Operation) {
        match operation {
            Operation::Swap {
                owner,
                input_token_idx,
                input_amount,
            } => {
                self.check_account_authentication(owner);

                let account_on_amm_chain = self.get_account_on_amm_chain(owner);
                self.transfer(owner, input_amount, account_on_amm_chain, input_token_idx);

                let message = Message::Swap {
                    owner,
                    input_token_idx,
                    input_amount,
                };

                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(self.get_amm_chain_id());
            }

            Operation::AddLiquidity {
                owner,
                max_token0_amount,
                max_token1_amount,
            } => {
                self.check_account_authentication(owner);

                let account_on_amm_chain = self.get_account_on_amm_chain(owner);
                self.transfer(owner, max_token0_amount, account_on_amm_chain, 0);
                self.transfer(owner, max_token1_amount, account_on_amm_chain, 1);

                let message = Message::AddLiquidity {
                    owner,
                    max_token0_amount,
                    max_token1_amount,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(self.get_amm_chain_id());
            }

            // When removing liquidity, you'll specify one of the tokens you want to
            // remove and the amount, and we'll calculate the amount for the other token that
            // we'll remove based on the current ratio, and remove them.
            Operation::RemoveLiquidity {
                owner,
                token_to_remove_idx,
                token_to_remove_amount,
            } => {
                self.check_account_authentication(owner);

                let message = Message::RemoveLiquidity {
                    owner,
                    token_to_remove_idx,
                    token_to_remove_amount,
                };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(self.get_amm_chain_id());
            }

            Operation::RemoveAllAddedLiquidity { owner } => {
                self.check_account_authentication(owner);

                let message = Message::RemoveAllAddedLiquidity { owner };
                self.runtime
                    .prepare_message(message)
                    .with_authentication()
                    .send_to(self.get_amm_chain_id());
            }

            Operation::CloseChain => panic!("Can't close the chain remotely"),
        }
    }

    fn calculate_output_amount(
        &mut self,
        input_amount: Amount,
        input_pool_balance: Amount,
        output_pool_balance: Amount,
    ) -> Amount {
        assert!(
            input_pool_balance > Amount::ZERO && output_pool_balance > Amount::ZERO,
            "Invalid pool balance"
        );

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
        Amount::from_attos(
            output_amount_bigint
                .to_u128()
                .expect("Couldn't convert output_amount_bigint to u128"),
        )
    }

    fn get_pool_balance(&mut self, token_idx: u32) -> Amount {
        let pool_owner = AccountOwner::Application(self.runtime.application_id().forget_abi());
        self.balance(&pool_owner, token_idx)
    }

    fn fungible_id(&mut self, token_idx: u32) -> ApplicationId<FungibleTokenAbi> {
        self.runtime.application_parameters().tokens[token_idx as usize]
    }

    fn transfer(
        &mut self,
        source_owner: AccountOwner,
        amount: Amount,
        target_account: Account,
        token_idx: u32,
    ) {
        let token = self.fungible_id(token_idx);
        let operation = fungible::Operation::Transfer {
            owner: source_owner,
            amount,
            target_account,
        };

        self.runtime.call_application(true, token, &operation);
    }

    fn balance(&mut self, owner: &AccountOwner, token_idx: u32) -> Amount {
        let balance = fungible::Operation::Balance { owner: *owner };
        let token = self.fungible_id(token_idx);
        match self.runtime.call_application(true, token, &balance) {
            fungible::FungibleResponse::Balance(balance) => balance,
            response => panic!("Unexpected response from fungible token application: {response:?}"),
        }
    }
}
