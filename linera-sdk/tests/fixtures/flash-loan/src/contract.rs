// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use flash_loan::{FlashLoanAbi, FlashLoanInitialState, FlashLoanParameters, Operation};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    linera_base_types::{Account, AccountOwner, Amount, WithContractAbi},
    views::{linera_views, RegisterView, RootView, View, ViewStorageContext},
    Contract, ContractRuntime,
};

/// The flash-loan contract state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct FlashLoanState {
    /// Cumulative amount lent out via `get_cash`.
    pub total_borrowed: RegisterView<Amount>,
    /// Cumulative amount received back via `repay_loan`.
    pub total_repaid: RegisterView<Amount>,
}

pub struct FlashLoanContract {
    state: FlashLoanState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(FlashLoanContract);

impl WithContractAbi for FlashLoanContract {
    type Abi = FlashLoanAbi;
}

impl Contract for FlashLoanContract {
    type Message = ();
    type Parameters = FlashLoanParameters;
    type InstantiationArgument = FlashLoanInitialState;
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = FlashLoanState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        FlashLoanContract { state, runtime }
    }

    async fn instantiate(&mut self, _initial: FlashLoanInitialState) {
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) {
        match operation {
            Operation::GetCash { amount } => self.get_cash(amount),
            Operation::RepayLoan { amount } => self.repay_loan(amount),
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Flash-loan application doesn't support cross-chain messages");
    }

    async fn save(&mut self) {
        self.state.save().await.expect("Failed to save state");
    }

    /// Validate that all loans have been repaid with sufficient interest.
    ///
    /// This runs at the end of the block. Cross-application calls are not allowed here,
    /// so we check the accounting tracked in our own state. The actual token transfers
    /// were performed via the fungible token during `execute_operation`.
    async fn terminate(mut self) {
        let total_borrowed = *self.state.total_borrowed.get();
        let total_repaid = *self.state.total_repaid.get();
        if total_borrowed == Amount::ZERO {
            return;
        }
        let params = self.runtime.application_parameters();
        let interest_millionths = params.interest_millionths as u128;
        let borrowed_attos = u128::from(total_borrowed);
        let min_interest = Amount::from_attos(borrowed_attos * interest_millionths / 1_000_000);
        let min_repayment = total_borrowed.saturating_add(min_interest);
        assert!(
            total_repaid >= min_repayment,
            "Flash loan terminate: insufficient repayment. \
             Repaid: {total_repaid}, required: {min_repayment} \
             (borrowed: {total_borrowed}, min interest: {min_interest})"
        );
    }
}

impl FlashLoanContract {
    fn params(&mut self) -> FlashLoanParameters {
        self.runtime.application_parameters()
    }

    /// Lend `amount` tokens to the caller via the fungible token.
    fn get_cash(&mut self, amount: Amount) {
        assert!(amount > Amount::ZERO, "Cannot borrow zero");
        let params = self.params();
        let my_account = AccountOwner::from(self.runtime.application_id());
        let caller = self
            .runtime
            .authenticated_owner()
            .expect("get_cash requires an authenticated owner");
        let target_account = Account {
            chain_id: self.runtime.chain_id(),
            owner: caller,
        };
        let transfer = FungibleOperation::Transfer {
            owner: my_account,
            amount,
            target_account,
        };
        self.runtime
            .call_application(true, params.fungible_app_id, &transfer);
        let total = *self.state.total_borrowed.get();
        self.state.total_borrowed.set(total.saturating_add(amount));
    }

    /// Accept repayment of `amount` tokens from the caller via the fungible token.
    fn repay_loan(&mut self, amount: Amount) {
        assert!(amount > Amount::ZERO, "Cannot repay zero");
        let params = self.params();
        let caller = self
            .runtime
            .authenticated_owner()
            .expect("repay_loan requires an authenticated owner");
        let my_account = AccountOwner::from(self.runtime.application_id());
        let target_account = Account {
            chain_id: self.runtime.chain_id(),
            owner: my_account,
        };
        let transfer = FungibleOperation::Transfer {
            owner: caller,
            amount,
            target_account,
        };
        self.runtime
            .call_application(true, params.fungible_app_id, &transfer);
        let total = *self.state.total_repaid.get();
        self.state.total_repaid.set(total.saturating_add(amount));
    }
}
