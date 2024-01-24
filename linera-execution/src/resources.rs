// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module tracks the resources used during the execution of a transaction.

use crate::{
    policy::ResourceControlPolicy, system::SystemExecutionError, ExecutionError,
    ExecutionStateView, Message, Operation,
};
use custom_debug_derive::Debug;
use futures::FutureExt;
use linera_base::{
    data_types::{Amount, ArithmeticError},
    identifiers::Owner,
};
use linera_views::{common::Context, views::ViewError};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct ResourceController<Account = Amount, Tracker = ResourceTracker> {
    /// The (fixed) policy used to charge fees and control resource usage.
    pub policy: Arc<ResourceControlPolicy>,
    /// How the resources were used so far.
    pub tracker: Tracker,
    /// The account paying for the resource usage.
    pub account: Account,
}

/// The resources used so far by an execution process.
#[derive(Copy, Debug, Clone, Default)]
pub struct ResourceTracker {
    /// The number of blocks created.
    pub blocks: u32,
    /// The fuel used so far.
    pub fuel: u64,
    /// The number of read operations.
    pub read_operations: u32,
    /// The number of write operations.
    pub write_operations: u32,
    /// The number of bytes read.
    pub bytes_read: u64,
    /// The number of bytes written.
    pub bytes_written: u64,
    /// The change in the number of bytes being stored by user applications.
    pub bytes_stored: i32,
    /// The number of operations executed.
    pub operations: u32,
    /// The total size of the arguments of user operations.
    pub operation_bytes: u64,
    /// The number of outgoing messages created (system and user).
    pub messages: u32,
    /// The total size of the arguments of outgoing user messages.
    pub message_bytes: u64,
}

/// How to access the balance of an account.
pub trait BalanceHolder {
    fn as_amount(&self) -> Amount;

    fn try_add_assign(&mut self, other: Amount) -> Result<(), ArithmeticError>;

    fn try_sub_assign(&mut self, other: Amount) -> Result<(), ArithmeticError>;
}

// The main accounting functions for a ResourceController.
impl<Account, Tracker> ResourceController<Account, Tracker>
where
    Account: BalanceHolder,
    Tracker: AsMut<ResourceTracker>,
{
    /// Obtains the balance of the account.
    pub fn balance(&self) -> Amount {
        self.account.as_amount()
    }

    /// Operates a 3-way merge by transferring the difference between `initial`
    /// and `other` to `self`.
    pub fn merge_balance(&mut self, initial: Amount, other: Amount) -> Result<(), ExecutionError> {
        if other <= initial {
            self.account
                .try_sub_assign(initial.try_sub(other).expect("other <= initial"))
                .map_err(|_| SystemExecutionError::InsufficientFunding {
                    current_balance: self.balance(),
                })?;
        } else {
            self.account
                .try_add_assign(other.try_sub(initial).expect("other > initial"))?;
        }
        Ok(())
    }

    /// Subtracts an amount from a balance and reports an error if that is impossible.
    fn update_balance(&mut self, fees: Amount) -> Result<(), ExecutionError> {
        self.account.try_sub_assign(fees).map_err(|_| {
            SystemExecutionError::InsufficientFunding {
                current_balance: self.balance(),
            }
        })?;
        Ok(())
    }

    /// Obtains the amount of fuel that could be spent by consuming the entire balance.
    pub(crate) fn remaining_fuel(&self) -> u64 {
        self.policy.remaining_fuel(self.balance())
    }

    /// Tracks the creation of a block.
    pub fn track_block(&mut self) -> Result<(), ExecutionError> {
        self.tracker.as_mut().blocks = self
            .tracker
            .as_mut()
            .blocks
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.block)
    }

    /// Tracks the execution of an operation in block.
    pub fn track_operation(&mut self, operation: &Operation) -> Result<(), ExecutionError> {
        self.tracker.as_mut().operations = self
            .tracker
            .as_mut()
            .operations
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.operation)?;
        match operation {
            Operation::System(_) => Ok(()),
            Operation::User { bytes, .. } => {
                let size = bytes.len();
                self.tracker.as_mut().operation_bytes = self
                    .tracker
                    .as_mut()
                    .operation_bytes
                    .checked_add(size as u64)
                    .ok_or(ArithmeticError::Overflow)?;
                self.update_balance(self.policy.operation_byte_price(size as u64)?)?;
                Ok(())
            }
        }
    }

    /// Tracks the creation of an outgoing message.
    pub fn track_message(&mut self, message: &Message) -> Result<(), ExecutionError> {
        self.tracker.as_mut().messages = self
            .tracker
            .as_mut()
            .messages
            .checked_add(1)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.message)?;
        match message {
            Message::System(_) => Ok(()),
            Message::User { bytes, .. } => {
                let size = bytes.len();
                self.tracker.as_mut().message_bytes = self
                    .tracker
                    .as_mut()
                    .message_bytes
                    .checked_add(size as u64)
                    .ok_or(ArithmeticError::Overflow)?;
                self.update_balance(self.policy.message_byte_price(size as u64)?)?;
                Ok(())
            }
        }
    }

    /// Tracks a number of fuel units used.
    pub(crate) fn track_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError> {
        self.tracker.as_mut().fuel = self
            .tracker
            .as_mut()
            .fuel
            .checked_add(fuel)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.fuel_price(fuel)?)
    }

    /// Tracks a read operation.
    pub(crate) fn track_read_operations(&mut self, count: u32) -> Result<(), ExecutionError> {
        self.tracker.as_mut().read_operations = self
            .tracker
            .as_mut()
            .read_operations
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.read_operations_price(count)?)
    }

    /// Tracks a write operation.
    pub(crate) fn track_write_operations(&mut self, count: u32) -> Result<(), ExecutionError> {
        self.tracker.as_mut().write_operations = self
            .tracker
            .as_mut()
            .write_operations
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.write_operations_price(count)?)
    }

    /// Tracks a number of bytes read.
    pub(crate) fn track_bytes_read(&mut self, count: u64) -> Result<(), ExecutionError> {
        self.tracker.as_mut().bytes_read = self
            .tracker
            .as_mut()
            .bytes_read
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        if self.tracker.as_mut().bytes_read >= self.policy.maximum_bytes_read_per_block {
            return Err(ExecutionError::ExcessiveRead);
        }
        self.update_balance(self.policy.bytes_read_price(count)?)?;
        Ok(())
    }

    /// Tracks a number of bytes written.
    pub(crate) fn track_bytes_written(&mut self, count: u64) -> Result<(), ExecutionError> {
        self.tracker.as_mut().bytes_written = self
            .tracker
            .as_mut()
            .bytes_written
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        if self.tracker.as_mut().bytes_written >= self.policy.maximum_bytes_written_per_block {
            return Err(ExecutionError::ExcessiveWrite);
        }
        self.update_balance(self.policy.bytes_written_price(count)?)?;
        Ok(())
    }

    /// Tracks a change in the number of bytes stored.
    // TODO(#1536): This is not fully implemented.
    #[allow(dead_code)]
    pub(crate) fn track_stored_bytes(&mut self, delta: i32) -> Result<(), ExecutionError> {
        self.tracker.as_mut().bytes_stored = self
            .tracker
            .as_mut()
            .bytes_stored
            .checked_add(delta)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }
}

// The simplest `BalanceHolder` is an `Amount`.
impl BalanceHolder for Amount {
    fn as_amount(&self) -> Amount {
        *self
    }

    fn try_add_assign(&mut self, other: Amount) -> Result<(), ArithmeticError> {
        self.try_add_assign(other)
    }

    fn try_sub_assign(&mut self, other: Amount) -> Result<(), ArithmeticError> {
        self.try_sub_assign(other)
    }
}

// This is also needed for the default instantiation `ResourceController<Amount, ResourceTracker>`.
// See https://doc.rust-lang.org/std/convert/trait.AsMut.html#reflexivity for general context.
impl AsMut<ResourceTracker> for ResourceTracker {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

/// A temporary object made of an optional [`Owner`] together with a mutable reference
/// on an execution state view [`ExecutionStateView`].
///
/// This type is meant to implement [`BalanceHolder`] and make the accounting functions of
/// [`ResourceController`] available to temporary objects of type
/// `ResourceController<OwnedView<'a, C>, &'a mut ResourceTracker>`.
///
/// Such temporary objects can be obtained from values of type
/// `ResourceController<Option<Owner>>` using the method `with` to provide the missing
/// reference on [`ExecutionStateView`].
pub struct OwnedView<'a, C> {
    owner: Option<Owner>,
    view: &'a mut ExecutionStateView<C>,
}

impl<'a, C> OwnedView<'a, C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    fn get_owner_balance(&self, owner: &Owner) -> Amount {
        self.view
            .system
            .balances
            .get(owner)
            .now_or_never()
            .expect("The map entry was previously loaded by OwnedView::with")
            .expect("Account was created there as well")
            .expect("No I/O can fail here")
    }

    fn get_owner_balance_mut(&mut self, owner: &Owner) -> &mut Amount {
        self.view
            .system
            .balances
            .get_mut(owner)
            .now_or_never()
            .expect("The map entry was previously loaded by OwnedView::with")
            .expect("Account was created there as well")
            .expect("No I/O can fail here")
    }
}

impl<C> BalanceHolder for OwnedView<'_, C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    fn as_amount(&self) -> Amount {
        match &self.owner {
            None => *self.view.system.balance.get(),
            Some(owner) => self
                .view
                .system
                .balance
                .get()
                .try_add(self.get_owner_balance(owner))
                .expect("Overflow was tested in `ResourceController::with` and `add_assign`"),
        }
    }

    fn try_add_assign(&mut self, other: Amount) -> Result<(), ArithmeticError> {
        match self.owner {
            None => self.view.system.balance.get_mut().try_add_assign(other),
            Some(owner) => {
                let balance = self.get_owner_balance_mut(&owner);
                balance.try_add_assign(other)?;
                // Safety check. (See discussion below in `ResourceController::with`).
                balance.try_add(*self.view.system.balance.get())?;
                Ok(())
            }
        }
    }

    fn try_sub_assign(&mut self, other: Amount) -> Result<(), ArithmeticError> {
        match self.owner {
            None => self.view.system.balance.get_mut().try_sub_assign(other),
            Some(owner) => {
                // Charge the owner's account first, then the chain's account for the
                // reminder.
                if self
                    .get_owner_balance_mut(&owner)
                    .try_sub_assign(other)
                    .is_err()
                {
                    let balance = self.get_owner_balance(&owner);
                    let delta = other.try_sub(balance).expect("balance < other");
                    self.view.system.balance.get_mut().try_sub_assign(delta)?;
                    *self.get_owner_balance_mut(&owner) = Amount::ZERO;
                }
                Ok(())
            }
        }
    }
}

impl ResourceController<Option<Owner>, ResourceTracker> {
    /// Provides a reference to the current execution state and obtains a temporary object
    /// where the accounting functions of [`ResourceController`] are available.
    pub async fn with<'a, C>(
        &mut self,
        view: &'a mut ExecutionStateView<C>,
    ) -> Result<ResourceController<OwnedView<'a, C>, &mut ResourceTracker>, ViewError>
    where
        C: Context + Clone + Send + Sync + 'static,
        ViewError: From<C::Error>,
    {
        if let Some(owner) = &self.account {
            // Make sure `owner` has an account and that the account is loaded in memory.
            let balance = view.system.balances.get_mut(owner).await?;
            if let Some(balance) = balance {
                // Making sure the sum doesn't overflow. In practice, though, the total
                // supply of tokens is known so this should never happen.
                view.system.balance.get().try_add(*balance)?;
            } else {
                view.system.balances.insert(owner, Amount::ZERO)?;
            }
        }
        Ok(ResourceController {
            policy: self.policy.clone(),
            tracker: &mut self.tracker,
            account: OwnedView {
                owner: self.account,
                view,
            },
        })
    }
}
