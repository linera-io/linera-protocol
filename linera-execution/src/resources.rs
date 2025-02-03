// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module tracks the resources used during the execution of a transaction.

use std::sync::Arc;

use custom_debug_derive::Debug;
use linera_base::{
    data_types::{Amount, ArithmeticError},
    ensure,
    identifiers::{AccountOwner, Owner},
};
use linera_views::{context::Context, views::ViewError};
use serde::Serialize;

use crate::{
    system::SystemExecutionError, ExecutionError, ExecutionStateView, Message, Operation,
    ResourceControlPolicy,
};

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
    /// The total size of the executed block so far.
    pub block_size: u64,
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
    /// The amount allocated to message grants.
    pub grants: Amount,
}

/// How to access the balance of an account.
pub trait BalanceHolder {
    fn balance(&self) -> Result<Amount, ArithmeticError>;

    fn try_add_assign(&mut self, other: Amount) -> Result<(), ArithmeticError>;

    fn try_sub_assign(&mut self, other: Amount) -> Result<(), ArithmeticError>;
}

// The main accounting functions for a ResourceController.
impl<Account, Tracker> ResourceController<Account, Tracker>
where
    Account: BalanceHolder,
    Tracker: AsRef<ResourceTracker> + AsMut<ResourceTracker>,
{
    /// Obtains the balance of the account. The only possible error is an arithmetic
    /// overflow, which should not happen in practice due to final token supply.
    pub fn balance(&self) -> Result<Amount, ArithmeticError> {
        self.account.balance()
    }

    /// Operates a 3-way merge by transferring the difference between `initial`
    /// and `other` to `self`.
    pub fn merge_balance(&mut self, initial: Amount, other: Amount) -> Result<(), ExecutionError> {
        if other <= initial {
            self.account
                .try_sub_assign(initial.try_sub(other).expect("other <= initial"))
                .map_err(|_| SystemExecutionError::InsufficientFundingForFees {
                    balance: self.balance().unwrap_or(Amount::MAX),
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
            SystemExecutionError::InsufficientFundingForFees {
                balance: self.balance().unwrap_or(Amount::MAX),
            }
        })?;
        Ok(())
    }

    /// Obtains the amount of fuel that could be spent by consuming the entire balance.
    pub(crate) fn remaining_fuel(&self) -> u64 {
        self.policy
            .remaining_fuel(self.balance().unwrap_or(Amount::MAX))
            .min(
                self.policy
                    .maximum_fuel_per_block
                    .saturating_sub(self.tracker.as_ref().fuel),
            )
    }

    /// Tracks the allocation of a grant.
    pub fn track_grant(&mut self, grant: Amount) -> Result<(), ExecutionError> {
        self.tracker.as_mut().grants.try_add_assign(grant)?;
        self.update_balance(grant)
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
                self.update_balance(self.policy.operation_bytes_price(size as u64)?)?;
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
                self.update_balance(self.policy.message_bytes_price(size as u64)?)?;
                Ok(())
            }
        }
    }

    /// Tracks a number of fuel units used.
    pub(crate) fn track_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError> {
        self.tracker.as_mut().fuel = self
            .tracker
            .as_ref()
            .fuel
            .checked_add(fuel)
            .ok_or(ArithmeticError::Overflow)?;
        ensure!(
            self.tracker.as_ref().fuel <= self.policy.maximum_fuel_per_block,
            ExecutionError::MaximumFuelExceeded
        );
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

impl<Account, Tracker> ResourceController<Account, Tracker>
where
    Tracker: AsMut<ResourceTracker>,
{
    /// Tracks the extension of a sequence in an executed block.
    ///
    /// The sequence length is ULEB128-encoded, so extending a sequence can add an additional byte.
    pub fn track_executed_block_size_sequence_extension(
        &mut self,
        old_len: usize,
        delta: usize,
    ) -> Result<(), ExecutionError> {
        if delta == 0 {
            return Ok(());
        }
        let new_len = old_len + delta;
        // ULEB128 uses one byte per 7 bits of the number. It always uses at least one byte.
        let old_size = ((usize::BITS - old_len.leading_zeros()) / 7).max(1);
        let new_size = ((usize::BITS - new_len.leading_zeros()) / 7).max(1);
        if new_size > old_size {
            self.track_block_size((new_size - old_size) as usize)?;
        }
        Ok(())
    }

    /// Tracks the serialized size of an executed block, or parts of it.
    pub fn track_block_size_of(&mut self, data: &impl Serialize) -> Result<(), ExecutionError> {
        self.track_block_size(bcs::serialized_size(data)?)
    }

    /// Tracks the serialized size of an executed block, or parts of it.
    pub fn track_block_size(&mut self, size: usize) -> Result<(), ExecutionError> {
        let tracker = self.tracker.as_mut();
        tracker.block_size = u64::try_from(size)
            .ok()
            .and_then(|size| tracker.block_size.checked_add(size))
            .ok_or(ExecutionError::ExecutedBlockTooLarge)?;
        ensure!(
            tracker.block_size <= self.policy.maximum_executed_block_size,
            ExecutionError::ExecutedBlockTooLarge
        );
        Ok(())
    }
}

// The simplest `BalanceHolder` is an `Amount`.
impl BalanceHolder for Amount {
    fn balance(&self) -> Result<Amount, ArithmeticError> {
        Ok(*self)
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

impl AsRef<ResourceTracker> for ResourceTracker {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// A temporary object holding a number of references to funding sources.
pub struct Sources<'a> {
    sources: Vec<&'a mut Amount>,
}

impl BalanceHolder for Sources<'_> {
    fn balance(&self) -> Result<Amount, ArithmeticError> {
        let mut amount = Amount::ZERO;
        for source in self.sources.iter() {
            amount.try_add_assign(**source)?;
        }
        Ok(amount)
    }

    fn try_add_assign(&mut self, other: Amount) -> Result<(), ArithmeticError> {
        // Try to credit the owner account first.
        // TODO(#1648): This may need some additional design work.
        let source = self.sources.last_mut().expect("at least one source");
        source.try_add_assign(other)
    }

    fn try_sub_assign(&mut self, mut other: Amount) -> Result<(), ArithmeticError> {
        for source in self.sources.iter_mut() {
            if source.try_sub_assign(other).is_ok() {
                return Ok(());
            }
            other.try_sub_assign(**source).expect("*source < other");
            **source = Amount::ZERO;
        }
        if other > Amount::ZERO {
            Err(ArithmeticError::Underflow)
        } else {
            Ok(())
        }
    }
}

impl ResourceController<Option<Owner>, ResourceTracker> {
    /// Provides a reference to the current execution state and obtains a temporary object
    /// where the accounting functions of [`ResourceController`] are available.
    pub async fn with_state<'a, C>(
        &mut self,
        view: &'a mut ExecutionStateView<C>,
    ) -> Result<ResourceController<Sources<'a>, &mut ResourceTracker>, ViewError>
    where
        C: Context + Clone + Send + Sync + 'static,
    {
        self.with_state_and_grant(view, None).await
    }

    /// Provides a reference to the current execution state as well as an optional grant,
    /// and obtains a temporary object where the accounting functions of
    /// [`ResourceController`] are available.
    pub async fn with_state_and_grant<'a, C>(
        &mut self,
        view: &'a mut ExecutionStateView<C>,
        grant: Option<&'a mut Amount>,
    ) -> Result<ResourceController<Sources<'a>, &mut ResourceTracker>, ViewError>
    where
        C: Context + Clone + Send + Sync + 'static,
    {
        let mut sources = Vec::new();
        // First, use the grant (e.g. for messages) and otherwise use the chain account
        // (e.g. for blocks and operations).
        if let Some(grant) = grant {
            sources.push(grant);
        } else {
            sources.push(view.system.balance.get_mut());
        }
        // Then the local account, if any. Currently, any negative fee (e.g. storage
        // refund) goes preferably to this account.
        if let Some(owner) = &self.account {
            if let Some(balance) = view
                .system
                .balances
                .get_mut(&AccountOwner::User(*owner))
                .await?
            {
                sources.push(balance);
            }
        }

        Ok(ResourceController {
            policy: self.policy.clone(),
            tracker: &mut self.tracker,
            account: Sources { sources },
        })
    }
}
