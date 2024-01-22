// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module tracks the resources used during the execution of a transaction.

use crate::{policy::ResourceControlPolicy, system::SystemExecutionError, ExecutionError};
use custom_debug_derive::Debug;
use linera_base::data_types::{Amount, ArithmeticError};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct ResourceController {
    /// The (fixed) policy used to charge fees and control resource usage.
    pub(crate) policy: Arc<ResourceControlPolicy>,
    /// How the resources were used so far.
    pub(crate) tracker: ResourceTracker,
    /// The remaining balance of the account paying for the resource usage.
    pub(crate) balance: Amount,
}

/// The resources used so far by an execution process.
#[derive(Copy, Debug, Clone, Default)]
pub struct ResourceTracker {
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
    pub operation_bytes: u32,
    /// The number of messages executed.
    pub messages: u32,
    /// The total size of the arguments of user messages.
    pub message_bytes: u32,
}

impl ResourceController {
    /// Subtracts an amount from a balance and reports an error if that is impossible.
    fn update_balance(&mut self, fees: Amount) -> Result<(), ExecutionError> {
        self.balance.try_sub_assign(fees).map_err(|_| {
            SystemExecutionError::InsufficientFunding {
                current_balance: self.balance,
            }
        })?;
        Ok(())
    }

    /// Obtains the amount of fuel that could be spent by consuming the entire balance.
    pub(crate) fn remaining_fuel(&self) -> u64 {
        self.policy.remaining_fuel(self.balance)
    }

    /// Tracks the used fuel.
    pub(crate) fn track_fuel(&mut self, fuel: u64) -> Result<(), ExecutionError> {
        self.tracker.fuel = self
            .tracker
            .fuel
            .checked_add(fuel)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.fuel_price(fuel)?)
    }

    /// Tracks a read operation.
    pub(crate) fn track_read_operations(&mut self, count: u32) -> Result<(), ExecutionError> {
        self.tracker.read_operations = self
            .tracker
            .read_operations
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.read_operations_price(count)?)
    }

    /// Tracks a write operation.
    pub(crate) fn track_write_operations(&mut self, count: u32) -> Result<(), ExecutionError> {
        self.tracker.write_operations = self
            .tracker
            .write_operations
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        self.update_balance(self.policy.write_operations_price(count)?)
    }

    /// Tracks a number of bytes read.
    pub(crate) fn track_bytes_read(&mut self, count: u64) -> Result<(), ExecutionError> {
        self.tracker.bytes_read = self
            .tracker
            .bytes_read
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        if self.tracker.bytes_read >= self.policy.maximum_bytes_read_per_block {
            return Err(ExecutionError::ExcessiveRead);
        }
        self.update_balance(self.policy.bytes_read_price(count)?)?;
        Ok(())
    }

    /// Tracks a number of bytes written.
    pub(crate) fn track_bytes_written(&mut self, count: u64) -> Result<(), ExecutionError> {
        self.tracker.bytes_written = self
            .tracker
            .bytes_written
            .checked_add(count)
            .ok_or(ArithmeticError::Overflow)?;
        if self.tracker.bytes_written >= self.policy.maximum_bytes_written_per_block {
            return Err(ExecutionError::ExcessiveWrite);
        }
        self.update_balance(self.policy.bytes_written_price(count)?)?;
        Ok(())
    }

    /// Tracks a change in the number of bytes stored.
    // TODO(#1536): This is not fully implemented.
    #[allow(dead_code)]
    pub(crate) fn track_stored_bytes(&mut self, delta: i32) -> Result<(), ExecutionError> {
        self.tracker.bytes_stored = self
            .tracker
            .bytes_stored
            .checked_add(delta)
            .ok_or(ArithmeticError::Overflow)?;
        Ok(())
    }
}
