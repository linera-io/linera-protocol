// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module tracks the resources used during the execution of a transaction.

use crate::{policy::ResourceControlPolicy, system::SystemExecutionError, ExecutionError};

use custom_debug_derive::Debug;
use linera_base::data_types::Amount;

/// The resource constraints applicable to an execution process.
#[derive(Copy, Debug, Clone)]
pub struct RuntimeLimits {
    /// The maximum read requests per block
    pub max_budget_num_reads: u64,
    /// The maximum number of bytes that can be read per block
    pub max_budget_bytes_read: u64,
    /// The maximum number of bytes that can be written per block
    pub max_budget_bytes_written: u64,
    /// The maximum size of read allowed per block
    pub maximum_bytes_left_to_read: u64,
    /// The maximum size of write allowed per block
    pub maximum_bytes_left_to_write: u64,
}

/// The resources used so far by an execution process.
#[derive(Copy, Debug, Clone, Default)]
pub struct ResourceTracker {
    /// The used fuel in the computation
    pub used_fuel: u64,
    /// The number of reads in the computation
    pub num_reads: u64,
    /// The total number of bytes read
    pub bytes_read: u64,
    /// The total number of bytes written
    pub bytes_written: u64,
    /// The change in the total data being stored
    pub stored_size_delta: i32,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        RuntimeLimits {
            max_budget_num_reads: u64::MAX / 2,
            max_budget_bytes_read: u64::MAX / 2,
            max_budget_bytes_written: u64::MAX / 2,
            maximum_bytes_left_to_read: u64::MAX / 2,
            maximum_bytes_left_to_write: u64::MAX / 2,
        }
    }
}

impl ResourceTracker {
    /// Subtracts an amount from a balance and reports an error if that is impossible
    fn sub_assign_fees(balance: &mut Amount, fees: Amount) -> Result<(), SystemExecutionError> {
        balance
            .try_sub_assign(fees)
            .map_err(|_| SystemExecutionError::InsufficientFunding {
                current_balance: *balance,
            })
    }

    /// Updates the limits for the maximum and updates the balance.
    pub fn update_limits(
        &mut self,
        balance: &mut Amount,
        policy: &ResourceControlPolicy,
        runtime_counts: RuntimeCounts,
    ) -> Result<(), ExecutionError> {
        // The fuel being used
        let initial_fuel = policy.remaining_fuel(*balance);
        let used_fuel = initial_fuel.saturating_sub(runtime_counts.remaining_fuel);
        self.used_fuel += used_fuel;
        Self::sub_assign_fees(balance, policy.fuel_price(used_fuel)?)?;

        // The number of reads
        Self::sub_assign_fees(
            balance,
            policy.storage_num_reads_price(runtime_counts.num_reads)?,
        )?;
        self.num_reads += runtime_counts.num_reads;

        // The number of bytes read
        let bytes_read = runtime_counts.bytes_read;
        self.bytes_read += runtime_counts.bytes_read;
        Self::sub_assign_fees(balance, policy.storage_bytes_read_price(bytes_read)?)?;

        // The number of bytes written
        let bytes_written = runtime_counts.bytes_written;
        self.bytes_written += bytes_written;
        Self::sub_assign_fees(balance, policy.storage_bytes_written_price(bytes_written)?)?;

        Ok(())
    }

    /// Obtain the limits for the running of the system
    pub fn limits(&self, policy: &ResourceControlPolicy, balance: &Amount) -> RuntimeLimits {
        let max_budget_num_reads =
            u64::try_from(balance.saturating_div(policy.storage_num_reads)).unwrap_or(u64::MAX);
        let max_budget_bytes_read =
            u64::try_from(balance.saturating_div(policy.storage_bytes_read)).unwrap_or(u64::MAX);
        let max_budget_bytes_written =
            u64::try_from(balance.saturating_div(policy.storage_bytes_written)).unwrap_or(u64::MAX);
        let maximum_bytes_left_to_read = policy.maximum_bytes_read_per_block.saturating_sub(self.bytes_read);
        let maximum_bytes_left_to_write = policy.maximum_bytes_written_per_block.saturating_sub(self.bytes_written);
        RuntimeLimits {
            max_budget_num_reads,
            max_budget_bytes_read,
            max_budget_bytes_written,
            maximum_bytes_left_to_read,
            maximum_bytes_left_to_write,
        }
    }
}

/// The entries of the runtime related to fuel and storage
#[derive(Copy, Debug, Clone, Default)]
pub struct RuntimeCounts {
    /// The remaining fuel available
    pub remaining_fuel: u64,
    /// The number of read operations
    pub num_reads: u64,
    /// The bytes that have been read
    pub bytes_read: u64,
    /// The bytes that have been written
    pub bytes_written: u64,
    /// The change in the total data stored
    pub stored_size_delta: i32,
}

impl RuntimeCounts {
    pub fn increment_num_reads(&mut self, limits: &RuntimeLimits) -> Result<(), ExecutionError> {
        self.num_reads += 1;
        if self.num_reads >= limits.max_budget_num_reads {
            return Err(ExecutionError::ExcessiveNumReads);
        }
        Ok(())
    }

    pub fn increment_bytes_read(
        &mut self,
        limits: &RuntimeLimits,
        increment: u64,
    ) -> Result<(), ExecutionError> {
        self.bytes_read = self
            .bytes_read
            .checked_add(increment)
            .ok_or(ExecutionError::ExcessiveRead)?;
        if self.bytes_read >= limits.max_budget_bytes_read
            || self.bytes_read >= limits.maximum_bytes_left_to_read
        {
            return Err(ExecutionError::ExcessiveRead);
        }
        Ok(())
    }

    pub fn increment_bytes_written(
        &mut self,
        limits: &RuntimeLimits,
        increment: u64,
    ) -> Result<(), ExecutionError> {
        self.bytes_written = self
            .bytes_written
            .checked_add(increment)
            .ok_or(ExecutionError::ExcessiveWrite)?;
        if self.bytes_written >= limits.max_budget_bytes_written
            || self.bytes_written >= limits.maximum_bytes_left_to_write
        {
            return Err(ExecutionError::ExcessiveWrite);
        }
        Ok(())
    }

    pub fn update_stored_size(&mut self, delta: i32) -> Result<(), ExecutionError> {
        self.stored_size_delta += delta;
        Ok(())
    }
}
