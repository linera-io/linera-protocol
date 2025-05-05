// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.
//! Here we implement the Database traits of Revm.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use alloy::primitives::{Address, B256, U256};
use linera_views::common::from_bytes_option;
use revm::{
    db::AccountState,
    primitives::{keccak256, state::AccountInfo},
    Database, DatabaseCommit, DatabaseRef,
};
use revm_primitives::{address, BlobExcessGasAndPrice, BlockEnv, EvmState};

use crate::{BaseRuntime, Batch, ContractRuntime, ExecutionError, ViewError};

/// The cost of loading from storage.
const SLOAD_COST: u64 = 2100;

/// The cost of storing a non-zero value in the storage for the first time.
const SSTORE_COST_SET: u64 = 20000;

/// The cost of not changing the state of the variable in the storage.
const SSTORE_COST_NO_OPERATION: u64 = 100;

/// The cost of overwriting the storage to a different value.
const SSTORE_COST_RESET: u64 = 2900;

/// The refund from releasing data.
const SSTORE_REFUND_RELEASE: u64 = 4800;

#[derive(Clone, Default)]
pub(crate) struct StorageStats {
    key_no_operation: u64,
    key_reset: u64,
    key_set: u64,
    key_release: u64,
    key_read: u64,
}

impl StorageStats {
    pub fn storage_costs(&self) -> u64 {
        let mut storage_costs = 0;
        storage_costs += self.key_no_operation * SSTORE_COST_NO_OPERATION;
        storage_costs += self.key_reset * SSTORE_COST_RESET;
        storage_costs += self.key_set * SSTORE_COST_SET;
        storage_costs += self.key_read * SLOAD_COST;
        storage_costs
    }

    pub fn storage_refund(&self) -> u64 {
        self.key_release * SSTORE_REFUND_RELEASE
    }
}

pub(crate) struct DatabaseRuntime<Runtime> {
    storage_stats: Arc<Mutex<StorageStats>>,
    pub runtime: Arc<Mutex<Runtime>>,
    pub changes: EvmState,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self {
            storage_stats: self.storage_stats.clone(),
            runtime: self.runtime.clone(),
            changes: self.changes.clone(),
        }
    }
}

#[repr(u8)]
enum KeyTag {
    /// Key prefix for the storage of the zero contract.
    NullAddress,
    /// Key prefix for the storage of the contract address.
    ContractAddress,
}

#[repr(u8)]
pub enum KeyCategory {
    AccountInfo,
    AccountState,
    Storage,
}

impl<Runtime> DatabaseRuntime<Runtime> {
    /// Encode the `index` of the EVM storage associated to the smart contract
    /// in a linera key.
    fn get_linera_key(val: u8, index: U256) -> Result<Vec<u8>, ExecutionError> {
        let mut key = vec![val, KeyCategory::Storage as u8];
        bcs::serialize_into(&mut key, &index)?;
        Ok(key)
    }

    /// Returns the tag associated to the contract.
    fn get_contract_address_key(&self, address: &Address) -> Option<u8> {
        if address == &Address::ZERO {
            return Some(KeyTag::NullAddress as u8);
        }
        if address == &Address::ZERO.create(0) {
            return Some(KeyTag::ContractAddress as u8);
        }
        None
    }

    /// Creates a new `DatabaseRuntime`.
    pub fn new(runtime: Runtime) -> Self {
        let storage_stats = StorageStats::default();
        Self {
            storage_stats: Arc::new(Mutex::new(storage_stats)),
            runtime: Arc::new(Mutex::new(runtime)),
            changes: HashMap::new(),
        }
    }

    /// Returns the current storage states and clears it to default.
    pub fn take_storage_stats(&self) -> StorageStats {
        let mut storage_stats_read = self
            .storage_stats
            .lock()
            .expect("The lock should be possible");
        let storage_stats = storage_stats_read.clone();
        *storage_stats_read = StorageStats::default();
        storage_stats
    }
}

impl<Runtime> Database for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.basic_ref(address)
    }

    fn code_by_hash(
        &mut self,
        _code_hash: B256,
    ) -> Result<revm::primitives::Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash not implemented");
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, ExecutionError> {
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

impl<Runtime> DatabaseCommit for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    fn commit(&mut self, changes: EvmState) {
        self.changes = changes;
    }
}

impl<Runtime> DatabaseRef for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        if !self.changes.is_empty() {
            let account = self.changes.get(&address).unwrap();
            return Ok(Some(account.info.clone()));
        }
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let val = self.get_contract_address_key(&address);
        let Some(val) = val else {
            return Ok(Some(AccountInfo::default()));
        };
        let key_info = vec![val, KeyCategory::AccountInfo as u8];
        let promise = runtime.read_value_bytes_new(key_info)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        let account_info = from_bytes_option::<AccountInfo, ViewError>(&result)?;
        Ok(account_info)
    }

    fn code_by_hash_ref(
        &self,
        _code_hash: B256,
    ) -> Result<revm::primitives::Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash_ref not implemented");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        if !self.changes.is_empty() {
            let account = self.changes.get(&address).unwrap();
            return Ok(match account.storage.get(&index) {
                None => U256::ZERO,
                Some(slot) => slot.present_value(),
            });
        }
        let val = self.get_contract_address_key(&address);
        let Some(val) = val else {
            panic!("There is no storage associated to externally owned account");
        };
        let key = Self::get_linera_key(val, index)?;
        {
            let mut storage_stats = self
                .storage_stats
                .lock()
                .expect("The lock should be possible");
            storage_stats.key_read += 1;
        }
        let result = {
            let mut runtime = self.runtime.lock().expect("The lock should be possible");
            let promise = runtime.read_value_bytes_new(key)?;
            runtime.read_value_bytes_wait(&promise)
        }?;
        Ok(from_bytes_option::<U256, ViewError>(&result)?.unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, ExecutionError> {
        Ok(keccak256(number.to_string().as_bytes()))
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    /// Effectively commits changes to storage.
    pub fn commit_changes(&mut self) -> Result<(), ExecutionError> {
        let mut storage_stats = self
            .storage_stats
            .lock()
            .expect("The lock should be possible");
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let mut batch = Batch::new();
        let mut list_new_balances = Vec::new();
        for (address, account) in &self.changes {
            if !account.is_touched() {
                continue;
            }
            let val = self.get_contract_address_key(address);
            if let Some(val) = val {
                let key_prefix = vec![val, KeyCategory::Storage as u8];
                let key_info = vec![val, KeyCategory::AccountInfo as u8];
                let key_state = vec![val, KeyCategory::AccountState as u8];
                if account.is_selfdestructed() {
                    batch.delete_key_prefix(key_prefix);
                    batch.put_key_value(key_info, &AccountInfo::default())?;
                    batch.put_key_value(key_state, &AccountState::NotExisting)?;
                } else {
                    let is_newly_created = account.is_created();
                    batch.put_key_value(key_info, &account.info)?;

                    let account_state = if is_newly_created {
                        batch.delete_key_prefix(key_prefix);
                        AccountState::StorageCleared
                    } else {
                        let promise = runtime.read_value_bytes_new(key_state.clone())?;
                        let result = runtime.read_value_bytes_wait(&promise)?;
                        let account_state = from_bytes_option::<AccountState, ViewError>(&result)?
                            .unwrap_or_default();
                        if account_state.is_storage_cleared() {
                            AccountState::StorageCleared
                        } else {
                            AccountState::Touched
                        }
                    };
                    batch.put_key_value(key_state, &account_state)?;
                    for (index, value) in &account.storage {
                        if value.present_value() == value.original_value() {
                            storage_stats.key_no_operation += 1;
                        } else {
                            let key = Self::get_linera_key(val, *index)?;
                            if value.original_value() == U256::ZERO {
                                batch.put_key_value(key, &value.present_value())?;
                                storage_stats.key_set += 1;
                            } else if value.present_value() == U256::ZERO {
                                batch.delete_key(key);
                                storage_stats.key_release += 1;
                            } else {
                                batch.put_key_value(key, &value.present_value())?;
                                storage_stats.key_reset += 1;
                            }
                        }
                    }
                }
            } else {
                if !account.storage.is_empty() {
                    panic!("For user account, storage must be empty");
                }
                // TODO(#3756): Implement EVM transfers within Linera.
                // The only allowed operations are the ones for the
                // account balances.
                if account.info.balance != U256::ZERO {
                    let new_balance = (address, account.info.balance);
                    list_new_balances.push(new_balance);
                }
            }
        }
        runtime.write_batch(batch)?;
        if !list_new_balances.is_empty() {
            panic!("The conversion Ethereum address / Linera address is not yet implemented");
        }
        self.changes.clear();
        Ok(())
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    pub fn is_initialized(&self) -> Result<bool, ExecutionError> {
        let mut keys = Vec::new();
        for key_tag in [KeyTag::NullAddress, KeyTag::ContractAddress] {
            let key = vec![key_tag as u8, KeyCategory::AccountInfo as u8];
            keys.push(key);
        }
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let promise = runtime.contains_keys_new(keys)?;
        let result = runtime.contains_keys_wait(&promise)?;
        Ok(result[0] && result[1])
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    pub fn get_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        // The block height being used
        let block_height_linera = runtime.block_height()?;
        let block_height_evm = U256::from(block_height_linera.0);
        // This is the receiver address of all the gas spent in the block.
        let beneficiary = address!("00000000000000000000000000000000000000bb");
        // The difficulty which is no longer relevant after The Merge.
        let difficulty = U256::ZERO;
        // We do not have access to the Resources so we keep it to the maximum
        // and the control is done elsewhere.
        let gas_limit = U256::MAX;
        // The timestamp. Both the EVM and Linera use the same UNIX epoch.
        // But the Linera epoch is in microseconds since the start and the
        // Ethereum epoch is in seconds
        let timestamp_linera = runtime.read_system_timestamp()?;
        let timestamp_evm = U256::from(timestamp_linera.micros() / 1_000_000);
        // The basefee is the minimum feee for executing. We have no such
        // concept in Linera
        let basefee = U256::ZERO;
        let chain_id = runtime.chain_id()?;
        let entry = format!("{}{}", chain_id, block_height_linera);
        // The randomness beacon being used.
        let prevrandao = keccak256(entry.as_bytes());
        // The blob excess gas and price is not relevant to the execution
        // on Linera. We set up a default value as in REVM.
        let entry = BlobExcessGasAndPrice {
            excess_blob_gas: 0,
            blob_gasprice: 1,
        };
        let blob_excess_gas_and_price = Some(entry);
        Ok(BlockEnv {
            number: block_height_evm,
            coinbase: beneficiary,
            difficulty,
            gas_limit,
            timestamp: timestamp_evm,
            basefee,
            prevrandao: Some(prevrandao),
            blob_excess_gas_and_price,
        })
    }
}
