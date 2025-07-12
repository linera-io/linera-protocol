// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.
//! Here we implement the Database traits of Revm.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use linera_base::vm::VmRuntime;
use linera_views::common::from_bytes_option;
use revm::{primitives::keccak256, Database, DatabaseCommit, DatabaseRef};
use revm_context::BlockEnv;
use revm_context_interface::block::BlobExcessGasAndPrice;
use revm_database::{AccountState, DBErrorMarker};
use revm_primitives::{address, Address, B256, U256};
use revm_state::{AccountInfo, Bytecode, EvmState};

use crate::{ApplicationId, BaseRuntime, Batch, ContractRuntime, ExecutionError, ServiceRuntime};

// The runtime costs are not available in service operations.
// We need to set a limit to gas usage in order to avoid blocking
// the validator.
// We set up the limit similarly to Infura to 20 million.
pub const EVM_SERVICE_GAS_LIMIT: u64 = 20_000_000;

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

/// The number of key writes, reads, release, and no change in EVM has to be accounted for.
/// Then we remove those costs from the final bill.
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

/// This is the encapsulation of the `Runtime` corresponding to the contract.
pub(crate) struct DatabaseRuntime<Runtime> {
    /// This is the storage statistics of the read/write in order to adjust gas costs.
    storage_stats: Arc<Mutex<StorageStats>>,
    /// This is the EVM address of the contract.
    /// At the creation, it is set to `Address::ZERO` and then later set to the correct value.
    pub contract_address: Address,
    /// The runtime of the contract.
    pub runtime: Arc<Mutex<Runtime>>,
    /// The uncommitted changes to the contract.
    pub changes: EvmState,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self {
            storage_stats: self.storage_stats.clone(),
            contract_address: self.contract_address,
            runtime: self.runtime.clone(),
            changes: self.changes.clone(),
        }
    }
}

#[repr(u8)]
pub enum KeyCategory {
    AccountInfo,
    AccountState,
    Storage,
}

fn application_id_to_address(application_id: ApplicationId) -> Address {
    let application_id: [u64; 4] = <[u64; 4]>::from(application_id.application_description_hash);
    let application_id: [u8; 32] = linera_base::crypto::u64_array_to_be_bytes(application_id);
    Address::from_slice(&application_id[0..20])
}

impl<Runtime: BaseRuntime> DatabaseRuntime<Runtime> {
    /// Encode the `index` of the EVM storage associated to the smart contract
    /// in a linera key.
    fn get_linera_key(key_prefix: &[u8], index: U256) -> Result<Vec<u8>, ExecutionError> {
        let mut key = key_prefix.to_vec();
        bcs::serialize_into(&mut key, &index)?;
        Ok(key)
    }

    /// Returns the tag associated to the contract.
    fn get_address_key(&self, prefix: u8, address: Address) -> Vec<u8> {
        let mut key = vec![prefix];
        key.extend(address);
        key
    }

    /// Creates a new `DatabaseRuntime`.
    pub fn new(runtime: Runtime) -> Self {
        let storage_stats = StorageStats::default();
        // We cannot acquire a lock on runtime here.
        // So, we set the contract_address to a default value
        // and update it later.
        Self {
            storage_stats: Arc::new(Mutex::new(storage_stats)),
            contract_address: Address::ZERO,
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

impl DBErrorMarker for ExecutionError {}

impl<Runtime> Database for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
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
        let key_info = self.get_address_key(KeyCategory::AccountInfo as u8, address);
        let promise = runtime.read_value_bytes_new(key_info)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        let account_info = from_bytes_option::<AccountInfo>(&result)?;
        Ok(account_info)
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
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
        let key_prefix = self.get_address_key(KeyCategory::Storage as u8, address);
        let key = Self::get_linera_key(&key_prefix, index)?;
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
        Ok(from_bytes_option::<U256>(&result)?.unwrap_or_default())
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
        for (address, account) in &self.changes {
            if !account.is_touched() {
                continue;
            }
            let key_prefix = self.get_address_key(KeyCategory::Storage as u8, *address);
            let key_info = self.get_address_key(KeyCategory::AccountInfo as u8, *address);
            let key_state = self.get_address_key(KeyCategory::AccountState as u8, *address);
            if account.is_selfdestructed() {
                batch.delete_key_prefix(key_prefix);
                batch.put_key_value(key_info, &AccountInfo::default())?;
                batch.put_key_value(key_state, &AccountState::NotExisting)?;
            } else {
                let is_newly_created = account.is_created();
                batch.put_key_value(key_info, &account.info)?;
                let account_state = if is_newly_created {
                    batch.delete_key_prefix(key_prefix.clone());
                    AccountState::StorageCleared
                } else {
                    let promise = runtime.read_value_bytes_new(key_state.clone())?;
                    let result = runtime.read_value_bytes_wait(&promise)?;
                    let account_state =
                        from_bytes_option::<AccountState>(&result)?.unwrap_or_default();
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
                        let key = Self::get_linera_key(&key_prefix, *index)?;
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
        }
        runtime.write_batch(batch)?;
        self.changes.clear();
        Ok(())
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    /// Reads the nonce of the user
    pub fn get_nonce(&self, address: &Address) -> Result<u64, ExecutionError> {
        let account_info = self.basic_ref(*address)?;
        Ok(match account_info {
            None => 0,
            Some(account_info) => account_info.nonce,
        })
    }

    /// Sets the EVM contract address from the value Address::ZERO.
    /// The value is set from the `ApplicationId`.
    pub fn set_contract_address(&mut self) -> Result<(), ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let application_id = runtime.application_id()?;
        self.contract_address = application_id_to_address(application_id);
        Ok(())
    }

    /// Checks if the contract is already initialized. It is possible
    /// that the constructor has not yet been called.
    pub fn is_initialized(&self) -> Result<bool, ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let evm_address = runtime.application_id()?.evm_address();
        let key_info = self.get_address_key(KeyCategory::AccountInfo as u8, evm_address);
        let promise = runtime.contains_key_new(key_info)?;
        let result = runtime.contains_key_wait(&promise)?;
        Ok(result)
    }

    pub fn get_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        // The block height being used
        let block_height_linera = runtime.block_height()?;
        let block_height_evm = block_height_linera.0;
        // This is the receiver address of all the gas spent in the block.
        let beneficiary = address!("00000000000000000000000000000000000000bb");
        // The difficulty which is no longer relevant after The Merge.
        let difficulty = U256::ZERO;
        // We do not have access to the Resources so we keep it to the maximum
        // and the control is done elsewhere.
        let gas_limit = u64::MAX;
        // The timestamp. Both the EVM and Linera use the same UNIX epoch.
        // But the Linera epoch is in microseconds since the start and the
        // Ethereum epoch is in seconds
        let timestamp_linera = runtime.read_system_timestamp()?;
        let timestamp_evm = timestamp_linera.micros() / 1_000_000;
        // The basefee is the minimum feee for executing. We have no such
        // concept in Linera
        let basefee = 0;
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
            beneficiary,
            difficulty,
            gas_limit,
            timestamp: timestamp_evm,
            basefee,
            prevrandao: Some(prevrandao),
            blob_excess_gas_and_price,
        })
    }

    pub fn constructor_argument(&self) -> Result<Vec<u8>, ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let constructor_argument = runtime.application_parameters()?;
        Ok(serde_json::from_slice::<Vec<u8>>(&constructor_argument)?)
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn get_contract_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut block_env = self.get_block_env()?;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        // We use the gas_limit from the runtime
        let gas_limit = runtime.maximum_fuel_per_block(VmRuntime::Evm)?;
        block_env.gas_limit = gas_limit;
        Ok(block_env)
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn get_service_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut block_env = self.get_block_env()?;
        block_env.gas_limit = EVM_SERVICE_GAS_LIMIT;
        Ok(block_env)
    }
}
