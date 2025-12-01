// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.
//! Here we implement the Database traits of Revm.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use linera_base::{data_types::Amount, ensure, identifiers::Account, vm::VmRuntime};
use linera_views::common::from_bytes_option;
use revm::{primitives::keccak256, Database, DatabaseCommit, DatabaseRef};
use revm_context::BlockEnv;
use revm_context_interface::block::BlobExcessGasAndPrice;
use revm_database::{AccountState, DBErrorMarker};
use revm_primitives::{address, Address, B256, U256};
use revm_state::{AccountInfo, Bytecode, EvmState};

use crate::{
    evm::inputs::{FAUCET_ADDRESS, FAUCET_BALANCE, ZERO_ADDRESS},
    BaseRuntime, Batch, ContractRuntime, EvmExecutionError, ExecutionError, ServiceRuntime,
};

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
    /// The caller to the smart contract.
    pub caller: Address,
    /// The value of the call to the smart contract.
    pub value: U256,
    /// The runtime of the contract.
    pub runtime: Arc<Mutex<Runtime>>,
    /// The uncommitted changes to the contract.
    pub changes: EvmState,
    /// Whether the contract has been instantiated in REVM.
    pub is_revm_instantiated: bool,
    /// The error that can occur during runtime.
    pub error: Arc<Mutex<Option<String>>>,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self {
            storage_stats: self.storage_stats.clone(),
            contract_address: self.contract_address,
            caller: self.caller,
            value: self.value,
            runtime: self.runtime.clone(),
            changes: self.changes.clone(),
            is_revm_instantiated: self.is_revm_instantiated,
            error: self.error.clone(),
        }
    }
}

#[repr(u8)]
pub enum KeyCategory {
    AccountInfo,
    AccountState,
    Storage,
}

impl<Runtime: BaseRuntime> DatabaseRuntime<Runtime> {
    /// Encodes the `index` of the EVM storage associated to the smart contract
    /// in a Linera key.
    fn get_linera_key(key_prefix: &[u8], index: U256) -> Vec<u8> {
        let mut key = key_prefix.to_vec();
        key.extend(index.as_le_slice());
        key
    }

    /// Returns the tag associated to the contract.
    fn get_address_key(prefix: u8, address: Address) -> Vec<u8> {
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
            caller: Address::ZERO,
            value: U256::ZERO,
            runtime: Arc::new(Mutex::new(runtime)),
            changes: HashMap::new(),
            is_revm_instantiated: false,
            error: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the current storage states and clears it to default.
    pub fn take_storage_stats(&self) -> StorageStats {
        let mut storage_stats_read = self.storage_stats.lock().unwrap();
        let storage_stats = storage_stats_read.clone();
        *storage_stats_read = StorageStats::default();
        storage_stats
    }

    /// Insert error into the database
    pub fn insert_error(&self, exec_error: ExecutionError) {
        let mut error = self.error.lock().unwrap();
        *error = Some(format!("Runtime error {:?}", exec_error));
    }

    /// Process the error.
    pub fn process_any_error(&self) -> Result<(), EvmExecutionError> {
        let error = self.error.lock().unwrap();
        if let Some(error) = error.clone() {
            return Err(EvmExecutionError::RuntimeError(error.clone()));
        }
        Ok(())
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

    /// The `basic_ref` is the function for reading the state of the application.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        if address == FAUCET_ADDRESS {
            return Ok(Some(AccountInfo {
                balance: FAUCET_BALANCE,
                ..AccountInfo::default()
            }));
        }
        if !self.changes.is_empty() {
            // This case occurs in only one scenario:
            // * A service call to a contract that has not yet been
            //   initialized by a contract call.
            // When we do a service calls to a contract that has
            // already been initialized, then changes will be empty.
            let account = self.changes.get(&address);
            return Ok(account.map(|account| account.info.clone()));
        }
        let mut runtime = self.runtime.lock().unwrap();
        let account_owner = address.into();
        // The balances being used are the ones of Linera. So, we need to
        // access them at first.
        let balance = runtime.read_owner_balance(account_owner)?;

        let balance: U256 = balance.into();
        let key_info = Self::get_address_key(KeyCategory::AccountInfo as u8, address);
        let promise = runtime.read_value_bytes_new(key_info)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        let mut account_info = match result {
            None => AccountInfo::default(),
            Some(bytes) => bcs::from_bytes(&bytes)?,
        };
        // The design is the following:
        // * The funds have been deposited in deposit_funds.
        // * The order of the operations is the following:
        //   + Access to the storage (this functions) of relevant accounts.
        //   + Transfer according to the input.
        //   + Running the constructor.
        // * So, the transfer is done twice: One at deposit_funds.
        //   Another in the transfer by REVM.
        // * So, we need to correct the balances so that when Revm
        //   is doing the transfer, the balance are the ones after
        //   deposit_funds.
        let start_balance = if self.caller == address {
            balance + self.value
        } else if self.contract_address == address {
            assert!(
                balance >= self.value,
                "We should have balance >= self.value"
            );
            balance - self.value
        } else {
            balance
        };
        account_info.balance = start_balance;
        // We return an account as there is no difference between
        // a default account and the absence of account.
        Ok(Some(account_info))
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
        let key_prefix = Self::get_address_key(KeyCategory::Storage as u8, address);
        let key = Self::get_linera_key(&key_prefix, index);
        {
            let mut storage_stats = self.storage_stats.lock().unwrap();
            storage_stats.key_read += 1;
        }
        let result = {
            let mut runtime = self.runtime.lock().unwrap();
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
        let mut storage_stats = self.storage_stats.lock().unwrap();
        let mut runtime = self.runtime.lock().unwrap();
        let mut batch = Batch::new();
        for (address, account) in &self.changes {
            if address == &FAUCET_ADDRESS {
                // We do not write the faucet address nor expect any coherency from it.
                continue;
            }
            let owner = (*address).into();
            let linera_balance: U256 = runtime.read_owner_balance(owner)?.into();
            let revm_balance = account.info.balance;
            ensure!(
                linera_balance == revm_balance,
                EvmExecutionError::IncoherentBalances(*address, linera_balance, revm_balance)
            );
            if !account.is_touched() {
                continue;
            }
            let key_prefix = Self::get_address_key(KeyCategory::Storage as u8, *address);
            let key_info = Self::get_address_key(KeyCategory::AccountInfo as u8, *address);
            let key_state = Self::get_address_key(KeyCategory::AccountState as u8, *address);
            if account.is_selfdestructed() {
                batch.delete_key_prefix(key_prefix);
                batch.put_key_value(key_info, &AccountInfo::default())?;
                batch.put_key_value(key_state, &AccountState::NotExisting)?;
            } else {
                let is_newly_created = account.is_created();
                // We write here the state of the user in question. But that does not matter
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
                        let key = Self::get_linera_key(&key_prefix, *index);
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
        let account_info: Option<AccountInfo> = self.basic_ref(*address)?;
        Ok(match account_info {
            None => 0,
            Some(account_info) => account_info.nonce,
        })
    }

    pub fn get_deployed_bytecode(&self) -> Result<Vec<u8>, ExecutionError> {
        let account_info = self.basic_ref(self.contract_address)?;
        Ok(match account_info {
            None => Vec::new(),
            Some(account_info) => {
                let bytecode = account_info
                    .code
                    .ok_or(EvmExecutionError::MissingBytecode)?;
                bytecode.bytes_ref().to_vec()
            }
        })
    }

    /// Sets the EVM contract address from the value Address::ZERO.
    /// The value is set from the `ApplicationId`.
    pub fn set_contract_address(&mut self) -> Result<(), ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let application_id = runtime.application_id()?;
        self.contract_address = application_id.evm_address();
        Ok(())
    }

    /// A contract is called initialized if the execution of the constructor
    /// with the constructor argument yield the storage and the deployed
    /// bytecode. The deployed bytecode is stored in the storage of the
    /// bytecode address.
    /// We determine whether the contract is already initialized, sets the
    /// `is_revm_initialized` and then returns the result.
    pub fn set_is_initialized(&mut self) -> Result<bool, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let evm_address = runtime.application_id()?.evm_address();
        let key_info = Self::get_address_key(KeyCategory::AccountInfo as u8, evm_address);
        let promise = runtime.contains_key_new(key_info)?;
        let result = runtime.contains_key_wait(&promise)?;
        self.is_revm_instantiated = result;
        Ok(result)
    }

    pub fn get_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
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
        // The base fee is the minimum fee for executing a transaction.
        // We have no such concept in Linera.
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
        let mut runtime = self.runtime.lock().unwrap();
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
        let mut runtime = self.runtime.lock().unwrap();
        // We use the gas_limit from the runtime
        let gas_limit = runtime.maximum_fuel_per_block(VmRuntime::Evm)?;
        block_env.gas_limit = gas_limit;
        Ok(block_env)
    }

    pub fn deposit_funds(&self) -> Result<(), ExecutionError> {
        if self.value != U256::ZERO {
            if self.caller == ZERO_ADDRESS {
                let error = EvmExecutionError::UnknownSigner;
                return Err(error.into());
            }
            let source = self.caller.into();
            let amount = Amount::try_from(self.value).map_err(EvmExecutionError::from)?;
            let mut runtime = self.runtime.lock().expect("The lock should be possible");
            let chain_id = runtime.chain_id()?;
            let application_id = runtime.application_id()?;
            let owner = application_id.into();
            let destination = Account { chain_id, owner };
            let authenticated_caller = runtime.authenticated_caller_id()?;
            if authenticated_caller.is_none() {
                runtime.transfer(source, destination, amount)?;
            }
        }
        Ok(())
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
