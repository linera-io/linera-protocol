// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.
//! Here we implement the Database traits of Revm.

use std::{
    collections::HashMap,
    mem,
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use linera_base::{
    data_types::Amount,
    ensure,
    identifiers::{self, ApplicationId, ModuleId},
    vm::{EvmInstantiation, EvmQuery, VmRuntime},
};
use linera_views::common::from_bytes_option;
use revm::{primitives::keccak256, Database, DatabaseCommit, DatabaseRef};
use revm_context::BlockEnv;
use revm_context_interface::block::BlobExcessGasAndPrice;
use revm_database::DBErrorMarker;
use revm_primitives::{address, Address, B256, KECCAK_EMPTY, U256};
use revm_state::{AccountInfo, Bytecode, EvmState};

use crate::{
    evm::{
        inputs::{FAUCET_ADDRESS, FAUCET_BALANCE, ZERO_ADDRESS},
        revm::{
            address_to_user_application_id, ALREADY_CREATED_CONTRACT_SELECTOR,
            COMMIT_CONTRACT_CHANGES_SELECTOR, GET_ACCOUNT_INFO_SELECTOR,
            GET_CONTRACT_STORAGE_SELECTOR, JSON_EMPTY_VECTOR,
        },
    },
    BaseRuntime, Batch, ContractRuntime, EvmExecutionError, ExecutionError, ServiceRuntime,
};

// The runtime costs are not available in service operations.
// We need to set a limit to gas usage in order to avoid blocking
// the validator.
// We set up the limit similarly to Infura to 20 million.
pub const EVM_SERVICE_GAS_LIMIT: u64 = 20_000_000;

impl DBErrorMarker for ExecutionError {}

/// Core database implementation shared by both contract and service execution modes.
///
/// This structure bridges Linera's storage layer with Revm's database requirements,
/// managing state for EVM contract execution. It is used as the foundation for both
/// `ContractDatabase` (mutable operations) and `ServiceDatabase` (read-only queries).
///
/// # Lifecycle
///
/// 1. Created with `contract_address` set to `Address::ZERO`
/// 2. Address updated via `set_contract_address()` once runtime is available
/// 3. Execution proceeds with proper address context
///
/// # Error Handling
///
/// Errors during database operations are captured in the `error` field rather than
/// immediately propagating, allowing Revm to complete its execution flow before
/// error handling occurs.
pub(crate) struct InnerDatabase<Runtime> {
    /// The EVM address of the contract being executed.
    ///
    /// Initially set to `Address::ZERO` and updated to the actual contract address
    /// derived from the Linera `ApplicationId` once the runtime becomes available.
    pub contract_address: Address,

    /// The address of the entity calling this smart contract.
    ///
    /// Corresponds to `msg.sender` in Solidity. May be an EOA address, another
    /// contract address, or `Address::ZERO` for contracts for which the caller
    /// does not have an EVM address or has not been authenticated.
    pub caller: Address,

    /// The amount of native tokens being transferred with this call.
    ///
    /// Corresponds to `msg.value` in Solidity.
    pub value: U256,

    /// The Linera runtime providing access to storage and system operations.
    ///
    /// Wrapped in `Arc<Mutex<>>` to allow shared access across database clones
    /// while maintaining safe mutation.
    pub runtime: Arc<Mutex<Runtime>>,

    /// Uncommitted state changes from EVM execution.
    ///
    /// For contract operations, accumulated during execution and committed at the end.
    /// For service queries on uninitialized contracts, holds the temporary state
    /// since persistent storage is not available.
    pub changes: EvmState,

    /// Whether the contract has been instantiated in Revm's execution context.
    ///
    /// A contract may be instantiated in Linera (storage allocated) but not yet
    /// in Revm (constructor not executed). This flag tracks the Revm state.
    pub is_revm_instantiated: bool,

    /// Runtime errors captured during database operations.
    ///
    /// Errors are stored here rather than immediately returned to allow Revm
    /// to complete its execution flow. Checked via `process_any_error()` after
    /// execution completes.
    pub error: Arc<Mutex<Option<String>>>,
}

impl<Runtime> Clone for InnerDatabase<Runtime> {
    fn clone(&self) -> Self {
        Self {
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

/// Encodes the `index` of the EVM storage associated with the smart contract
/// in a Linera key.
fn get_storage_key(index: U256) -> Vec<u8> {
    let mut key = vec![KeyCategory::Storage as u8];
    key.extend(index.as_le_slice());
    key
}

/// Returns the storage key for a given category.
fn get_category_key(category: KeyCategory) -> Vec<u8> {
    vec![category as u8]
}

impl<Runtime> InnerDatabase<Runtime>
where
    Runtime: BaseRuntime,
{
    /// Creates a new database instance with default values.
    ///
    /// The contract address is initially set to `Address::ZERO` and must be updated
    /// later via `set_contract_address()` once the runtime can be safely accessed.
    /// This deferred initialization is necessary because locking the runtime during
    /// construction is not possible in the use cases of this code.
    pub fn new(runtime: Runtime) -> Self {
        Self {
            contract_address: Address::ZERO,
            caller: Address::ZERO,
            value: U256::ZERO,
            runtime: Arc::new(Mutex::new(runtime)),
            changes: HashMap::new(),
            is_revm_instantiated: false,
            error: Arc::new(Mutex::new(None)),
        }
    }

    /// Acquires exclusive access to the runtime.
    ///
    /// # Returns
    ///
    /// A mutex guard providing mutable access to the runtime. The lock is
    /// automatically released when the guard goes out of scope.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned (another thread panicked while holding the lock).
    /// This should not occur in normal operation as the runtime is not shared across threads.
    pub fn lock_runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.runtime.lock().unwrap()
    }

    /// Captures an execution error for later processing.
    ///
    /// Errors are stored rather than immediately returned to allow Revm to complete
    /// its execution flow. The error can be checked later via `process_any_error()`.
    pub fn insert_error(&self, exec_error: ExecutionError) {
        let mut error = self.error.lock().unwrap();
        *error = Some(format!("Runtime error {:?}", exec_error));
    }

    /// Checks for and returns any captured errors.
    ///
    /// This should be called after Revm execution completes to handle any errors
    /// that were captured during database operations.
    ///
    /// # Errors
    ///
    /// Returns an error if one was previously captured via `insert_error()`.
    pub fn process_any_error(&self) -> Result<(), EvmExecutionError> {
        let error = self.error.lock().unwrap();
        if let Some(error) = error.clone() {
            return Err(EvmExecutionError::RuntimeError(error.clone()));
        }
        Ok(())
    }

    /// Reads the account info from the storage for a contract A
    /// whose address is `address`.
    /// * The function `f` is accessing the state for an account
    ///   whose address is different from `contract_address` (e.g.
    ///   a contract created in the contract A or accessed by A,
    ///   e.g. ERC20).
    ///   For `ContractRuntime` and `ServiceRuntime` the method
    ///   to access is different. So, the function has to be
    ///   provided as argument.
    /// * `address`: The address being read.
    /// * `is_newly_created`: Whether the contract is newly created
    ///   or not. For newly created contract, the balance is the
    ///   one of Revm. For existing contract, the balance has to
    ///   be accessed from Linera.
    ///
    /// For Externally Owned Accounts, the function is indeed
    /// called, but it does not access the storage. Instead it
    /// creates a default `AccountInfo` whose balance is computed
    /// from the one in Linera. This is the case both for the faucet
    /// and for other accounts.
    ///
    /// For the contract for which `address == contract_address` we
    /// access the `AccountInfo` locally from the storage. For other
    /// contracts we need to access other contracts (with the
    /// function `f`)
    fn read_basic_ref(
        &self,
        f: fn(&Self, Address) -> Result<Option<AccountInfo>, ExecutionError>,
        address: Address,
        is_newly_created: bool,
    ) -> Result<Option<AccountInfo>, ExecutionError> {
        if address == FAUCET_ADDRESS {
            return Ok(Some(AccountInfo {
                balance: FAUCET_BALANCE,
                ..AccountInfo::default()
            }));
        }
        let mut account_info = self
            .account_info_from_storage(f, address)?
            .unwrap_or_default();
        if !is_newly_created {
            // For EOA and old contract the balance comes from Linera.
            account_info.balance = self.get_start_balance(address)?;
        }
        // We return an account as there is no difference between
        // a default account and the absence of account.
        Ok(Some(account_info))
    }

    /// Reads the state from the local storage.
    fn account_info_from_local_storage(&self) -> Result<Option<AccountInfo>, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let key_info = get_category_key(KeyCategory::AccountInfo);
        let promise = runtime.read_value_bytes_new(key_info)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        Ok(from_bytes_option::<AccountInfo>(&result)?)
    }

    /// Reads the state from the inner database.
    ///
    /// If `changes` is not empty, then it means that
    /// the contract has been instantiated for a
    /// service query. In that case we do not have
    /// any storage possible to access, just changes.
    ///
    /// In the other case, we access the storage directly.
    fn account_info_from_inner_database(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, ExecutionError> {
        if !self.changes.is_empty() {
            // This case occurs in only one scenario:
            // * A service call to a contract that has not yet been
            //   initialized by a contract call.
            // When we do a service calls to a contract that has
            // already been initialized, then changes will be empty.
            let account = self.changes.get(&address);
            return Ok(account.map(|account| account.info.clone()));
        }
        if address == self.contract_address {
            // This is for the contract and its storage.
            self.account_info_from_local_storage()
        } else {
            // This matches EOA and other contracts.
            Ok(None)
        }
    }

    /// Reads the state from local contract storage.
    fn account_info_from_storage(
        &self,
        f: fn(&Self, Address) -> Result<Option<AccountInfo>, ExecutionError>,
        address: Address,
    ) -> Result<Option<AccountInfo>, ExecutionError> {
        let account_info = self.account_info_from_inner_database(address)?;
        if let Some(account_info) = account_info {
            // This matches service calls or the contract itself.
            return Ok(Some(account_info));
        }
        if self.has_empty_storage(address)? {
            // This matches EOA
            Ok(None)
        } else {
            // This matches other EVM contracts.
            f(self, address)
        }
    }

    /// Returns whether the address has empty storage.
    /// An address has an empty storage if and only if it is
    /// an externally owned account (EOA).
    fn has_empty_storage(&self, address: Address) -> Result<bool, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let mut runtime = self.runtime.lock().unwrap();
        runtime.has_empty_storage(application_id)
    }

    /// Reads the starting balance for an account, adjusting for double-transfer prevention.
    ///
    /// # Balance Adjustment Logic
    ///
    /// To prevent double-transfers, balances are adjusted based on the account role:
    ///
    /// 1. **Execution Flow:**
    ///    - `deposit_funds()` transfers value from caller to contract (Linera layer)
    ///    - Account balances are read (this function)
    ///    - Revm performs its own transfer during execution
    ///
    /// 2. **Adjustments:**
    ///    - **Caller:** Balance increased by `self.value` (compensates for pre-transfer)
    ///    - **Contract:** Balance decreased by `self.value` (compensates for pre-receipt)
    ///    - **Others:** Balance unchanged
    ///
    /// This ensures Revm sees the correct post-`deposit_funds` state when it
    /// performs its transfer, avoiding double-counting.
    fn get_start_balance(&self, address: Address) -> Result<U256, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let account_owner = address.into();
        let balance = runtime.read_owner_balance(account_owner)?;
        let balance: U256 = balance.into();

        Ok(if self.caller == address {
            // Caller has already transferred funds, so we add them back
            balance + self.value
        } else if self.contract_address == address {
            // Contract has already received funds, so we subtract them
            assert!(
                balance >= self.value,
                "Contract balance should be >= transferred value"
            );
            balance - self.value
        } else {
            // Other accounts are unaffected
            balance
        })
    }

    pub fn get_account_info(&self) -> Result<AccountInfo, ExecutionError> {
        let address = self.contract_address;
        let account_info = self.account_info_from_inner_database(address)?;
        let mut account_info = account_info.ok_or(EvmExecutionError::MissingAccountInfo)?;
        account_info.balance = self.get_start_balance(address)?;
        Ok(account_info)
    }

    /// Reads the storage entry.
    /// * The function `f` is about accessing a storage value.
    ///   The function varies for `Contract` and `Service`.
    /// * The `address` and `index` are the one of the query.
    fn read_storage(
        &self,
        f: fn(&Self, Address, U256) -> Result<U256, ExecutionError>,
        address: Address,
        index: U256,
    ) -> Result<U256, ExecutionError> {
        if !self.changes.is_empty() {
            // This is the case of a contract instantiated for a service call.
            // The storage values are accessed there.
            let account = self.changes.get(&address).unwrap();
            return Ok(match account.storage.get(&index) {
                None => U256::ZERO,
                Some(slot) => slot.present_value(),
            });
        }
        if address == self.contract_address {
            // In that case we access the value from the
            // local storage.
            return self.read_from_local_storage(index);
        }
        // Use the function for accessing the value.
        f(self, address, index)
    }

    /// Reads the value from the local storage.
    pub fn read_from_local_storage(&self, index: U256) -> Result<U256, ExecutionError> {
        let key = get_storage_key(index);
        let mut runtime = self.runtime.lock().unwrap();
        let promise = runtime.read_value_bytes_new(key)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        Ok(from_bytes_option::<U256>(&result)?.unwrap_or_default())
    }

    /// Initializes the contract address from the Linera `ApplicationId`.
    ///
    /// During database construction, the contract address is set to `Address::ZERO`
    /// because the runtime cannot be locked at that time. This method updates it to
    /// the actual EVM address derived from the Linera `ApplicationId`.
    ///
    /// # Errors
    ///
    /// Returns an error if some step fails.
    pub fn set_contract_address(&mut self) -> Result<(), ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let application_id = runtime.application_id()?;
        self.contract_address = application_id.evm_address();
        Ok(())
    }

    /// Checks whether the contract has been initialized in Revm and updates the flag.
    ///
    /// A contract is considered Revm-initialized if the constructor has been executed,
    /// producing both the deployed bytecode and initial storage state. This is distinct
    /// from Linera initialization, which only allocates storage without executing the
    /// constructor.
    ///
    /// The initialization status is determined by checking for the presence of
    /// `AccountInfo` in storage, which is written only after successful constructor
    /// execution.
    ///
    /// # Returns
    ///
    /// Returns `true` if the contract is initialized (has `AccountInfo` in storage),
    /// `false` otherwise. Also updates `self.is_revm_instantiated` with the result.
    ///
    /// # Errors
    ///
    /// Returns an error if some step fails.
    pub fn set_is_initialized(&mut self) -> Result<bool, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let key_info = get_category_key(KeyCategory::AccountInfo);
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

impl<Runtime> InnerDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    /// Gets the smart contract code if existing.
    fn get_contract_account_info(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let argument = GET_ACCOUNT_INFO_SELECTOR.to_vec();
        let mut runtime = self.runtime.lock().unwrap();
        let account_info = runtime.try_call_application(false, application_id, argument)?;
        let account_info = bcs::from_bytes(&account_info)?;
        Ok(Some(account_info))
    }

    /// Gets the storage value of another contract.
    fn get_contract_storage_value(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let mut argument = GET_CONTRACT_STORAGE_SELECTOR.to_vec();
        argument.extend(bcs::to_bytes(&index)?);
        let mut runtime = self.runtime.lock().unwrap();
        let value = runtime.try_call_application(false, application_id, argument)?;
        let value = bcs::from_bytes(&value)?;
        Ok(value)
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
            let destination = identifiers::Account { chain_id, owner };
            let authenticated_caller = runtime.authenticated_caller_id()?;
            if authenticated_caller.is_none() {
                runtime.transfer(source, destination, amount)?;
            }
        }
        Ok(())
    }
}

impl<Runtime> InnerDatabase<Runtime>
where
    Runtime: ServiceRuntime,
{
    /// Gets the account info via a service query.
    fn get_service_account_info(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let argument = serde_json::to_vec(&EvmQuery::AccountInfo)?;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let account_info = runtime.try_query_application(application_id, argument)?;
        let account_info = serde_json::from_slice::<AccountInfo>(&account_info)?;
        Ok(Some(account_info))
    }

    /// Gets the storage value by doing a storage service query.
    fn get_service_storage_value(
        &self,
        address: Address,
        index: U256,
    ) -> Result<U256, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let argument = serde_json::to_vec(&EvmQuery::Storage(index))?;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let value = runtime.try_query_application(application_id, argument)?;
        let value = serde_json::from_slice::<U256>(&value)?;
        Ok(value)
    }
}

impl<Runtime> ContractDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn new(runtime: Runtime) -> Self {
        Self {
            inner: InnerDatabase::new(runtime),
            modules: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn lock_runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.inner.lock_runtime()
    }

    /// Balances of the contracts have to be checked when
    /// writing. There is a balance in Linera and a balance
    /// in EVM and they have to be coherent.
    fn check_balance(
        &mut self,
        address: Address,
        revm_balance: U256,
    ) -> Result<(), ExecutionError> {
        let mut runtime = self.inner.runtime.lock().unwrap();
        let owner = address.into();
        let linera_balance: U256 = runtime.read_owner_balance(owner)?.into();
        ensure!(
            linera_balance == revm_balance,
            EvmExecutionError::IncoherentBalances(address, linera_balance, revm_balance)
        );
        Ok(())
    }

    /// Effectively commits changes to storage.
    pub fn commit_contract_changes(
        &mut self,
        account: &revm_state::Account,
    ) -> Result<(), ExecutionError> {
        let mut runtime = self.inner.runtime.lock().unwrap();
        let mut batch = Batch::new();
        let key_prefix = get_category_key(KeyCategory::Storage);
        let key_info = get_category_key(KeyCategory::AccountInfo);
        if account.is_selfdestructed() {
            batch.delete_key_prefix(key_prefix);
            batch.put_key_value(key_info, &AccountInfo::default())?;
        } else {
            batch.put_key_value(key_info, &account.info)?;
            for (index, value) in &account.storage {
                if value.present_value() != value.original_value() {
                    let key = get_storage_key(*index);
                    if value.present_value() == U256::ZERO {
                        batch.delete_key(key);
                    } else {
                        batch.put_key_value(key, &value.present_value())?;
                    }
                }
            }
        }
        runtime.write_batch(batch)?;
        Ok(())
    }

    /// Returns whether the account is writable.
    /// We do not write the accounts of Externally Owned Accounts.
    fn is_account_writable(&self, address: &Address, account: &revm_state::Account) -> bool {
        if *address == FAUCET_ADDRESS {
            // We do not write the faucet address nor expect any coherency from it.
            return false;
        }
        if !account.is_touched() {
            // Not modified accounts do not need to be written down.
            return false;
        }
        let code_hash = account.info.code_hash;
        // User accounts are not written. This is fine since the balance
        // is accessed from Linera and the nonce are not accessible in
        // EVM smart contracts.
        let code_empty = code_hash == KECCAK_EMPTY || code_hash.is_zero();
        !code_empty
    }

    /// Whether the balance of this account needs to be checked.
    fn is_account_checkable(&self, address: &Address) -> bool {
        if *address == FAUCET_ADDRESS {
            // We do not check the FAUCET balance.
            return false;
        }
        true
    }

    /// Creates a new contract. The `account` contains
    /// the AccountInfo and the storage to be written.
    /// The parameters is empty because the constructor
    /// does not need to be concatenated as it has
    /// already been concatenated to the bytecode in the
    /// init_code.
    fn create_new_contract(
        &mut self,
        address: Address,
        account: revm_state::Account,
        module_id: ModuleId,
    ) -> Result<(), ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let mut argument = ALREADY_CREATED_CONTRACT_SELECTOR.to_vec();
        argument.extend(bcs::to_bytes(&account)?);
        let evm_instantiation = EvmInstantiation {
            value: U256::ZERO,
            argument,
        };
        let argument = serde_json::to_vec(&evm_instantiation)?;
        let parameters = JSON_EMPTY_VECTOR.to_vec(); // No constructor
        let required_application_ids = Vec::new();
        let mut runtime = self.inner.runtime.lock().unwrap();
        let created_application_id = runtime.create_application(
            module_id,
            parameters,
            argument,
            required_application_ids,
        )?;
        ensure!(
            application_id == created_application_id,
            EvmExecutionError::IncorrectApplicationId
        );
        Ok(())
    }

    /// Commits the changes to another contract.
    /// This is done by doing a call application.
    fn commit_remote_contract(
        &mut self,
        address: Address,
        account: revm_state::Account,
    ) -> Result<(), ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let mut argument = COMMIT_CONTRACT_CHANGES_SELECTOR.to_vec();
        argument.extend(bcs::to_bytes(&account)?);
        let mut runtime = self.inner.runtime.lock().unwrap();
        runtime.try_call_application(false, application_id, argument)?;
        Ok(())
    }

    /// Effectively commits changes to storage.
    /// This is done in the following way:
    /// * Identify the balances that need to be checked
    /// * Write down the state of the contract for `contract_address` locally.
    /// * For the other contracts, if it already created, commit it.
    ///
    /// If not insert them into the map.
    /// * Iterates over the entries of the map and creates the contracts in the
    ///   right order.
    pub fn commit_changes(&mut self) -> Result<(), ExecutionError> {
        let changes = mem::take(&mut self.inner.changes);
        let mut balances = Vec::new();
        let modules = mem::take(self.modules.lock().unwrap().deref_mut());
        let mut contracts_to_create = vec![None; modules.len()];
        for (address, account) in changes {
            if self.is_account_checkable(&address) {
                let revm_balance = account.info.balance;
                balances.push((address, revm_balance));
            }
            if self.is_account_writable(&address, &account) {
                if address == self.inner.contract_address {
                    self.commit_contract_changes(&account)?;
                } else {
                    let application_id = address_to_user_application_id(address);
                    if let Some((module_id, index)) = modules.get(&application_id) {
                        contracts_to_create[*index as usize] = Some((address, account, *module_id));
                    } else {
                        self.commit_remote_contract(address, account)?;
                    }
                }
            }
        }
        for entry in contracts_to_create {
            let (address, account, module_id) =
                entry.expect("An entry since all have been matched above");
            self.create_new_contract(address, account, module_id)?;
        }
        for (address, revm_balance) in balances {
            self.check_balance(address, revm_balance)?;
        }
        Ok(())
    }
}

/// Categories for organizing different types of data in the storage.
///
/// Each category is prefixed with a unique byte when encoding storage keys,
/// allowing different types of data to coexist without key collisions.
#[repr(u8)]
pub enum KeyCategory {
    /// Account information including code hash, nonce, and balance.
    AccountInfo,
    /// Contract storage values indexed by U256 keys.
    Storage,
}

/// The Database for contracts
pub(crate) struct ContractDatabase<Runtime> {
    pub inner: InnerDatabase<Runtime>,
    pub modules: Arc<Mutex<HashMap<ApplicationId, (ModuleId, u32)>>>,
}

impl<Runtime> Clone for ContractDatabase<Runtime> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            modules: self.modules.clone(),
        }
    }
}

impl<Runtime> DatabaseRef for ContractDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    type Error = ExecutionError;

    /// The `basic_ref` is the function for reading the state of the application.
    /// The code `read_basic_ref` is used with the relevant access function.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        let is_newly_created = {
            let modules = self.modules.lock().unwrap();
            let application_id = address_to_user_application_id(address);
            modules.contains_key(&application_id)
        };
        self.inner.read_basic_ref(
            InnerDatabase::<Runtime>::get_contract_account_info,
            address,
            is_newly_created,
        )
    }

    /// There are two ways to implement the trait:
    /// * Returns entries with "code: Some(...)"
    /// * Returns entries with "code: None".
    ///
    /// Since we choose the first design, `code_by_hash_ref` is not needed. There
    /// is an example in the Revm source code of this kind.
    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
        panic!("Returned AccountInfo should have code: Some(...) and so code_by_hash_ref should never be called");
    }

    /// Accesses the storage by the relevant remote access function.
    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.inner.read_storage(
            InnerDatabase::<Runtime>::get_contract_storage_value,
            address,
            index,
        )
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, ExecutionError> {
        Ok(keccak256(number.to_string().as_bytes()))
    }
}

impl<Runtime> Database for ContractDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    type Error = ExecutionError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
        panic!("Returned AccountInfo should have code: Some(...) and so code_by_hash should never be called");
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, ExecutionError> {
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

impl<Runtime> DatabaseCommit for ContractDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    fn commit(&mut self, changes: EvmState) {
        self.inner.changes = changes;
    }
}

impl<Runtime> ContractDatabase<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn get_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut block_env = self.inner.get_block_env()?;
        let mut runtime = self.inner.runtime.lock().unwrap();
        // We use the gas_limit from the runtime
        let gas_limit = runtime.maximum_fuel_per_block(VmRuntime::Evm)?;
        block_env.gas_limit = gas_limit;
        Ok(block_env)
    }

    /// Reads the nonce of the user
    pub fn get_nonce(&self, address: &Address) -> Result<u64, ExecutionError> {
        let account_info = self.basic_ref(*address)?;
        Ok(match account_info {
            None => 0,
            Some(account_info) => account_info.nonce,
        })
    }
}

// The Database for service

pub(crate) struct ServiceDatabase<Runtime> {
    pub inner: InnerDatabase<Runtime>,
}

impl<Runtime> Clone for ServiceDatabase<Runtime> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Runtime> DatabaseRef for ServiceDatabase<Runtime>
where
    Runtime: ServiceRuntime,
{
    type Error = ExecutionError;

    /// The `basic_ref` is the function for reading the state of the application.
    /// There is no newly created contracts for services.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        let is_newly_created = false; // No contract creation in service
        self.inner.read_basic_ref(
            InnerDatabase::<Runtime>::get_service_account_info,
            address,
            is_newly_created,
        )
    }

    /// There are two ways to implements the trait:
    /// * Returns entries with "code: Some(...)"
    /// * Returns entries with "code: None".
    ///
    /// Since we choose the first design, `code_by_hash_ref` is not needed. There
    /// is an example in the Revm source code of this kind.
    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
        panic!("Returned AccountInfo should have code: Some(...) and so code_by_hash_ref should never be called");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.inner.read_storage(
            InnerDatabase::<Runtime>::get_service_storage_value,
            address,
            index,
        )
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, ExecutionError> {
        Ok(keccak256(number.to_string().as_bytes()))
    }
}

impl<Runtime> Database for ServiceDatabase<Runtime>
where
    Runtime: ServiceRuntime,
{
    type Error = ExecutionError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
        panic!("Returned AccountInfo should have code: Some(...) and so code_by_hash should never be called");
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, ExecutionError> {
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

impl<Runtime> DatabaseCommit for ServiceDatabase<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn commit(&mut self, changes: EvmState) {
        self.inner.changes = changes;
    }
}

impl<Runtime> ServiceDatabase<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn new(runtime: Runtime) -> Self {
        Self {
            inner: InnerDatabase::new(runtime),
        }
    }

    pub fn lock_runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.inner.lock_runtime()
    }

    pub fn get_block_env(&self) -> Result<BlockEnv, ExecutionError> {
        let mut block_env = self.inner.get_block_env()?;
        block_env.gas_limit = EVM_SERVICE_GAS_LIMIT;
        Ok(block_env)
    }

    /// Reads the nonce of the user
    pub fn get_nonce(&self, address: &Address) -> Result<u64, ExecutionError> {
        let account_info = self.basic_ref(*address)?;
        Ok(match account_info {
            None => 0,
            Some(account_info) => account_info.nonce,
        })
    }
}
