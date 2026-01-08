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
use revm_database::{AccountState, DBErrorMarker};
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

/// This is the database common to the `ContractDatabase<Runtime>` and
/// `ServiceDatabase<Runtime>`. It encapsulates the main data of the
/// query, the contract address, who calls the contract with which value.
/// It also store the error if one occurs, the access to the runtime.
pub(crate) struct InnerDatabase<Runtime> {
    /// This is the EVM address of the contract.
    /// At the creation, it is set to `Address::ZERO` and then later set to the correct value.
    pub contract_address: Address,
    /// The caller to the smart contract.
    pub caller: Address,
    /// The value of the call to the smart contract.
    pub value: U256,
    /// The runtime of the contract.
    pub runtime: Arc<Mutex<Runtime>>,
    /// The uncommitted changes to the contract. It is used only for service calls.
    pub changes: EvmState,
    /// Whether the contract has been instantiated in REVM.
    pub is_revm_instantiated: bool,
    /// The error that can occur during runtime.
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

/// Returns the tag associated with the contract.
fn get_address_key(category: KeyCategory) -> Vec<u8> {
    vec![category as u8]
}

impl<Runtime> InnerDatabase<Runtime>
where
    Runtime: BaseRuntime,
{
    /// Creates a new `DatabaseRuntime`.
    pub fn new(runtime: Runtime) -> Self {
        // We cannot acquire a lock on runtime here.
        // So, we set the contract_address to a default value
        // and update it later.
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

    pub fn lock_runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.runtime.lock().unwrap()
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

    /// Reads the account info from the storage for a contract A
    /// whose address is `address`.
    /// * The function `f` is accessing the state for an account
    ///   whose address is different from `contract_address` (e.g.
    ///   a contract created in the contract A or accessed by A,
    ///   e.g. ERC20).
    ///   For `ContractRuntime` and `ServiceRuntime` the method
    ///   to access is different. So, the function has to be
    ///   provided as argument.
    /// * The address in question being read.
    /// * Whether the contract is newly created or not. For newly
    ///   created contract, the balance is the one of REVM. For
    ///   existing contract, the balance has to be accessed from
    ///   Linera.
    ///
    /// For Externally Owned Accounts, the function is indeed
    /// called, but it does not access the storage. Instead it
    /// creates a default `AccountInfo` whose balance is computed
    /// from the one in Linera. This is the case both for the faucet
    /// and for other accounts.
    ///
    /// For the contract for which address == contract_address we
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
        let key_info = get_address_key(KeyCategory::AccountInfo);
        let promise = runtime.read_value_bytes_new(key_info)?;
        let result = runtime.read_value_bytes_wait(&promise)?;
        Ok(from_bytes_option::<AccountInfo>(&result)?)
    }

    /// Reads the state from the inner database.
    ///
    /// If changes is not empty, then it means that
    /// the contract has been instantiated for a
    /// service query. In that case we do not have
    /// any storage possible to access, just changes.
    ///
    /// In the other case, we access the storage directly
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
    /// an externally owned account(EOA).
    fn has_empty_storage(&self, address: Address) -> Result<bool, ExecutionError> {
        let application_id = address_to_user_application_id(address);
        let mut runtime = self.runtime.lock().unwrap();
        runtime.has_empty_storage(application_id)
    }

    /// Reads the starting balance
    fn get_start_balance(&self, address: Address) -> Result<U256, ExecutionError> {
        let mut runtime = self.runtime.lock().unwrap();
        let account_owner = address.into();
        let balance = runtime.read_owner_balance(account_owner)?;
        let balance: U256 = balance.into();
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
        Ok(if self.caller == address {
            balance + self.value
        } else if self.contract_address == address {
            assert!(
                balance >= self.value,
                "We should have balance >= self.value"
            );
            balance - self.value
        } else {
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
        let key_info = get_address_key(KeyCategory::AccountInfo);
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
        let key_prefix = get_address_key(KeyCategory::Storage);
        let key_info = get_address_key(KeyCategory::AccountInfo);
        let key_state = get_address_key(KeyCategory::AccountState);
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
                let account_state = from_bytes_option::<AccountState>(&result)?.unwrap_or_default();
                if account_state.is_storage_cleared() {
                    AccountState::StorageCleared
                } else {
                    AccountState::Touched
                }
            };
            batch.put_key_value(key_state, &account_state)?;
            for (index, value) in &account.storage {
                if value.present_value() != value.original_value() {
                    let key = get_storage_key(*index);
                    if value.original_value() == U256::ZERO {
                        batch.put_key_value(key, &value.present_value())?;
                    } else if value.present_value() == U256::ZERO {
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
    pub fn is_account_writable(&self, address: &Address, account: &revm_state::Account) -> bool {
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

    /// Creates a new contract. The account contains
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
    /// * Iterates over the entries of the map and creates the contract in the
    ///   right order.
    pub fn commit_changes(&mut self) -> Result<(), ExecutionError> {
        let changes = mem::take(&mut self.inner.changes);
        let mut balances = Vec::new();
        let map = mem::take(self.modules.lock().unwrap().deref_mut());
        let mut contracts_to_create = vec![None; map.len()];
        for (address, account) in changes {
            if self.is_account_writable(&address, &account) {
                let revm_balance = account.info.balance;
                if address == self.inner.contract_address {
                    self.commit_contract_changes(&account)?;
                } else {
                    let application_id = address_to_user_application_id(address);
                    if let Some((module_id, index)) = map.get(&application_id) {
                        contracts_to_create[*index as usize] = Some((address, account, *module_id));
                    } else {
                        self.commit_remote_contract(address, account)?;
                    }
                }
                balances.push((address, revm_balance));
            }
        }
        for entry in contracts_to_create {
            if let Some((address, account, module_id)) = entry {
                self.create_new_contract(address, account, module_id)?;
            } else {
                unreachable!();
            };
        }
        for (address, revm_balance) in balances {
            self.check_balance(address, revm_balance)?;
        }
        Ok(())
    }
}

#[repr(u8)]
pub enum KeyCategory {
    AccountInfo,
    AccountState,
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
            let map = self.modules.lock().unwrap();
            let application_id = address_to_user_application_id(address);
            map.contains_key(&application_id)
        };
        self.inner.read_basic_ref(
            InnerDatabase::<Runtime>::get_contract_account_info,
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
    /// Since we choose the first design, the "code_by_hash_ref" is not needed. There
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
