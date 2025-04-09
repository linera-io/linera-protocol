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
    primitives::{
        keccak256,
        state::{Account, AccountInfo},
    },
    Database, DatabaseCommit, DatabaseRef,
};

use crate::{BaseRuntime, Batch, ContractRuntime, EvmExecutionError, ExecutionError, ViewError};

pub(crate) struct DatabaseRuntime<Runtime> {
    commit_error: Option<Arc<ExecutionError>>,
    pub runtime: Arc<Mutex<Runtime>>,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self {
            commit_error: self.commit_error.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

#[repr(u8)]
enum KeyTag {
    /// Key prefix for the storage of the zero contract.
    ZeroContractAddress,
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
    fn get_uint256_key(val: u8, index: U256) -> Result<Vec<u8>, ExecutionError> {
        let mut key = vec![val, KeyCategory::Storage as u8];
        bcs::serialize_into(&mut key, &index)?;
        Ok(key)
    }

    fn get_contract_address_key(&self, address: &Address) -> Option<u8> {
        if address == &Address::ZERO {
            return Some(KeyTag::ZeroContractAddress as u8);
        }
        if address == &Address::ZERO.create(0) {
            return Some(KeyTag::ContractAddress as u8);
        }
        None
    }

    pub fn new(runtime: Runtime) -> Self {
        Self {
            commit_error: None,
            runtime: Arc::new(Mutex::new(runtime)),
        }
    }

    fn throw_error(&self) -> Result<(), ExecutionError> {
        if let Some(error) = &self.commit_error {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::CommitError(error);
            return Err(ExecutionError::EvmError(error));
        }
        Ok(())
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
        self.throw_error()?;
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

impl<Runtime> DatabaseCommit for DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        let result = self.commit_with_error(changes);
        if let Err(error) = result {
            self.commit_error = Some(error.into());
        }
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    fn commit_with_error(
        &mut self,
        changes: HashMap<Address, Account>,
    ) -> Result<(), ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let mut batch = Batch::new();
        let mut list_new_balances = Vec::new();
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }
            let val = self.get_contract_address_key(&address);
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
                    for (index, value) in account.storage {
                        let key = Self::get_uint256_key(val, index)?;
                        batch.put_key_value(key, &value.present_value())?;
                    }
                }
            } else {
                if !account.storage.is_empty() {
                    panic!("For user account, storage must be empty");
                }
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
        Ok(())
    }
}

impl<Runtime> DatabaseRef for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.throw_error()?;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let val = self.get_contract_address_key(&address);
        if let Some(val) = val {
            let key = vec![val, KeyCategory::AccountInfo as u8];
            let promise = runtime.read_value_bytes_new(key)?;
            let result = runtime.read_value_bytes_wait(&promise)?;
            let account_info = from_bytes_option::<AccountInfo, ViewError>(&result)?;
            Ok(account_info)
        } else {
            Ok(Some(AccountInfo::default()))
        }
    }

    fn code_by_hash_ref(
        &self,
        _code_hash: B256,
    ) -> Result<revm::primitives::Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash_ref not implemented");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.throw_error()?;
        let val = self.get_contract_address_key(&address);
        let Some(val) = val else {
            panic!("There is no storage associated to Externally Owned Account");
        };
        let key = Self::get_uint256_key(val, index)?;
        let result = {
            let mut runtime = self.runtime.lock().expect("The lock should be possible");
            let promise = runtime.read_value_bytes_new(key)?;
            runtime.read_value_bytes_wait(&promise)
        }?;
        Ok(from_bytes_option::<U256, ViewError>(&result)?.unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, ExecutionError> {
        self.throw_error()?;
        Ok(keccak256(number.to_string().as_bytes()))
    }
}
