// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use alloy::primitives::{Address, B256, U256};
use linera_base::identifiers::StreamName;
use linera_views::common::from_bytes_option;
use revm::{
    db::AccountState,
    primitives::{
        keccak256,
        state::{Account, AccountInfo},
        Bytecode, Bytes,
    },
    Database, DatabaseCommit, DatabaseRef, Evm,
};
use revm_primitives::{ExecutionResult, Log, Output, TxKind};

use crate::{
    wasm::RevmExecutionError, BaseRuntime, Batch, ContractRuntime, ExecutionError, FinalizeContext,
    MessageContext, OperationContext, QueryContext, ServiceRuntime, UserContract, UserService,
    ViewError, VmContractModule, VmExecutionError, VmServiceModule,
};

impl VmContractModule {
    /// Creates a new [`VmContractModule`] using Revm with the provided bytecodes.
    pub async fn from_revm(
        contract_bytecode: linera_base::data_types::Bytecode,
    ) -> Result<Self, VmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(VmContractModule::Revm { module })
    }
}

impl VmServiceModule {
    /// Creates a new [`VmServiceModule`] using Revm with the provided bytecodes.
    pub async fn from_revm(
        contract_bytecode: linera_base::data_types::Bytecode,
    ) -> Result<Self, VmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(VmServiceModule::Revm { module })
    }
}

struct DatabaseRuntime<Runtime> {
    contract_address: Address,
    runtime: Arc<Mutex<Runtime>>,
}

#[repr(u8)]
pub enum KeyTag {
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
        if address == &self.contract_address {
            return Some(KeyTag::ContractAddress as u8);
        }
        None
    }

    fn new(runtime: Runtime) -> Self {
        let nonce = 0;
        let contract_address = Address::ZERO.create(nonce);
        Self {
            contract_address,
            runtime: Arc::new(Mutex::new(runtime)),
        }
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
    Runtime: ContractRuntime,
{
    fn commit(&mut self, changes: HashMap<Address, Account>) {
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
                    batch
                        .put_key_value(key_info, &AccountInfo::default())
                        .expect("no error propagation");
                    batch
                        .put_key_value(key_state, &AccountState::NotExisting)
                        .expect("no error propagation");
                } else {
                    let is_newly_created = account.is_created();
                    batch
                        .put_key_value(key_info, &account.info)
                        .expect("no error propagation");

                    let account_state = if is_newly_created {
                        batch.delete_key_prefix(key_prefix);
                        AccountState::StorageCleared
                    } else {
                        let promise = runtime
                            .read_value_bytes_new(key_state.clone())
                            .expect("no error propagation");
                        let result = runtime
                            .read_value_bytes_wait(&promise)
                            .expect("no error propagation");
                        let account_state = from_bytes_option::<AccountState, ViewError>(&result)
                            .expect("no error propagation")
                            .unwrap_or_default();
                        if account_state.is_storage_cleared() {
                            AccountState::StorageCleared
                        } else {
                            AccountState::Touched
                        }
                    };
                    batch
                        .put_key_value(key_state, &account_state)
                        .expect("no error propagation");
                    for (index, value) in account.storage {
                        let key = Self::get_uint256_key(val, index).expect("no error propagation");
                        batch
                            .put_key_value(key, &value.present_value())
                            .expect("no error propagation");
                    }
                }
            } else {
                if !account.storage.is_empty() {
                    panic!("For user account, storage must be empty");
                }
                // The only allowed operations are the ones for the
                // account balances.
                let new_balance = (address, account.info.balance);
                list_new_balances.push(new_balance);
            }
        }
        runtime
            .write_batch(batch)
            .expect("We have to way to handle error");
        if !list_new_balances.is_empty() {
            panic!("The conversion Ethereum address / Linera address is not yet implemented");
        }
    }
}

impl<Runtime> DatabaseRef for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let val = self.get_contract_address_key(&address);
        if let Some(val) = val {
            let key = vec![val, KeyCategory::AccountInfo as u8];
            let promise = runtime.read_value_bytes_new(key)?;
            let result = runtime.read_value_bytes_wait(&promise)?;
            let account_info = from_bytes_option::<AccountInfo, ViewError>(&result)?;
            return Ok(account_info);
        }
        panic!("only contract address are supported thus far address={address:?}");
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash_ref not implemented");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
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
        Ok(keccak256(number.to_string().as_bytes()))
    }
}

pub struct RevmContractInstance<Runtime> {
    module: Vec<u8>,
    db: DatabaseRuntime<Runtime>,
}

enum Choice {
    Create,
    Call,
}

// The OperationContext / MessageContext / FinalizeContext are not used
// in the wasmer / wasmtime. Should we used it? It seems
impl<Runtime> UserContract for RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn instantiate(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        let mut vec = self.module.clone();
        vec.extend_from_slice(&argument);
        let tx_data = Bytes::copy_from_slice(&vec);
        let (output, logs) = self.transact_commit_tx_data(Choice::Create, tx_data)?;
        let contract_address = self.db.contract_address;
        self.write_logs(&contract_address, logs)?;
        let Output::Create(_, Some(contract_address_used)) = output else {
            unreachable!("It is impossible for a Choice::Create to lead to a Output::Call");
        };
        assert_eq!(contract_address_used, contract_address);
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let tx_data = Bytes::copy_from_slice(&operation);
        let (output, logs) = self.transact_commit_tx_data(Choice::Call, tx_data)?;
        let contract_address = self.db.contract_address;
        self.write_logs(&contract_address, logs)?;
        let Output::Call(output) = output else {
            unreachable!("It is impossible for a Choice::Call to lead to a Output::Create");
        };
        let output = output.as_ref().to_vec();
        Ok(output)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        _message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        panic!("The execute_message part of the Ethereum smart contract has not yet been coded");
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        Ok(())
    }
}

fn process_execution_result(result: ExecutionResult) -> Result<(Output, Vec<Log>), ExecutionError> {
    match result {
        ExecutionResult::Success {
            reason: _,
            gas_used: _,
            gas_refunded: _,
            logs,
            output,
        } => Ok((output, logs)),
        ExecutionResult::Revert { gas_used, output } => {
            let error = RevmExecutionError::Revert { gas_used, output };
            let error = VmExecutionError::RevmExecutionError(error);
            Err(ExecutionError::VmError(error))
        }
        ExecutionResult::Halt { gas_used, reason } => {
            let error = RevmExecutionError::Halt { gas_used, reason };
            let error = VmExecutionError::RevmExecutionError(error);
            Err(ExecutionError::VmError(error))
        }
    }
}

impl<Runtime> RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn prepare(module: Vec<u8>, runtime: Runtime) -> Self {
        let db = DatabaseRuntime::new(runtime);
        Self { module, db }
    }

    fn transact_commit_tx_data(
        &mut self,
        ch: Choice,
        tx_data: Bytes,
    ) -> Result<(Output, Vec<Log>), ExecutionError> {
        let kind = match ch {
            Choice::Create => TxKind::Create,
            Choice::Call => TxKind::Call(self.db.contract_address),
        };
        let mut evm: Evm<'_, (), _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = kind;
                tx.data = tx_data;
            })
            .build();

        let result = evm.transact_commit();
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                let error = format!("{:?}", error);
                let error = RevmExecutionError::TransactCommitError(error);
                let error = VmExecutionError::RevmExecutionError(error);
                return Err(ExecutionError::VmError(error));
            }
        };
        process_execution_result(result)
    }

    fn write_logs(
        &mut self,
        contract_address: &Address,
        logs: Vec<Log>,
    ) -> Result<(), ExecutionError> {
        if !logs.is_empty() {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            let stream_name = bcs::to_bytes("ethereum_event").unwrap();
            let stream_name = StreamName(stream_name);
            for (log, index) in logs.iter().enumerate() {
                let mut key = bcs::to_bytes(&contract_address).unwrap();
                bcs::serialize_into(&mut key, "deploy").unwrap();
                bcs::serialize_into(&mut key, index).unwrap();
                let value = bcs::to_bytes(&log).unwrap();
                runtime.emit(stream_name.clone(), key, value)?;
            }
        }
        Ok(())
    }
}

pub struct RevmServiceInstance<Runtime> {
    db: DatabaseRuntime<Runtime>,
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn prepare(_module: Vec<u8>, runtime: Runtime) -> Self {
        let db = DatabaseRuntime::new(runtime);
        Self { db }
    }
}

impl<Runtime> UserService for RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        let tx_data = Bytes::copy_from_slice(&argument);
        let address = self.db.contract_address;
        let mut evm: Evm<'_, (), _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = TxKind::Call(address);
                tx.data = tx_data;
            })
            .build();

        let result = evm.transact();
        let result_state = match result {
            Ok(result_state) => result_state,
            Err(error) => {
                let error = format!("{:?}", error);
                let error = RevmExecutionError::TransactCommitError(error);
                let error = VmExecutionError::RevmExecutionError(error);
                return Err(ExecutionError::VmError(error));
            }
        };
        let (output, _logs) = process_execution_result(result_state.result)?;
        // We drop the logs since the "eth_call" execution does not return any log.
        let Output::Call(output) = output else {
            unreachable!("It is impossible for a Choice::Call to lead to a Output::Create");
        };
        Ok(output.as_ref().to_vec())
    }
}
