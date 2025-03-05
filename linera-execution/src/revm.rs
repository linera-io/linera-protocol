// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use alloy::primitives::{Address, B256, U256};
use linera_base::{data_types::Bytecode, ensure, identifiers::StreamName};
use linera_views::common::from_bytes_option;
use revm::{
    db::AccountState,
    primitives::{
        keccak256,
        state::{Account, AccountInfo},
        Bytes,
    },
    Database, DatabaseCommit, DatabaseRef, Evm,
};
use revm_primitives::{ExecutionResult, HaltReason, Log, Output, TxKind};
use thiserror::Error;
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, MeasureLatency as _,
    },
    prometheus::HistogramVec,
    std::sync::LazyLock,
};

use crate::{
    BaseRuntime, Batch, ContractRuntime, ContractSyncRuntimeHandle, EvmRuntime, ExecutionError,
    FinalizeContext, MessageContext, OperationContext, QueryContext, ServiceRuntime,
    ServiceSyncRuntimeHandle, UserContract, UserContractInstance, UserContractModule, UserService,
    UserServiceInstance, UserServiceModule, ViewError,
};

const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::revm::EXECUTE_MESSAGE_SELECTOR;

    // The function keccak256 is not const so we cannot build the execute_message
    // selector directly.
    #[test]
    fn check_execute_message_selector() {
        let selector = &keccak256("execute_message(bytes)".as_bytes())[..4];
        assert_eq!(selector, EXECUTE_MESSAGE_SELECTOR);
    }
}

#[cfg(with_metrics)]
static CONTRACT_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_contract_instantiation_latency",
        "Evm contract instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

#[cfg(with_metrics)]
static SERVICE_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_service_instantiation_latency",
        "Evm service instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

#[derive(Debug, Error)]
pub enum EvmExecutionError {
    #[error("Failed to load contract EVM module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service EVM module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[error("Commit error")]
    CommitError(String),
    #[error("It is illegal to call execute_message from an operation")]
    OperationCallExecuteMessage,
    #[error("The operation should contain the evm selector and so have length 4 or more")]
    TooShortOperation,
    #[error("Transact commit error")]
    TransactCommitError(String),
    #[error("The operation was reverted")]
    Revert {
        gas_used: u64,
        output: alloy::primitives::Bytes,
    },
    #[error("The operation was halted")]
    Halt { gas_used: u64, reason: HaltReason },
}

#[derive(Clone)]
pub enum EvmContractModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using the EVM module with the provided `contract_bytecode`.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(contract_bytecode).await,
        }
    }

    /// Creates a new [`EvmContractModule`] using the WebAssembly module in `contract_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        contract_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(contract_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadContractModule)?,
            runtime,
        )
        .await
    }
}

impl UserContractModule for EvmContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = CONTRACT_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserContractInstance = match self {
            #[cfg(with_revm)]
            EvmContractModule::Revm { module } => {
                Box::new(RevmContractInstance::prepare(module.to_vec(), runtime))
            }
        };

        Ok(instance)
    }
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmContractModule::Revm { module })
    }
}

/// A user service in a compiled EVM module.
#[derive(Clone)]
pub enum EvmServiceModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmServiceModule {
    /// Creates a new [`EvmServiceModule`] using the EVM module with the provided bytecode.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(service_bytecode).await,
        }
    }

    /// Creates a new [`EvmServiceModule`] using the EVM module in `service_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        service_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(service_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadServiceModule)?,
            runtime,
        )
        .await
    }
}

impl UserServiceModule for EvmServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = SERVICE_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserServiceInstance = match self {
            #[cfg(with_revm)]
            EvmServiceModule::Revm { module } => {
                Box::new(RevmServiceInstance::prepare(module.to_vec(), runtime))
            }
        };

        Ok(instance)
    }
}

impl EvmServiceModule {
    /// Creates a new [`EvmServiceModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmServiceModule::Revm { module })
    }
}

struct DatabaseRuntime<Runtime> {
    contract_address: Address,
    commit_error: Option<ExecutionError>,
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
            self.commit_error = Some(error);
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
                let new_balance = (address, account.info.balance);
                list_new_balances.push(new_balance);
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
            return Ok(account_info);
        }
        panic!("only contract address are supported thus far address={address:?}");
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
        let argument = serde_json::from_slice::<Vec<u8>>(&argument)?;
        let mut vec = self.module.clone();
        vec.extend_from_slice(&argument);
        let tx_data = Bytes::copy_from_slice(&vec);
        let (output, logs) = self.transact_commit_tx_data(Choice::Create, tx_data)?;
        let contract_address = self.db.contract_address;
        self.write_logs(&contract_address, logs)?;
        let Output::Create(_, Some(contract_address_used)) = output else {
            unreachable!("It is impossible for a Choice::Create to lead to an Output::Call");
        };
        assert_eq!(contract_address_used, contract_address);
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
        ensure!(
            operation.len() >= 4,
            ExecutionError::EvmError(EvmExecutionError::TooShortOperation)
        );
        ensure!(
            &operation[..4] != EXECUTE_MESSAGE_SELECTOR,
            ExecutionError::EvmError(EvmExecutionError::OperationCallExecuteMessage)
        );
        let tx_data = Bytes::copy_from_slice(&operation);
        let (output, logs) = self.transact_commit_tx_data(Choice::Call, tx_data)?;
        let contract_address = self.db.contract_address;
        self.write_logs(&contract_address, logs)?;
        let Output::Call(output) = output else {
            unreachable!("It is impossible for a Choice::Call to lead to an Output::Create");
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
            let error = EvmExecutionError::Revert { gas_used, output };
            Err(ExecutionError::EvmError(error))
        }
        ExecutionResult::Halt { gas_used, reason } => {
            let error = EvmExecutionError::Halt { gas_used, reason };
            Err(ExecutionError::EvmError(error))
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

        let result = evm.transact_commit().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })?;
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
        let argument: serde_json::Value = serde_json::from_slice(&argument)?;
        let argument = argument["query"].to_string();
        if let Some(residual) = argument.strip_prefix("\"mutation { v") {
            let operation = &residual[0..residual.len() - 3];
            let operation = hex::decode(operation).unwrap();
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.schedule_operation(operation)?;
            let answer = serde_json::json!({"data": ""});
            let answer = serde_json::to_vec(&answer).unwrap();
            return Ok(answer);
        }
        let argument = argument[10..argument.len() - 3].to_string();
        let argument = hex::decode(&argument).unwrap();
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

        let result_state = evm.transact().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })?;
        let (output, _logs) = process_execution_result(result_state.result)?;
        // We drop the logs since the "eth_call" execution does not return any log.
        let Output::Call(output) = output else {
            unreachable!("It is impossible for a Choice::Call to lead to a Output::Create");
        };
        let answer = output.as_ref().to_vec();
        let answer = hex::encode(&answer);
        let answer: serde_json::Value = serde_json::json!({"data": answer});
        let answer = serde_json::to_vec(&answer).unwrap();
        Ok(answer)
    }
}
