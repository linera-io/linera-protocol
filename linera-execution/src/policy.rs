// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module contains types related to fees and pricing.

use std::{collections::BTreeSet, fmt};

use async_graphql::InputObject;
use linera_base::{
    data_types::{Amount, ArithmeticError, BlobContent, CompressedBytecode, Resources},
    ensure,
    identifiers::BlobType,
    vm::VmRuntime,
};
use serde::{Deserialize, Serialize};

use crate::ExecutionError;

/// A collection of prices and limits associated with block execution.
#[derive(Eq, PartialEq, Hash, Clone, Debug, Serialize, Deserialize, InputObject)]
pub struct ResourceControlPolicy {
    /// The base price for creating a new block.
    pub block: Amount,
    /// The price per unit of fuel (aka gas) for Wasm execution.
    pub wasm_fuel_unit: Amount,
    /// The price per unit of fuel (aka gas) for EVM execution.
    pub evm_fuel_unit: Amount,
    /// The price of one read operation.
    pub read_operation: Amount,
    /// The price of one write operation.
    pub write_operation: Amount,
    /// The price of reading a byte.
    pub byte_read: Amount,
    /// The price of writing a byte
    pub byte_written: Amount,
    /// The base price to read a blob.
    pub blob_read: Amount,
    /// The base price to publish a blob.
    pub blob_published: Amount,
    /// The price to read a blob, per byte.
    pub blob_byte_read: Amount,
    /// The price to publish a blob, per byte.
    pub blob_byte_published: Amount,
    /// The price of increasing storage by a byte.
    // TODO(#1536): This is not fully supported.
    pub byte_stored: Amount,
    /// The base price of adding an operation to a block.
    pub operation: Amount,
    /// The additional price for each byte in the argument of a user operation.
    pub operation_byte: Amount,
    /// The base price of sending a message from a block.
    pub message: Amount,
    /// The additional price for each byte in the argument of a user message.
    pub message_byte: Amount,
    /// The price per query to a service as an oracle.
    pub service_as_oracle_query: Amount,
    /// The price for a performing an HTTP request.
    pub http_request: Amount,

    // TODO(#1538): Cap the number of transactions per block and the total size of their
    // arguments.
    /// The maximum amount of Wasm fuel a block can consume.
    pub maximum_wasm_fuel_per_block: u64,
    /// The maximum amount of EVM fuel a block can consume.
    pub maximum_evm_fuel_per_block: u64,
    /// The maximum time in milliseconds that a block can spend executing services as oracles.
    pub maximum_service_oracle_execution_ms: u64,
    /// The maximum size of a block. This includes the block proposal itself as well as
    /// the execution outcome.
    pub maximum_block_size: u64,
    /// The maximum size of decompressed contract or service bytecode, in bytes.
    pub maximum_bytecode_size: u64,
    /// The maximum size of a blob.
    pub maximum_blob_size: u64,
    /// The maximum number of published blobs per block.
    pub maximum_published_blobs: u64,
    /// The maximum size of a block proposal.
    pub maximum_block_proposal_size: u64,
    /// The maximum data to read per block
    pub maximum_bytes_read_per_block: u64,
    /// The maximum data to write per block
    pub maximum_bytes_written_per_block: u64,
    /// The maximum size in bytes of an oracle response.
    pub maximum_oracle_response_bytes: u64,
    /// The maximum size in bytes of a received HTTP response.
    pub maximum_http_response_bytes: u64,
    /// The maximum amount of time allowed to wait for an HTTP response.
    pub http_request_timeout_ms: u64,
    /// The list of hosts that contracts and services can send HTTP requests to.
    pub http_request_allow_list: BTreeSet<String>,
}

impl fmt::Display for ResourceControlPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ResourceControlPolicy {
            block,
            wasm_fuel_unit,
            evm_fuel_unit,
            read_operation,
            write_operation,
            byte_read,
            byte_written,
            blob_read,
            blob_published,
            blob_byte_read,
            blob_byte_published,
            byte_stored,
            operation,
            operation_byte,
            message,
            message_byte,
            service_as_oracle_query,
            http_request,
            maximum_wasm_fuel_per_block,
            maximum_evm_fuel_per_block,
            maximum_service_oracle_execution_ms,
            maximum_block_size,
            maximum_blob_size,
            maximum_published_blobs,
            maximum_bytecode_size,
            maximum_block_proposal_size,
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
            maximum_oracle_response_bytes,
            maximum_http_response_bytes,
            http_request_allow_list,
            http_request_timeout_ms,
        } = self;
        write!(
            f,
            "Resource control policy:\n\
            {block:.2} base cost per block\n\
            {wasm_fuel_unit:.2} cost per Wasm fuel unit\n\
            {evm_fuel_unit:.2} cost per EVM fuel unit\n\
            {read_operation:.2} cost per read operation\n\
            {write_operation:.2} cost per write operation\n\
            {byte_read:.2} cost per byte read\n\
            {byte_written:.2} cost per byte written\n\
            {blob_read:.2} base cost per read blob\n\
            {blob_published:.2} base cost per published blob\n\
            {blob_byte_read:.2} cost of reading blobs, per byte\n\
            {blob_byte_published:.2} cost of publishing blobs, per byte\n\
            {byte_stored:.2} cost per byte stored\n\
            {operation:.2} per operation\n\
            {operation_byte:.2} per byte in the argument of an operation\n\
            {service_as_oracle_query:.2} per query to a service as an oracle\n\
            {message:.2} per outgoing messages\n\
            {message_byte:.2} per byte in the argument of an outgoing messages\n\
            {http_request:.2} per HTTP request performed\n\
            {maximum_wasm_fuel_per_block} maximum Wasm fuel per block\n\
            {maximum_evm_fuel_per_block} maximum EVM fuel per block\n\
            {maximum_service_oracle_execution_ms} ms maximum service-as-oracle execution time per \
                block\n\
            {maximum_block_size} maximum size of a block\n\
            {maximum_blob_size} maximum size of a data blob, bytecode or other binary blob\n\
            {maximum_published_blobs} maximum number of blobs published per block\n\
            {maximum_bytecode_size} maximum size of service and contract bytecode\n\
            {maximum_block_proposal_size} maximum size of a block proposal\n\
            {maximum_bytes_read_per_block} maximum number of bytes read per block\n\
            {maximum_bytes_written_per_block} maximum number of bytes written per block\n\
            {maximum_oracle_response_bytes} maximum number of bytes of an oracle response\n\
            {maximum_http_response_bytes} maximum number of bytes of an HTTP response\n\
            {http_request_timeout_ms} ms timeout for HTTP requests\n\
            HTTP hosts allowed for contracts and services: {http_request_allow_list:#?}\n",
        )?;
        Ok(())
    }
}

impl Default for ResourceControlPolicy {
    fn default() -> Self {
        Self::no_fees()
    }
}

impl ResourceControlPolicy {
    /// Creates a policy with no cost for anything.
    ///
    /// This can be used in tests or benchmarks.
    pub fn no_fees() -> Self {
        Self {
            block: Amount::ZERO,
            wasm_fuel_unit: Amount::ZERO,
            evm_fuel_unit: Amount::ZERO,
            read_operation: Amount::ZERO,
            write_operation: Amount::ZERO,
            byte_read: Amount::ZERO,
            byte_written: Amount::ZERO,
            blob_read: Amount::ZERO,
            blob_published: Amount::ZERO,
            blob_byte_read: Amount::ZERO,
            blob_byte_published: Amount::ZERO,
            byte_stored: Amount::ZERO,
            operation: Amount::ZERO,
            operation_byte: Amount::ZERO,
            message: Amount::ZERO,
            message_byte: Amount::ZERO,
            service_as_oracle_query: Amount::ZERO,
            http_request: Amount::ZERO,
            maximum_wasm_fuel_per_block: u64::MAX,
            maximum_evm_fuel_per_block: u64::MAX,
            maximum_service_oracle_execution_ms: u64::MAX,
            maximum_block_size: u64::MAX,
            maximum_blob_size: u64::MAX,
            maximum_published_blobs: u64::MAX,
            maximum_bytecode_size: u64::MAX,
            maximum_block_proposal_size: u64::MAX,
            maximum_bytes_read_per_block: u64::MAX,
            maximum_bytes_written_per_block: u64::MAX,
            maximum_oracle_response_bytes: u64::MAX,
            maximum_http_response_bytes: u64::MAX,
            http_request_timeout_ms: u64::MAX,
            http_request_allow_list: BTreeSet::new(),
        }
    }

    /// The maximum fuel per block according to the `VmRuntime`.
    pub fn maximum_fuel_per_block(&self, vm_runtime: VmRuntime) -> u64 {
        match vm_runtime {
            VmRuntime::Wasm => self.maximum_wasm_fuel_per_block,
            VmRuntime::Evm => self.maximum_evm_fuel_per_block,
        }
    }

    /// Creates a policy with no cost for anything except fuel.
    ///
    /// This can be used in tests that need whole numbers in their chain balance.
    #[cfg(with_testing)]
    pub fn only_fuel() -> Self {
        Self {
            wasm_fuel_unit: Amount::from_micros(1),
            evm_fuel_unit: Amount::from_micros(1),
            ..Self::no_fees()
        }
    }

    /// Creates a policy with no cost for anything except fuel, and 0.001 per block.
    ///
    /// This can be used in tests, and that keep track of how many blocks were created.
    #[cfg(with_testing)]
    pub fn fuel_and_block() -> Self {
        Self {
            block: Amount::from_millis(1),
            wasm_fuel_unit: Amount::from_micros(1),
            evm_fuel_unit: Amount::from_micros(1),
            ..Self::no_fees()
        }
    }

    /// Creates a policy where all categories have a small non-zero cost.
    #[cfg(with_testing)]
    pub fn all_categories() -> Self {
        Self {
            block: Amount::from_millis(1),
            wasm_fuel_unit: Amount::from_nanos(1),
            evm_fuel_unit: Amount::from_nanos(1),
            byte_read: Amount::from_attos(100),
            byte_written: Amount::from_attos(1_000),
            blob_read: Amount::from_nanos(1),
            blob_published: Amount::from_nanos(10),
            blob_byte_read: Amount::from_attos(100),
            blob_byte_published: Amount::from_attos(1_000),
            operation: Amount::from_attos(10),
            operation_byte: Amount::from_attos(1),
            message: Amount::from_attos(10),
            message_byte: Amount::from_attos(1),
            http_request: Amount::from_micros(1),
            ..Self::no_fees()
        }
    }

    /// Creates a policy that matches the Testnet.
    pub fn testnet() -> Self {
        Self {
            block: Amount::from_millis(1),
            wasm_fuel_unit: Amount::from_nanos(10),
            evm_fuel_unit: Amount::from_nanos(10),
            byte_read: Amount::from_nanos(10),
            byte_written: Amount::from_nanos(100),
            blob_read: Amount::from_nanos(100),
            blob_published: Amount::from_nanos(1000),
            blob_byte_read: Amount::from_nanos(10),
            blob_byte_published: Amount::from_nanos(100),
            read_operation: Amount::from_micros(10),
            write_operation: Amount::from_micros(20),
            byte_stored: Amount::from_nanos(10),
            message_byte: Amount::from_nanos(100),
            operation_byte: Amount::from_nanos(10),
            operation: Amount::from_micros(10),
            message: Amount::from_micros(10),
            service_as_oracle_query: Amount::from_millis(10),
            http_request: Amount::from_micros(50),
            maximum_wasm_fuel_per_block: 100_000_000,
            maximum_evm_fuel_per_block: 100_000_000,
            maximum_service_oracle_execution_ms: 10_000,
            maximum_block_size: 1_000_000,
            maximum_blob_size: 1_000_000,
            maximum_published_blobs: 10,
            maximum_bytecode_size: 10_000_000,
            maximum_block_proposal_size: 13_000_000,
            maximum_bytes_read_per_block: 100_000_000,
            maximum_bytes_written_per_block: 10_000_000,
            maximum_oracle_response_bytes: 10_000,
            maximum_http_response_bytes: 10_000,
            http_request_timeout_ms: 20_000,
            http_request_allow_list: BTreeSet::new(),
        }
    }

    pub fn block_price(&self) -> Amount {
        self.block
    }

    pub fn total_price(&self, resources: &Resources) -> Result<Amount, ArithmeticError> {
        let mut amount = Amount::ZERO;
        amount.try_add_assign(self.fuel_price(resources.wasm_fuel, VmRuntime::Wasm)?)?;
        amount.try_add_assign(self.fuel_price(resources.evm_fuel, VmRuntime::Evm)?)?;
        amount.try_add_assign(self.read_operations_price(resources.read_operations)?)?;
        amount.try_add_assign(self.write_operations_price(resources.write_operations)?)?;
        amount.try_add_assign(self.bytes_read_price(resources.bytes_to_read as u64)?)?;
        amount.try_add_assign(self.bytes_written_price(resources.bytes_to_write as u64)?)?;
        amount.try_add_assign(
            self.blob_byte_read
                .try_mul(resources.blob_bytes_to_read as u128)?
                .try_add(self.blob_read.try_mul(resources.blobs_to_read as u128)?)?,
        )?;
        amount.try_add_assign(
            self.blob_byte_published
                .try_mul(resources.blob_bytes_to_publish as u128)?
                .try_add(
                    self.blob_published
                        .try_mul(resources.blobs_to_publish as u128)?,
                )?,
        )?;
        amount.try_add_assign(self.message.try_mul(resources.messages as u128)?)?;
        amount.try_add_assign(self.message_bytes_price(resources.message_size as u64)?)?;
        amount.try_add_assign(self.bytes_stored_price(resources.storage_size_delta as u64)?)?;
        amount.try_add_assign(
            self.service_as_oracle_queries_price(resources.service_as_oracle_queries)?,
        )?;
        amount.try_add_assign(self.http_requests_price(resources.http_requests)?)?;
        Ok(amount)
    }

    pub(crate) fn operation_bytes_price(&self, size: u64) -> Result<Amount, ArithmeticError> {
        self.operation_byte.try_mul(size as u128)
    }

    pub(crate) fn message_bytes_price(&self, size: u64) -> Result<Amount, ArithmeticError> {
        self.message_byte.try_mul(size as u128)
    }

    pub(crate) fn read_operations_price(&self, count: u32) -> Result<Amount, ArithmeticError> {
        self.read_operation.try_mul(count as u128)
    }

    pub(crate) fn write_operations_price(&self, count: u32) -> Result<Amount, ArithmeticError> {
        self.write_operation.try_mul(count as u128)
    }

    pub(crate) fn bytes_read_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_read.try_mul(count as u128)
    }

    pub(crate) fn bytes_written_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_written.try_mul(count as u128)
    }

    pub(crate) fn blob_read_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.blob_byte_read
            .try_mul(count as u128)?
            .try_add(self.blob_read)
    }

    pub(crate) fn blob_published_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.blob_byte_published
            .try_mul(count as u128)?
            .try_add(self.blob_published)
    }

    // TODO(#1536): This is not fully implemented.
    #[allow(dead_code)]
    pub(crate) fn bytes_stored_price(&self, count: u64) -> Result<Amount, ArithmeticError> {
        self.byte_stored.try_mul(count as u128)
    }

    /// Returns how much it would cost to perform `count` queries to services running as oracles.
    pub(crate) fn service_as_oracle_queries_price(
        &self,
        count: u32,
    ) -> Result<Amount, ArithmeticError> {
        self.service_as_oracle_query.try_mul(count as u128)
    }

    pub(crate) fn http_requests_price(&self, count: u32) -> Result<Amount, ArithmeticError> {
        self.http_request.try_mul(count as u128)
    }

    fn fuel_unit_price(&self, vm_runtime: VmRuntime) -> Amount {
        match vm_runtime {
            VmRuntime::Wasm => self.wasm_fuel_unit,
            VmRuntime::Evm => self.evm_fuel_unit,
        }
    }

    pub(crate) fn fuel_price(
        &self,
        fuel: u64,
        vm_runtime: VmRuntime,
    ) -> Result<Amount, ArithmeticError> {
        self.fuel_unit_price(vm_runtime).try_mul(u128::from(fuel))
    }

    /// Returns how much fuel can be paid with the given balance.
    pub(crate) fn remaining_fuel(&self, balance: Amount, vm_runtime: VmRuntime) -> u64 {
        let fuel_unit = self.fuel_unit_price(vm_runtime);
        u64::try_from(balance.saturating_div(fuel_unit)).unwrap_or(u64::MAX)
    }

    pub fn check_blob_size(&self, content: &BlobContent) -> Result<(), ExecutionError> {
        ensure!(
            u64::try_from(content.bytes().len())
                .ok()
                .is_some_and(|size| size <= self.maximum_blob_size),
            ExecutionError::BlobTooLarge
        );
        match content.blob_type() {
            BlobType::ContractBytecode | BlobType::ServiceBytecode | BlobType::EvmBytecode => {
                ensure!(
                    CompressedBytecode::decompressed_size_at_most(
                        content.bytes(),
                        self.maximum_bytecode_size
                    )?,
                    ExecutionError::BytecodeTooLarge
                );
            }
            BlobType::Data
            | BlobType::ApplicationDescription
            | BlobType::Committee
            | BlobType::ChainDescription => {}
        }
        Ok(())
    }
}
