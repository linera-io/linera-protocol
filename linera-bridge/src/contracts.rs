// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared `sol!` bindings for the bridge's EVM contracts.
//!
//! Defined once here so the off-chain relay and the end-to-end tests call the exact same ABI.
//! A signature change (e.g. an `addBlock` argument) then updates every caller in lockstep instead
//! of being copy-pasted into — and drifting between — each call site.

// `processBurns` carries the inclusion-proof components as separate arguments, so its
// `sol!`-generated binding exceeds clippy's argument-count threshold.
#![allow(clippy::too_many_arguments)]

use alloy::sol;

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata blockProof, bytes[] calldata eventBcs, uint32[] calldata eventsPerTx) external;
        function processBurns(
            bytes32 blockHash,
            bytes[] calldata eventBcs,
            uint32 txIndex,
            uint32 numTxs,
            uint32 numEventsInTx,
            uint32[] calldata positions,
            bytes32[] calldata innerSiblings,
            bytes32[] calldata outerSiblings
        ) external;
        function deposit(
            bytes32 target_chain_id,
            bytes32 target_application_id,
            bytes32 target_account_owner,
            uint256 amount
        ) external;
        function lightClient() external view returns (address);
        function token() external view returns (address);
        function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool);
    }

    #[sol(rpc)]
    interface ILightClient {
        function addCommittee(
            bytes calldata blockProof,
            bytes[] calldata eventBcs,
            uint32 txIndex,
            uint32 numTxs,
            uint32 numEventsInTx,
            uint32[] calldata positions,
            bytes32[] calldata innerSiblings,
            bytes32[] calldata outerSiblings,
            bytes calldata committeeBlob,
            bytes[] calldata validators
        ) external;
        function registerBlock(bytes calldata blockProof) external returns (bytes32);
        function registeredBlocks(bytes32 blockHash)
            external
            view
            returns (bytes32 eventsHash, uint64 height, bytes32 chainId);
        function proveEventsCommitted(
            bytes32 eventsHash,
            bytes[] calldata eventBcs,
            uint32 txIndex,
            uint32 numTxs,
            uint32 numEventsInTx,
            uint32[] calldata positions,
            bytes32[] calldata innerSiblings,
            bytes32[] calldata outerSiblings
        ) external pure;
        function currentEpoch() external view returns (uint32);
    }

    #[sol(rpc)]
    interface IMicrochain {
        function addBlock(bytes calldata blockProof, bytes[] calldata eventBcs, uint32[] calldata eventsPerTx) external;
        function lightClient() external view returns (address);
        function chainId() external view returns (bytes32);
    }

    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}
