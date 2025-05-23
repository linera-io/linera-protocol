// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./Linera.sol";

contract ExampleLineraFeatures {
    function test_chain_id() external {
        LineraTypes.ChainId memory chain_id = Linera.chain_id();
        LineraTypes.ChainId memory creator_chain_id = Linera.application_creator_chain_id();
        require(chain_id.value.value == creator_chain_id.value.value);
    }

    function test_read_data_blob(bytes32 hash, uint32 len) external {
        bytes memory blob = Linera.read_data_blob(hash);
        require(blob.length == len);
    }

    function test_assert_data_blob_exists(bytes32 hash) external {
        Linera.assert_data_blob_exists(hash);
    }

    function test_chain_ownership() external {
        LineraTypes.ChainOwnership memory chain_ownership = Linera.chain_ownership();
        require(chain_ownership.super_owners.length == 0);
        require(chain_ownership.owners.length == 1);
    }
}
