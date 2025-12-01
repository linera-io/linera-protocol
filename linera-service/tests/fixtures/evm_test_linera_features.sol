// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./Linera.sol";

contract ExampleLineraFeatures {
    constructor() payable {
    }

    function get_balance(address account) external returns (uint256) {
        uint256 balance = account.balance;
        return balance;
    }

    function test_chain_id() external {
        Linera.ChainId memory chain_id = Linera.chain_id();
        Linera.ChainId memory creator_chain_id = Linera.application_creator_chain_id();
        require(chain_id.value == creator_chain_id.value);
    }

    function test_read_data_blob(bytes32 hash, uint32 len) external {
        bytes memory blob = Linera.read_data_blob(hash);
        require(blob.length == len);
    }

    function test_assert_data_blob_exists(bytes32 hash) external {
        Linera.assert_data_blob_exists(hash);
    }

    function test_chain_ownership() external {
        Linera.ChainOwnership memory chain_ownership = Linera.chain_ownership();
        require(chain_ownership.super_owners.length == 0);
        require(chain_ownership.owners.length == 1);
    }

    function test_authenticated_owner_caller_id() external {
        Linera.opt_AccountOwner memory signer = Linera.authenticated_owner();
        require(signer.has_value);
        require(signer.value.choice == 2);
        address signer_address = address(signer.value.address20);
        require(signer_address == msg.sender);
        Linera.opt_ApplicationId memory caller_id = Linera.authenticated_caller_id();
        require(caller_id.has_value == false);
    }

    function test_chain_balance(uint256 expected_balance) external {
        uint256 balance = Linera.read_chain_balance();
        require(balance == expected_balance);
    }

    function test_read_owners() external {
        Linera.AccountOwnerBalance[] memory owner_balances = Linera.read_owner_balances();
        require(owner_balances.length == 2);
        Linera.AccountOwner[] memory owners = Linera.read_balance_owners();
        require(owners.length == 2);
    }

    function test_linera_transfer(bytes32 chain_id, address destination, uint256 amount) external {
        uint8 reserved = 0;
        bytes32 address32;
        bytes20 address20 = bytes20(destination);
        Linera.AccountOwner memory owner = Linera.AccountOwner(2, reserved, address32, address20);
        Linera.ChainId memory chain_id1 = Linera.ChainId(chain_id);
        Linera.Account memory account = Linera.Account(chain_id1, owner);
        Linera.transfer(account, amount);
    }
}
