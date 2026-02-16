// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "FungibleTypes.sol";
import "Microchain.sol";

/// Tracks fungible token operations from verified blocks on a Linera microchain.
contract FungibleBridge is Microchain {
    bytes32 public immutable applicationId;
    uint64 public operationCount;

    constructor(address _lightClient, bytes32 _chainId, bytes32 _applicationId)
        Microchain(_lightClient, _chainId)
    {
        applicationId = _applicationId;
    }

    function _onBlock(BridgeTypes.Block memory blockValue) internal override {
        for (uint i = 0; i < blockValue.body.transactions.length; i++) {
            BridgeTypes.Transaction memory txn = blockValue.body.transactions[i];
            // choice==1 is ExecuteOperation
            if (txn.choice != 1) continue;
            BridgeTypes.Operation memory op = txn.execute_operation;
            // choice==1 is User
            if (op.choice != 1) continue;
            if (op.user.application_id.application_description_hash.value != applicationId) continue;

            // Deserialize the opaque bytes as FungibleOperation
            FungibleTypes.FungibleOperation memory fungibleOp =
                FungibleTypes.bcs_deserialize_FungibleOperation(op.user.bytes_);

            operationCount++;
        }
    }
}
