// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract BridgeTest {
    function deserialize(bytes calldata data)
        external
        pure
        returns (
            bytes32 chainId,
            uint32 epoch,
            uint64 height,
            uint64 txCount,
            bytes32 firstTxAppId,
            bytes memory firstTxBytes
        )
    {
        BridgeTypes.ConfirmedBlockCertificate memory cert =
            BridgeTypes.bcs_deserialize_ConfirmedBlockCertificate(data);
        chainId = cert.value.header.chain_id.value.value;
        epoch = cert.value.header.epoch.value;
        height = cert.value.header.height.value;
        txCount = uint64(cert.value.body.transactions.length);
        if (txCount > 0) {
            BridgeTypes.Transaction memory tx0 = cert.value.body.transactions[0];
            // choice=1 is ExecuteOperation
            require(tx0.choice == 1, "expected ExecuteOperation");
            BridgeTypes.Operation memory op = tx0.execute_operation;
            // choice=1 is User
            require(op.choice == 1, "expected User operation");
            firstTxAppId = op.user.application_id.application_description_hash.value;
            firstTxBytes = op.user.bytes_;
        }
    }
}
