// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract BridgeTest {
    function deserialize(bytes calldata data)
        external
        pure
        returns (bytes32 chainId, uint32 epoch, uint64 height)
    {
        BridgeTypes.ConfirmedBlockCertificate memory cert =
            BridgeTypes.bcs_deserialize_ConfirmedBlockCertificate(data);
        chainId = cert.value.header.chain_id.value.value;
        epoch = cert.value.header.epoch.value;
        height = cert.value.header.height.value;
    }
}
