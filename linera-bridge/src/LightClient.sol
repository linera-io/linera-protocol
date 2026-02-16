// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract LightClient {
    function addCommittee(bytes calldata data) external {
        BridgeTypes.ConfirmedBlockCertificate memory cert =
            BridgeTypes.bcs_deserialize_ConfirmedBlockCertificate(data);
    }

    function addBlock(bytes calldata data) external {
        BridgeTypes.ConfirmedBlockCertificate memory cert =
            BridgeTypes.bcs_deserialize_ConfirmedBlockCertificate(data);
    }
}
