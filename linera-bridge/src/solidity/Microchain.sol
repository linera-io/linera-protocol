// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;

    constructor(address _lightClient, bytes32 _chainId) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
    }

    /// Verifies a certificate and dispatches to the subclass. Subclasses
    /// MUST be idempotent under repeated calls for the same block: this
    /// contract does not gate on `signedHash`. The off-chain relayer
    /// relies on that idempotency to safely re-submit `addBlock(cert)`
    /// after partial settlement.
    function addBlock(bytes calldata data) external {
        (BridgeTypes.BlockProof memory blockValue,) = lightClient.verifyBlock(data);
        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");
        _onBlock(blockValue);
    }

    /// Called after a block has been verified and accepted. Subcontracts
    /// implement this to extract and store application-specific data.
    function _onBlock(BridgeTypes.BlockProof memory blockValue) internal virtual;
}
