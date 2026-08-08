// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "IBurnEventDecoder.sol";
import "WrappedFungibleTypesV1.sol";

/// @notice Decoder for the current wrapped-fungible `BurnEvent` schema
///         (`target: bytes20 ++ amount: u128`). Isolates the
///         `WrappedFungibleTypesV1` BCS dependency so a future schema change is a
///         `setDecoder` swap rather than a `FungibleBridge` redeployment.
contract FungibleBurnEventDecoderV1 is IBurnEventDecoder {
    function decodeBurnEvent(bytes calldata eventValue)
        external
        pure
        override
        returns (address recipient, uint256 amount)
    {
        WrappedFungibleTypesV1.BurnEvent memory burnEvt = WrappedFungibleTypesV1.bcs_deserialize_BurnEvent(eventValue);
        return (address(burnEvt.target), burnEvt.amount);
    }
}
