// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

/// @notice Decodes a Linera fungible-application BurnEvent *payload* — the bytes
///         of `BridgeTypes.Event.value` — into the `(recipient, amount)` pair the
///         bridge needs to release tokens.
/// @dev    Implementations MUST be `pure`: no state, no external calls, no
///         callbacks. A buggy decoder can return wrong values but cannot read
///         storage or re-enter. The decoder owns only fungible-app payload schema
///         knowledge; stream matching and the dedup index stay in the bridge
///         (they require `BridgeTypes.Event`).
interface IBurnEventDecoder {
    function decode(bytes calldata eventValue) external pure returns (address recipient, uint256 amount);
}
