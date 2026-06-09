// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {Microchain} from "../Microchain.sol";
import {BridgeTypes} from "../BridgeTypes.sol";

/// Returns a header directly from `verifyBlockFromEvents` (no event/quorum checks), so the
/// idempotency test can drive `addBlock` without constructing a real proof.
contract MockLightClient {
    bytes32 public immutable expectedChainId;

    constructor(bytes32 _chainId) {
        expectedChainId = _chainId;
    }

    function verifyBlockFromEvents(bytes calldata, bytes[] calldata, uint32[] calldata)
        external
        view
        returns (BridgeTypes.BlockHeader memory header, bytes32 blockHash)
    {
        header.chain_id.value.value = expectedChainId;
        blockHash = bytes32(uint256(0x1234));
    }
}

/// Counts `_onBlock` invocations so the test can assert re-entry is allowed.
contract CountingMicrochain is Microchain {
    uint256 public onBlockCalls;

    constructor(address _lc, bytes32 _cid) Microchain(_lc, _cid) {}

    function _onBlock(BridgeTypes.BlockHeader memory, bytes[] calldata) internal override {
        onBlockCalls += 1;
    }
}

contract MicrochainIdempotencyTest is Test {
    bytes32 constant CHAIN_ID = bytes32(uint256(0xC1));

    function test_addBlock_can_be_called_repeatedly_for_same_cert() public {
        MockLightClient lc = new MockLightClient(CHAIN_ID);
        CountingMicrochain mc = new CountingMicrochain(address(lc), CHAIN_ID);

        bytes memory cert = hex"deadbeef";
        bytes[] memory noEvents = new bytes[](0);
        uint32[] memory noCounts = new uint32[](0);
        mc.addBlock(cert, noEvents, noCounts);
        mc.addBlock(cert, noEvents, noCounts);

        assertEq(mc.onBlockCalls(), 2, "addBlock must accept repeated calls; subclass owns dedup");
    }
}
