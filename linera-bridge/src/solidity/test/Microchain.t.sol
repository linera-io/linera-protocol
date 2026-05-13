// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {Microchain} from "../Microchain.sol";
import {BridgeTypes} from "../BridgeTypes.sol";

/// Returns a hand-built `BridgeTypes.Block` directly from `verifyBlock`
/// without going through `vm.mockCall(...abi.encode(Block)...)`, which
/// triggers a solc 0.8.30 stack-too-deep on the `BridgeTypes.Block`
/// struct's via_ir codegen.
contract MockLightClient {
    bytes32 public immutable expectedChainId;

    constructor(bytes32 _chainId) {
        expectedChainId = _chainId;
    }

    function verifyBlock(bytes calldata)
        external
        view
        returns (BridgeTypes.Block memory b, bytes32 sigHash)
    {
        b.header.chain_id.value.value = expectedChainId;
        sigHash = bytes32(uint256(0x1234));
    }
}

/// Counts `_onBlock` invocations so the test can assert re-entry is allowed.
contract CountingMicrochain is Microchain {
    uint256 public onBlockCalls;

    constructor(address _lc, bytes32 _cid) Microchain(_lc, _cid) {}

    function _onBlock(BridgeTypes.Block memory) internal override {
        onBlockCalls += 1;
    }
}

contract MicrochainIdempotencyTest is Test {
    bytes32 constant CHAIN_ID = bytes32(uint256(0xC1));

    function test_addBlock_can_be_called_repeatedly_for_same_cert() public {
        MockLightClient lc = new MockLightClient(CHAIN_ID);
        CountingMicrochain mc = new CountingMicrochain(address(lc), CHAIN_ID);

        bytes memory cert = hex"deadbeef";
        mc.addBlock(cert);
        mc.addBlock(cert);

        assertEq(mc.onBlockCalls(), 2, "addBlock must accept repeated calls; subclass owns dedup");
    }
}
