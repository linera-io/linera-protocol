// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {FungibleBurnEventDecoderV1} from "../FungibleBurnEventDecoderV1.sol";
import {IBurnEventDecoder} from "../IBurnEventDecoder.sol";
import {WrappedFungibleTypes} from "../WrappedFungibleTypes.sol";

contract FungibleBurnEventDecoderV1Test is Test {
    FungibleBurnEventDecoderV1 internal decoder;

    function setUp() public {
        decoder = new FungibleBurnEventDecoderV1();
    }

    /// A canonical BurnEvent payload (target ++ amount, no enum tag) round-trips
    /// through the decoder to the same (recipient, amount). Cross-checks against
    /// the generated BCS serializer.
    function test_decode_returns_recipient_and_amount() public view {
        address target = address(0xA0);
        uint128 amount = 1_000_000_000_000_000_000;

        WrappedFungibleTypes.BurnEvent memory burnEvt;
        burnEvt.target = bytes20(target);
        burnEvt.amount = amount;
        bytes memory value = WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);

        (address recipient, uint256 decodedAmount) = decoder.decode(value);
        assertEq(recipient, target, "recipient should match the burn target");
        assertEq(decodedAmount, uint256(amount), "amount should match the burn amount");
    }

    /// The V1 decoder satisfies the IBurnEventDecoder interface.
    function test_implements_interface() public view {
        IBurnEventDecoder iface = IBurnEventDecoder(address(decoder));
        address target = address(0xBEEF);
        uint128 amount = 7;

        WrappedFungibleTypes.BurnEvent memory burnEvt;
        burnEvt.target = bytes20(target);
        burnEvt.amount = amount;
        bytes memory value = WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);

        (address recipient, uint256 decodedAmount) = iface.decode(value);
        assertEq(recipient, target, "interface decode recipient");
        assertEq(decodedAmount, uint256(amount), "interface decode amount");
    }

    /// A truncated payload (shorter than 20-byte target + 16-byte amount) reverts
    /// rather than silently returning garbage.
    function test_decode_malformed_reverts() public {
        bytes memory tooShort = hex"0102";
        vm.expectRevert();
        decoder.decode(tooShort);
    }
}
