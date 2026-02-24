// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "FungibleTypes.sol";
import "Microchain.sol";

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
}

/// Bridges ERC20 tokens from a Linera microchain to Ethereum.
/// When a Credit message is received targeting an Ethereum address,
/// the contract transfers tokens from its own balance to the recipient.
contract FungibleBridge is Microchain {
    bytes32 public immutable applicationId;
    IERC20 public immutable token;

    constructor(
        address _lightClient,
        bytes32 _chainId,
        uint64 _nextExpectedHeight,
        bytes32 _applicationId,
        address _token
    )
        Microchain(_lightClient, _chainId, _nextExpectedHeight)
    {
        applicationId = _applicationId;
        token = IERC20(_token);
    }

    function _onBlock(BridgeTypes.Block memory blockValue) internal override {
        for (uint i = 0; i < blockValue.body.transactions.length; i++) {
            BridgeTypes.Transaction memory txn = blockValue.body.transactions[i];
            // choice==0 is ReceiveMessages
            if (txn.choice != 0) continue;

            BridgeTypes.IncomingBundle memory bundle = txn.receive_messages;
            for (uint j = 0; j < bundle.bundle.messages.length; j++) {
                BridgeTypes.PostedMessage memory posted = bundle.bundle.messages[j];
                // choice==1 is User
                if (posted.message.choice != 1) continue;
                if (posted.message.user.application_id.application_description_hash.value != applicationId) continue;

                FungibleTypes.Message memory msg_ =
                    FungibleTypes.bcs_deserialize_Message(posted.message.user.bytes_);

                // choice==0 is Credit
                if (msg_.choice != 0) continue;

                FungibleTypes.Message_Credit memory credit = msg_.credit;
                // choice==2 is Address20 (Ethereum address)
                if (credit.target.choice != 2) continue;
                address target = address(credit.target.address20);
                require(token.transfer(target, credit.amount.value), "token transfer failed");
            }
        }
    }
}
