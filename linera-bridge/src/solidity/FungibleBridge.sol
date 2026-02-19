// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "FungibleTypes.sol";
import "Microchain.sol";

/// Tracks fungible token messages from verified blocks on a Linera microchain.
contract FungibleBridge is Microchain {
    bytes32 public immutable applicationId;
    mapping(address => uint256) public balances;

    event Credit(
        BridgeTypes.AccountOwner target,
        uint128 amount,
        BridgeTypes.AccountOwner source
    );

    constructor(address _lightClient, bytes32 _chainId, bytes32 _applicationId)
        Microchain(_lightClient, _chainId)
    {
        applicationId = _applicationId;
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
                balances[target] += credit.amount.value;
                emit Credit(credit.target, credit.amount.value, credit.source);
            }
        }
    }
}
