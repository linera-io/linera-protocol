// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "WrappedFungibleTypes.sol";
import "Microchain.sol";

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

/// Bridges ERC20 tokens between Linera and EVM.
/// Linera→EVM: processes Credit messages from Linera blocks and releases ERC-20 tokens.
/// EVM→Linera: accepts deposits via deposit() and emits DepositInitiated events.
contract FungibleBridge is Microchain {
    /// Emitted when a user deposits ERC-20 tokens for bridging to Linera.
    /// The off-chain relayer uses this event (plus an MPT receipt proof) to
    /// mint the corresponding tokens on the target Linera chain.
    event DepositInitiated(
        uint256 source_chain_id,
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        address indexed depositor,
        address token,
        uint256 amount,
        uint256 nonce
    );

    bytes32 public immutable applicationId;
    IERC20 public immutable token;
    uint256 public depositNonce;

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

    /// Locks ERC-20 tokens in the bridge and emits a DepositInitiated event.
    /// Caller must first approve this contract to spend `amount` tokens.
    /// The emitted event is consumed by the off-chain relayer to produce an
    /// MPT proof that the Linera bridge app verifies before minting.
    function deposit(
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        uint256 amount
    ) external {
        require(amount > 0, "amount=0");
        require(target_application_id == applicationId, "target application mismatch");

        uint256 before = token.balanceOf(address(this));
        _safeTransferFrom(msg.sender, address(this), amount);
        uint256 received = token.balanceOf(address(this)) - before;
        require(received == amount, "fee-on-transfer tokens unsupported");

        uint256 currentNonce = depositNonce++;

        emit DepositInitiated(
            block.chainid,
            target_chain_id,
            target_application_id,
            target_account_owner,
            msg.sender,
            address(token),
            amount,
            currentNonce
        );
    }

    /// Processes a Linera block and releases ERC-20 tokens for any Credit
    /// messages targeting an Ethereum address (Address20).
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

                WrappedFungibleTypes.Message memory msg_ =
                    WrappedFungibleTypes.bcs_deserialize_Message(posted.message.user.bytes_);

                // choice==0 is Credit
                if (msg_.choice != 0) continue;

                WrappedFungibleTypes.Message_Credit memory credit = msg_.credit;
                // choice==2 is Address20 (Ethereum address)
                if (credit.target.choice != 2) continue;
                address target = address(credit.target.address20);
                require(token.transfer(target, credit.amount.value), "token transfer failed");
            }
        }
    }

    /// @dev Calls transferFrom and handles tokens that don't return a boolean.
    function _safeTransferFrom(address from, address to, uint256 amount_) internal {
        (bool success, bytes memory data) = address(token).call(
            abi.encodeWithSelector(token.transferFrom.selector, from, to, amount_)
        );
        require(success && (data.length == 0 || abi.decode(data, (bool))), "safeTransferFrom failed");
    }
}
