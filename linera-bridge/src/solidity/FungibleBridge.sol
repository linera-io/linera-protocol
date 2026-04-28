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
/// Linera→EVM: processes Burn operations from Linera blocks and releases ERC-20 tokens.
/// EVM→Linera: accepts deposits via deposit() and emits DepositInitiated events.
contract FungibleBridge is Microchain {
    /// Emitted when a user deposits ERC-20 tokens for bridging to Linera.
    /// The off-chain relayer uses this event (plus an MPT receipt proof) to
    /// mint the corresponding tokens on the target Linera chain.
    event DepositInitiated(
        address indexed depositor,
        uint256 source_chain_id,
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        address token,
        uint256 amount,
        uint256 nonce
    );

    // WrappedFungible application ID on Linera,
    // used to identify Burn events in the block stream.
    // Set once via registerFungibleApplicationId after deployment.
    bytes32 public fungibleApplicationId;
    // The ERC-20 token being bridged.
    IERC20 public immutable token;
    // The deployer, authorized to register the application ID.
    address public immutable deployer;
    uint256 public depositNonce;

    constructor(
        address _lightClient,
        bytes32 _chainId,
        address _token
    )
        Microchain(_lightClient, _chainId)
    {
        token = IERC20(_token);
        deployer = msg.sender;
    }

    /// Registers the wrapped-fungible application ID. Can only be called once, by the deployer.
    function registerFungibleApplicationId(bytes32 _fungibleApplicationId) external {
        require(msg.sender == deployer, "only deployer can register");
        require(fungibleApplicationId == bytes32(0), "application ID already registered");
        fungibleApplicationId = _fungibleApplicationId;
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
        require(target_application_id == fungibleApplicationId, "target application mismatch");

        uint256 before = token.balanceOf(address(this));
        _safeTransferFrom(msg.sender, address(this), amount);
        uint256 received = token.balanceOf(address(this)) - before;
        require(received == amount, "fee-on-transfer tokens unsupported");

        uint256 currentNonce = depositNonce++;

        emit DepositInitiated(
            msg.sender,
            block.chainid,
            target_chain_id,
            target_application_id,
            target_account_owner,
            address(token),
            amount,
            currentNonce
        );
    }

    /// Processes a Linera block and releases ERC-20 tokens for any BurnEvent
    /// events on the "burns" stream from the wrapped-fungible application.
    function _onBlock(BridgeTypes.Block memory blockValue) internal override {
        bytes32 burnsHash = keccak256("burns");
        for (uint i = 0; i < blockValue.body.events.length; i++) {
            BridgeTypes.Event[] memory txEvents = blockValue.body.events[i];
            for (uint j = 0; j < txEvents.length; j++) {
                BridgeTypes.Event memory evt = txEvents[j];

                // choice==1 is User application
                if (evt.stream_id.application_id.choice != 1) continue;
                if (evt.stream_id.application_id.user.application_description_hash.value != fungibleApplicationId) continue;

                // Check stream name is "burns"
                if (keccak256(evt.stream_id.stream_name.value) != burnsHash) continue;

                WrappedFungibleTypes.BurnEvent memory burnEvt =
                    WrappedFungibleTypes.bcs_deserialize_BurnEvent(evt.value);

                address target = address(burnEvt.target);
                require(token.transfer(target, burnEvt.amount.value), "token transfer failed");
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
