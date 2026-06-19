// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "ILightClient.sol";

/// Base for consumer bridges. Holds the light client behind the narrow
/// `ILightClient` interface (swappable via a governance timelock) and the
/// emergency-pause / governance machinery shared by all bridges. Holds no funds.
abstract contract Microchain {
    // Swappable via the timelocked setLightClient flow below.
    ILightClient public lightClient;
    bytes32 public immutable chainId;

    // Governance. All immutable — rotation is a redeployment, never an on-chain
    // setter (avoids a recursive "who controls the rotation?" trust problem).
    address public immutable pauseGuardian;
    address public immutable proposer;
    address public immutable canceller;
    uint256 public immutable timelockDelay;

    // Pending light-client update (single-pending; cancel to replace).
    ILightClient public pendingLightClient;
    uint256 public pendingLightClientReadyAt;

    // Emergency pause (local to this bridge). Auto-expires.
    uint256 public pausedUntil;

    uint256 public constant PAUSE_MAX_DURATION = 14 days;
    uint256 public constant TIMELOCK_DELAY_MIN = 1 days;
    uint256 public constant TIMELOCK_DELAY_MAX = 90 days;

    event LightClientUpdateProposed(address indexed newLightClient, uint256 readyAt);
    event LightClientUpdateExecuted(address indexed oldLightClient, address indexed newLightClient);
    event LightClientUpdateCancelled(address indexed proposed);
    event EmergencyPaused(uint256 until);
    event EmergencyUnpaused();

    constructor(
        address _lightClient,
        bytes32 _chainId,
        address _pauseGuardian,
        address _proposer,
        address _canceller,
        uint256 _timelockDelay
    ) {
        require(_lightClient != address(0), "zero lightClient");
        require(_pauseGuardian != address(0), "zero pauseGuardian");
        require(_proposer != address(0), "zero proposer");
        require(_canceller != address(0), "zero canceller");
        require(_proposer != _canceller, "proposer == canceller");
        require(_timelockDelay >= TIMELOCK_DELAY_MIN, "timelock too short");
        require(_timelockDelay <= TIMELOCK_DELAY_MAX, "timelock too long");

        lightClient = ILightClient(_lightClient);
        chainId = _chainId;
        pauseGuardian = _pauseGuardian;
        proposer = _proposer;
        canceller = _canceller;
        timelockDelay = _timelockDelay;
    }

    modifier onlyProposer() {
        require(msg.sender == proposer, "only proposer");
        _;
    }

    modifier whenNotEmergencyPaused() {
        require(block.timestamp >= pausedUntil, "emergency paused");
        _;
    }

    // --- Light-client update (timelocked) ---

    /// Proposes repointing to `newLightClient`. The new client must track the
    /// same Linera network (same `adminChainId`); TVL is preserved because the
    /// per-burn replay key is independent of which client verified the block.
    function proposeLightClientUpdate(address newLightClient) external onlyProposer {
        require(newLightClient != address(0), "zero address");
        require(newLightClient != address(lightClient), "no-op update");
        require(address(pendingLightClient) == address(0), "update already pending");
        require(ILightClient(newLightClient).adminChainId() == lightClient.adminChainId(), "different network");

        pendingLightClient = ILightClient(newLightClient);
        pendingLightClientReadyAt = block.timestamp + timelockDelay;
        emit LightClientUpdateProposed(newLightClient, pendingLightClientReadyAt);
    }

    /// Permissionless once the delay elapses, so the second leg never depends on
    /// proposer liveness.
    function executeLightClientUpdate() external {
        require(address(pendingLightClient) != address(0), "no pending update");
        require(block.timestamp >= pendingLightClientReadyAt, "delay not elapsed");
        address old = address(lightClient);
        lightClient = pendingLightClient;
        emit LightClientUpdateExecuted(old, address(pendingLightClient));
        pendingLightClient = ILightClient(address(0));
        pendingLightClientReadyAt = 0;
    }

    /// Canceller (primary) or proposer (its own mistake) can revoke a pending
    /// update during the delay window.
    function cancelLightClientUpdate() external {
        require(msg.sender == canceller || msg.sender == proposer, "not authorized");
        require(address(pendingLightClient) != address(0), "no pending update");
        address proposed = address(pendingLightClient);
        pendingLightClient = ILightClient(address(0));
        pendingLightClientReadyAt = 0;
        emit LightClientUpdateCancelled(proposed);
    }

    // --- Emergency pause ---

    /// Pauses pause-gated entry points for `duration` (auto-expiring, capped at
    /// `PAUSE_MAX_DURATION`). Guardian-only. Re-pausing restarts the clock.
    function emergencyPause(uint256 duration) external {
        require(msg.sender == pauseGuardian, "only pause guardian");
        require(duration > 0 && duration <= PAUSE_MAX_DURATION, "invalid duration");
        pausedUntil = block.timestamp + duration;
        emit EmergencyPaused(pausedUntil);
    }

    /// Lifts an active pause early. Guardian-only.
    function emergencyUnpause() external {
        require(msg.sender == pauseGuardian, "only pause guardian");
        require(pausedUntil > block.timestamp, "not paused");
        pausedUntil = 0;
        emit EmergencyUnpaused();
    }
}
