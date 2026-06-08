# Bridge upgradeability: swappable LightClient and BurnEvent decoder

Date: 2026-06-09
Branch target: `block-header-commitment` (tracking `origin/block-header-committement`)
Files affected: `LightClient.sol`, `Microchain.sol`, `FungibleBridge.sol`, new
`ILightClient.sol`, new `IBurnEventDecoder.sol`, new `FungibleBurnEventDecoderV1.sol`
(+ tests, deploy scripts, Rust relay/test-helper plumbing)

## Problem

`Microchain.lightClient` is `immutable` (`Microchain.sol:8`) and typed as the
concrete `LightClient`. Every Linera-side change that affects certificate
verification — signature scheme, committee/blob format, freshness rules, or a bug
in the verification code itself — forces deploying a new `FungibleBridge` and
migrating TVL out of the old one. The block-header-commitment change demonstrated
the cost: it rewrote `LightClient` verification, and a deployed bridge would have
been stranded on the old protocol.

The same applies one level down: `FungibleBridge` BCS-decodes `BurnEvent` inline
(`FungibleBridge.sol:187` via `WrappedFungibleTypes`). A fungible-application
schema change (same `applicationId`) would also force a TVL migration.

## Goal

Replace `LightClient` (and the burn-event decoder) behind a deployed
`FungibleBridge` without migrating TVL, gated by a timelock so a compromised
owner key cannot swap in a malicious verifier instantly.

Out of scope (decided):

- **No proxy pattern** (UUPS/Transparent). Mutable state is two address slots plus
  pause state; proxy machinery, storage-layout discipline, and the larger trust
  surface are not justified. A bug in `FungibleBridge`'s own logic still means a
  redeploy-and-drain migration — accepted.
- **No on-chain rotation of `owner` or `timelockDelay`.** Rotation = migration.
  Avoids the recursive who-governs-the-governor problem.
- **No multi-role governance** (proposer/canceller/guardian split). This deployment
  is testnet-era; one owner Safe is enough. Tighten before mainnet.

## What the swap does and does not absorb

The header-commitment work changed the *payload type* the consumer sees
(`Block` → `BlockProof`), so even a swappable LC would not have absorbed it
without a consumer redeploy. The narrow interface below is chosen to make that
class of change as rare as possible:

- **Absorbed by `setLightClient` (no migration):** signature-scheme changes,
  committee/blob format changes, epoch/freshness rule changes, proof-envelope
  changes (any new way to *prove* the same events), LC bug fixes.
- **Absorbed by `setDecoder` (no migration):** `BurnEvent` BCS schema changes with
  the same semantics (target + amount).
- **Still a migration:** changes to the `Event` struct shape or stream-id model
  (the interface's return type), `fungibleApplicationId` rotation, `token` change,
  owner rotation, bugs in `FungibleBridge` logic.

## Design

### 1. `ILightClient` — the stable seam (new file)

The consumer-facing surface. Deliberately narrow: it returns only what
`FungibleBridge` consumes, not the full `BlockProof` (which also carries
`transactions`, `round`, `signatures` — those stay internal to the concrete LC;
`addCommittee` remains a function of the concrete contract, called directly by
the relayer, not through this interface).

```solidity
interface ILightClient {
    /// The Linera chain ID this client verifies blocks for. Immutable per instance.
    function expectedChainId() external view returns (bytes32);

    /// Verifies a serialized proof of a confirmed Linera block and returns the
    /// verified events.
    /// @return blockHash  CryptoHash::new(&BlockHeader) — the value the validator
    ///                    quorum signed. Stable across LC versions for a given
    ///                    Linera block.
    /// @return height     The block height (dedup key component).
    /// @return events     The block's events, one inner array per transaction,
    ///                    verified against the header's events_hash. Event.index
    ///                    is preserved (dedup key component).
    function verifyEvents(bytes calldata data)
        external
        view
        returns (bytes32 blockHash, uint64 height, BridgeTypes.Event[][] memory events);
}
```

Notes:

- `BridgeTypes.Event` stays in the signature. It is the one generated type the
  consumer fundamentally needs (stream filtering + `index` + payload bytes).
  Hiding it behind a flattened tuple would just re-encode the same coupling.
- `height` is returned explicitly so the consumer never touches `proof.header`.
- The chain-ID check moves *inside* the LC (see §2); the consumer constructor
  asserts `expectedChainId() == chainId` instead of checking per call.

### 2. `LightClient` changes

- Implement `ILightClient`.
- New immutable `expectedChainId`, set in the constructor (must be non-zero).
  `verifyCertificate` rejects `proof.header.chain_id != expectedChainId`
  ("wrong chain"). This replaces the per-call check in `Microchain.addBlock`
  (`Microchain.sol:23`) and `FungibleBridge.processBurns` (`FungibleBridge.sol:144`).
- `verifyEvents` wraps the existing `verifyCertificate` and projects
  `(blockHash, header.height.value, proof.events)`.
- `verifyBlock` (full `BlockProof` return) remains for `addCommittee`-adjacent
  tooling but is no longer referenced by `Microchain`/`FungibleBridge`.
- Everything else (quorum verification, `_hashEvents`, `_hashTransactions`,
  committee handling) is untouched.

`epochFreshnessWindow` (rejecting blocks from epochs older than
`currentEpoch - window`) is **deferred**: it is a hardening measure orthogonal to
upgradeability, and its interaction with migration drain windows deserves its own
discussion. Tracked as follow-up, not in these PRs.

### 3. `Microchain` changes — governance + swap

Storage:

```solidity
ILightClient public lightClient;          // mutable, timelock-gated
bytes32 public immutable chainId;
address public immutable owner;           // Safe
uint256 public immutable timelockDelay;

ILightClient public pendingLightClient;
uint256 public pendingLightClientReadyAt;

uint256 public pausedUntil;               // emergency pause, auto-expiring
```

Constants: `PAUSE_MAX_DURATION = 14 days`, `TIMELOCK_DELAY_MIN = 1 hours`,
`TIMELOCK_DELAY_MAX = 90 days`. (Min is 1 hour, not 1 day, because this is a
testnet deployment where day-long waits for routine swaps are pure friction;
mainnet redeployment chooses a longer delay within the same bounds.)

Constructor: requires non-zero `_lightClient`/`_owner`, delay within bounds, and
`ILightClient(_lightClient).expectedChainId() == _chainId` (catches deploy-script
mistakes at construction).

Update flow (propose / execute / cancel):

```solidity
function proposeLightClientUpdate(address newLC) external onlyOwner {
    // non-zero, != current, none pending, expectedChainId matches
    pendingLightClient = ILightClient(newLC);
    pendingLightClientReadyAt = block.timestamp + timelockDelay;
}

function executeLightClientUpdate() external {           // permissionless
    // pending exists, delay elapsed
    lightClient = pendingLightClient;
    // clear pending
}

function cancelLightClientUpdate() external onlyOwner {
    // pending exists; clear it
}
```

- **Permissionless execute**: liveness does not depend on the owner Safe after the
  delay elapses.
- **Single pending slot**: replacing a pending proposal requires cancel + propose,
  so observers see an explicit cancellation event.
- All three emit events (`LightClientUpdateProposed(newLC, readyAt)` /
  `...Executed(old, new)` / `...Cancelled(proposed)`).

Emergency pause (owner-held, auto-expiring):

```solidity
function pause(uint256 duration) external onlyOwner {   // 0 < duration <= 14 days
    pausedUntil = block.timestamp + duration;
}
function unpause() external onlyOwner { pausedUntil = 0; }
modifier whenNotPaused() { require(block.timestamp >= pausedUntil, "paused"); _; }
```

`whenNotPaused` gates **all three** value-moving entrypoints: `addBlock`,
`processBurns` (both in `Microchain`/`FungibleBridge`), and `deposit`. Auto-expiry
bounds a compromised/lost owner key to 14 days of DoS per pause call; re-pausing
extends visibly on-chain. The pause is the incident-response companion to the
timelock: pause first, then cancel/propose under no time pressure.

`addBlock` becomes:

```solidity
function addBlock(bytes calldata data) external whenNotPaused {
    (, uint64 height, BridgeTypes.Event[][] memory events) =
        lightClient.verifyEvents(data);
    _onEvents(height, events);
}

function _onEvents(uint64 height, BridgeTypes.Event[][] memory events) internal virtual;
```

`Microchain` no longer imports `LightClient` (concrete) nor reads
`proof.header.*`. The chain-id `require` disappears (now inside the LC). The
existing idempotency contract (subclasses dedup per `(height, Event.index)`) is
unchanged and documented on `_onEvents`.

### 4. `FungibleBridge` changes — decoder extraction + plumbing

New interface (own file):

```solidity
/// Decodes a wrapped-fungible BurnEvent payload. Implementations MUST be pure:
/// no state, no external calls. A buggy decoder can return wrong values but
/// cannot re-enter or read storage.
interface IBurnEventDecoder {
    function decode(bytes calldata eventValue)
        external pure returns (bytes20 target, uint128 amount);
}
```

(`bytes20`/`uint128` match the real `BurnEvent { bytes20 target; uint128 amount }`
in `WrappedFungibleTypes.sol:46` — not the `address`/`uint256` of the old spec.)

`FungibleBurnEventDecoderV1` wraps the existing
`WrappedFungibleTypes.bcs_deserialize_BurnEvent`.

`FungibleBridge`:

- `IBurnEventDecoder public decoder` — mutable, with a propose/execute/cancel flow
  structurally identical to the light-client one (same `owner`, same
  `timelockDelay`, separate pending slot, separate events).
- `_releaseBurn` calls `decoder.decode(evt.value)` instead of inline BCS. The
  dedup-flag-before-transfer ordering (`FungibleBridge.sol:188`) is preserved; the
  decoder being `pure` is compiler-enforced, so the token transfer remains the
  only external call after the flag is set.
- `_onBlock(BlockProof)` becomes `_onEvents(uint64 height, Event[][] events)`;
  the burn-matching loop, `(height, evt.index)` dedup keys, `isBurnProcessed`,
  and `BurnReleased` are unchanged.
- `processBurns` switches to `lightClient.verifyEvents` and drops its own chain-id
  require; chunked-settlement semantics (skip-already-processed, revert on
  non-burn positions) are unchanged.
- `deposit` gains `whenNotPaused`. No other changes.

### 5. Trust model

- **Owner Safe compromised:** worst case is proposing a malicious LC or decoder
  and waiting out `timelockDelay`, or 14-day-renewable DoS via pause. The delay is
  the community's detection-and-response window (and on testnet, the response is
  redeployment, which is cheap). No governance action moves tokens directly.
- **LC and decoder are inside the bridge trust boundary** — deployed and audited
  together. The consumer does not re-verify their output.
- **`blockHash` replay-key stability:** `CryptoHash::new(&BlockHeader)` is what
  validators sign; any correct LC produces the same value for the same block.
  (Currently informational — dedup is per `(height, index)`, not per block hash —
  but documented on `ILightClient` as a hard contract for future consumers.)

### 6. Rust-side blast radius

- `linera-bridge/src/evm/microchain.rs`, `evm/light_client.rs`, `relay/evm.rs`,
  `fungible_bridge.rs`: constructor signatures gain
  `owner`/`timelockDelay`/`decoder`/`expectedChainId` args; ABI bindings regenerate.
- Deploy scripts (`script/Deploy*.s.sol`) and `test_helpers.rs` updated.
- Codegen: `cargo build -p linera-bridge --features codegen` after any generated
  type changes (CI gates on a clean diff).
- Gas table (`gas.rs`) re-measured: decoder CALL adds ~700 gas warm per burn.

## PR split

- **PR-A — swappable LightClient + governance.** `ILightClient`, `expectedChainId`
  + internal chain-id check, `Microchain` owner/timelock/pause + `verifyEvents`
  consumption, `FungibleBridge` rewired (decoder still inline). Forge +
  Rust/revm + relay tests.
- **PR-B — swappable decoder.** `IBurnEventDecoder`, `FungibleBurnEventDecoderV1`,
  `decoder` slot + timelock flow, `_releaseBurn` via decoder. Depends on PR-A.

## Test plan

Forge (`linera-bridge/src/solidity/test/`):

- Timelock flows (LC and decoder symmetrically): propose→wait→execute happy path;
  execute-before-delay reverts; non-owner propose/cancel reverts; permissionless
  execute; cancel clears pending; second propose while pending reverts; propose
  zero/current address reverts; propose LC with wrong `expectedChainId` reverts.
- Pause: owner pauses → `addBlock`/`processBurns`/`deposit` revert; auto-expiry
  un-gates without any call; early unpause; re-pause extends; duration 0 or
  > 14 days reverts; non-owner reverts.
- Swap end-to-end: deploy LC-v2 (same `expectedChainId`), swap via timelock,
  verify a real fixture cert through the new LC, dedup state from pre-swap burns
  still honored (same `(height,index)` keys).
- Decoder swap end-to-end: V1 → V2 (V2 with a deliberately shifted schema fixture),
  burns decode through V2, pre-swap dedup intact.
- Constructor validation matrix.
- Regression: existing `Microchain.t.sol` / `FungibleBridge.t.sol` suites pass
  with only mechanical setup changes (extra constructor args).

Rust: `cargo test -p linera-bridge --features relay`; ignored anvil/solc tests;
e2e subcrate compile (`cd linera-bridge/tests/e2e`); evm-bridge contract
(`--features chain`) compile; `forge build --use "$(command -v solc)"` for
test/script contracts; codegen-diff clean.

## Risks

- **Constructor-arg churn** ripples through every deploy path (Solidity scripts,
  Rust test helpers, e2e docker, relay config). Mechanical but wide; the e2e
  subcrate is the one CI doesn't gate (known pre-push hook gap) — must be
  compile-checked manually.
- **Timelock on testnet** can stall protocol-upgrade coordination if the delay is
  set long; mitigated by the 1-hour minimum and choosing a short testnet delay.
- **Interface freeze:** `ILightClient` returning `BridgeTypes.Event[][]` means an
  `Event`-struct change is a consumer redeploy. Accepted and documented (§ "What
  the swap does and does not absorb").
