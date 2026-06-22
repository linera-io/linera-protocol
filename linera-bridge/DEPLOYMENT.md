# Linera Bridge — Contract Deployment & Governance Runbook

How to deploy the Linera→EVM bridge contracts, wire their governance, verify the
result, and operate them over their lifetime (timelocked upgrades, emergency
pause, committee retirement, and forced migrations).

This document is deploy-environment-agnostic. For **relayer** provisioning and
day-to-day relay operations on mainnet, see
[`docker/README.mainnet.md`](../docker/README.mainnet.md); this runbook covers
the **contracts** that runbook deliberately leaves "out-of-band".

> The EVM contracts are **not** upgradeable proxies. Upgradeability is achieved
> by swapping two narrow, governed seams (the light client and the burn-event
> decoder) behind a timelock. Everything else — including the governance
> addresses themselves — is immutable and can only change by deploying a fresh
> bridge (a migration). Choose the governance configuration carefully; it is the
> one thing you cannot change later without moving TVL.

---

## 1. What gets deployed

| Contract | Role | Cardinality |
|---|---|---|
| `LightClient` | Verifies Linera block headers + committee rotation; holds no funds. Shared by every consumer bridge on the same Linera network. | One per Linera network |
| `FungibleBridge` (extends `Microchain`) | Holds the locked ERC-20 TVL; releases tokens on proven Linera burns; accepts deposits. | One per bridged token |
| `FungibleBurnEventDecoderV1` | `pure` contract that decodes a `BurnEvent` payload into `(recipient, amount)`. **Deployed automatically by the `FungibleBridge` deploy script.** | One per decoder schema version |
| ERC-20 token | The token being bridged (`LineraToken` for tests, or a real ERC-20). | One per bridged token |

`Microchain` is the abstract governance base `FungibleBridge` inherits; it is not
deployed on its own.

**Deploy order:** ERC-20 (or use existing) → `LightClient` → `FungibleBridge`
(which deploys its decoder). The bridge constructor takes the light-client
address, so the light client must exist first.

> The account that *signs the deploy transactions* gets **no** special on-chain
> power afterwards — the contracts have no owner. Privilege lives only in the
> three governance roles below.

---

## 2. Governance roles

Three roles, each with deliberately narrow powers. All are set at construction
and are **immutable**.

| Role | Powers | Cannot |
|---|---|---|
| **Pause Guardian** | `emergencyPause` / `emergencyUnpause` (auto-expiring, ≤ 14 days) | Move funds, change any address, freeze indefinitely |
| **Proposer** | *Propose* a new light client / decoder (timelocked); call `expireEpochsBelow` (immediate) | Execute its own proposal early; move funds directly |
| **Canceller** | Cancel a pending proposal during the delay window | Anything else |

**Core safety property:** no governance action moves or redirects funds without
the full timelock elapsing. The worst a fully-compromised **proposer** can do is
propose a malicious light client/decoder and wait out the delay — during which
the **canceller** (a disjoint signer set) revokes and the **guardian** can pause.

### Recommended configuration

| Role | Suggested Safe | Notes |
|---|---|---|
| Pause Guardian | 2-of-3, hardware wallets, on-call rotation | Fast response; bounded blast radius |
| Proposer | 5-of-7 across ≥3 orgs / ≥3 time zones | EIP-712 verification, no blind-signing |
| Canceller | 2-of-3, ≥1 org disjoint from the proposer set | Lower threshold — cancelling is a safety action |

`proposer` and `canceller` **must be different addresses** (enforced on-chain).

### Timelock delay

`timelockDelay` is immutable and bounded to **`[1 day, 90 days]`**.

- **Mainnet: 30 days** recommended — the swaps target *scheduled* events (Linera
  hardforks, announced schema changes), and a long delay maximises the window to
  detect and cancel a malicious proposal.
- **Testnet: 1 day**.

---

## 3. Pre-deployment checklist

Decide and record, *before* touching a deploy script:

- [ ] **Governance addresses** — the three Safes (§2). Confirm `proposer ≠ canceller` and none is the zero address.
- [ ] **Timelock delay** — seconds, within `[86400, 7776000]` (1–90 days).
- [ ] **Linera network** — faucet URL; the admin chain ID and current committee are read from it.
- [ ] **Bridge chain ID** — the Linera chain this bridge settles burns for (32-byte hex).
- [ ] **`fungibleApplicationId`** — the wrapped-fungible app on Linera (the required `deposit` target).
- [ ] **`bridgeApplicationId`** — the EVM-bridge app whose `"burns"` stream this bridge releases against.
- [ ] **ERC-20 token address** — pre-deployed, or deploy `LineraToken` first.
- [ ] **Deploy/signing EVM key** — funded; used only to send deploy txs (no lasting privilege).

> ⚠️ The test/demo configs (`docker-compose.bridge-test.yml`,
> `examples/bridge-demo/setup.sh`) use **throwaway placeholder** governance
> addresses (`0x…dEaD`, `0x…bEEF`, `0x…Ca11`) and a 1-day timelock. **Never** use
> these in production — they are publicly known and controlled by no one.

---

## 4. Deploy the `LightClient`

The constructor needs the genesis committee (validators + weights), the admin
chain ID, the starting epoch, and the **pause guardian** + **proposer**
addresses. The first four are read from the Linera faucet; the governance
addresses you supply.

### 4.1 Build `light-client-args.json`

Use the bundled CLI — it queries the faucet for the committee and admin chain,
and folds in the two governance addresses:

```bash
linera-bridge init-light-client \
  --faucet-url   "$FAUCET_URL" \
  --pause-guardian "$PAUSE_GUARDIAN_SAFE" \
  --proposer       "$PROPOSER_SAFE" \
  --output         light-client-args.json
```

Resulting file (validators/weights/admin_chain_id/epoch from the faucet, the last
two from your flags):

```json
{
  "validators": ["0x…", "0x…"],
  "weights": [100, 100],
  "admin_chain_id": "0x…",
  "epoch": "0",
  "pause_guardian": "0x…",
  "proposer": "0x…"
}
```

### 4.2 Run the deploy script

```bash
cd linera-bridge/src/solidity
LIGHT_CLIENT_ARGS_JSON_FILE=../../../light-client-args.json \
forge script script/DeployLightClient.s.sol \
  --rpc-url "$RPC_URL" \
  --private-key "$EVM_PRIVATE_KEY" \
  --broadcast
```

The script asserts `lc.adminChainId() == admin_chain_id` post-deploy. Record the
deployed address as **`$LIGHT_CLIENT`**.

> Optional explorer verification: export `EXPLORER_API_KEY` + `VERIFIER_URL`
> before the script to append `--verify` (see `README.mainnet.md`).

---

## 5. Deploy the `FungibleBridge`

The deploy script reads everything from the environment, **deploys a fresh
`FungibleBurnEventDecoderV1`**, then deploys the bridge wired to it.

| Env var | Meaning |
|---|---|
| `LIGHT_CLIENT` | The `LightClient` from §4 |
| `BRIDGE_CHAIN_ID` | Linera chain this bridge settles (32-byte hex) |
| `TOKEN_ADDRESS` | The ERC-20 to bridge |
| `FUNGIBLE_APP_ID` | Wrapped-fungible app ID (deposit target) |
| `BRIDGE_APP_ID` | EVM-bridge app ID (burns-stream source) |
| `PAUSE_GUARDIAN` | Guardian Safe |
| `PROPOSER` | Proposer Safe |
| `CANCELLER` | Canceller Safe (≠ proposer) |
| `TIMELOCK_DELAY` | Seconds, `[86400, 7776000]` |

```bash
cd linera-bridge/src/solidity
LIGHT_CLIENT="$LIGHT_CLIENT" \
BRIDGE_CHAIN_ID="$BRIDGE_CHAIN_ID" \
TOKEN_ADDRESS="$TOKEN_ADDRESS" \
FUNGIBLE_APP_ID="$FUNGIBLE_APP_ID" \
BRIDGE_APP_ID="$BRIDGE_APP_ID" \
PAUSE_GUARDIAN="$PAUSE_GUARDIAN_SAFE" \
PROPOSER="$PROPOSER_SAFE" \
CANCELLER="$CANCELLER_SAFE" \
TIMELOCK_DELAY="2592000" \
forge script script/DeployFungibleBridge.s.sol \
  --rpc-url "$RPC_URL" \
  --private-key "$EVM_PRIVATE_KEY" \
  --broadcast
```

The script asserts the bridge points at `$LIGHT_CLIENT` and at the decoder it
just deployed. Record the bridge address as **`$BRIDGE`**.

> The broadcast artifact contains **two** `CREATE`s (decoder, then bridge).
> Select the bridge by `contractName` (`FungibleBridge`), not `transactions[0]`.

After deploy: fund `$BRIDGE` with the ERC-20 it will release, register the bridge
address with the Linera-side `evm-bridge` app, and populate the relayer `.env`
(`EVM_BRIDGE_ADDRESS`, `MONITOR_START_BLOCK` = the bridge's deploy block, etc.) —
see `README.mainnet.md` §3–4.

---

## 6. Post-deploy verification

```bash
# Bridge wiring
cast call "$BRIDGE" "lightClient()(address)"        --rpc-url "$RPC_URL"  # == $LIGHT_CLIENT
cast call "$BRIDGE" "decoder()(address)"            --rpc-url "$RPC_URL"  # the V1 decoder
cast call "$BRIDGE" "token()(address)"              --rpc-url "$RPC_URL"  # == $TOKEN_ADDRESS

# Governance (immutable) — confirm the Safes, not the placeholders
cast call "$BRIDGE" "pauseGuardian()(address)"      --rpc-url "$RPC_URL"
cast call "$BRIDGE" "proposer()(address)"           --rpc-url "$RPC_URL"
cast call "$BRIDGE" "canceller()(address)"          --rpc-url "$RPC_URL"
cast call "$BRIDGE" "timelockDelay()(uint256)"      --rpc-url "$RPC_URL"  # your delay, in seconds

# Light client governance + network binding
cast call "$LIGHT_CLIENT" "adminChainId()(bytes32)" --rpc-url "$RPC_URL"
cast call "$LIGHT_CLIENT" "pauseGuardian()(address)" --rpc-url "$RPC_URL"
cast call "$LIGHT_CLIENT" "proposer()(address)"      --rpc-url "$RPC_URL"
```

Confirm every governance field is a real Safe and the timelock matches intent
**before** funding the bridge.

---

## 7. Operational runbooks

All proposals are public on-chain; set up monitoring/alerting on the
`*UpdateProposed`, `*UpdateExecuted`, `*UpdateCancelled`, and
`EmergencyPaused`/`EmergencyUnpaused` events.

### 7.1 Swap the light client (Linera protocol / format / signature upgrade)

No TVL migration — the per-burn replay key is independent of which client
verified the block, so a swap cannot double-release.

1. **Bootstrap** a new `LightClient` for the **same** Linera network (same
   `adminChainId`) and catch its committee set up to the live epoch
   (`addCommittee`) — do this *before* the timelock elapses.
2. **Propose** (proposer Safe):
   ```bash
   cast send "$BRIDGE" "proposeLightClientUpdate(address)" "$NEW_LIGHT_CLIENT" --rpc-url "$RPC_URL"
   ```
   Reverts if the new client is zero, the current one, already-pending, or tracks
   a different network.
3. **Wait** `timelockDelay`. Anyone can then **execute** (permissionless):
   ```bash
   cast send "$BRIDGE" "executeLightClientUpdate()" --rpc-url "$RPC_URL"
   ```
4. The relayer **re-registers** any in-flight blocks on the new client and
   resumes `processBurns`. No user action; TVL never moves.

**To abort** during the window (canceller or proposer):
`cast send "$BRIDGE" "cancelLightClientUpdate()"`.

### 7.2 Swap the decoder (BurnEvent payload schema change, same `applicationId`)

No TVL migration. Audit the new decoder during the 30-day window.

```bash
# proposer:
cast send "$BRIDGE" "proposeDecoderUpdate(address)" "$NEW_DECODER" --rpc-url "$RPC_URL"
# after timelockDelay, anyone:
cast send "$BRIDGE" "executeDecoderUpdate()" --rpc-url "$RPC_URL"
# abort (canceller/proposer):
cast send "$BRIDGE" "cancelDecoderUpdate()" --rpc-url "$RPC_URL"
```

### 7.3 Emergency pause

Auto-expires (≤ 14 days); the guardian cannot freeze indefinitely.

```bash
# guardian — duration in seconds (e.g. 14 days = 1209600):
cast send "$BRIDGE" "emergencyPause(uint256)" 1209600 --rpc-url "$RPC_URL"   # halts deposit + processBurns
cast send "$BRIDGE" "emergencyUnpause()"                --rpc-url "$RPC_URL"   # lift early
```

> **Blast radius.** Pausing the **bridge** halts that one bridge's `deposit` +
> `processBurns`. Pausing the **shared `LightClient`**
> (`cast send "$LIGHT_CLIENT" "emergencyPause(uint256)" …`) halts `registerBlock`
> for **every** consumer bridge on the network — reserve it for network/protocol
> incidents. Re-pausing before expiry extends the window.

### 7.4 Retire a compromised retired committee

Immediate-effect, proposer-gated; monotonic and capped at `currentEpoch` (can
never retire the live committee):

```bash
cast send "$LIGHT_CLIENT" "expireEpochsBelow(uint32)" "$NEW_MIN_EPOCH" --rpc-url "$RPC_URL"
```

### 7.5 Forced migration (deploy-and-drain)

Required only for changes the timelock can't absorb (see §8). Outline:

1. Deploy the new contracts; verify + independent audit.
2. Pre-announce (forum/Discord/email): addresses, timeline, instructions.
3. `emergencyPause` deposits on the old bridge (extend by re-pausing).
4. Open the drain window: users burn on Linera and settle on the **old** bridge
   via the existing `registerBlock` + `processBurns` path; redeposit into the new
   bridge if desired.
5. Keep the old bridge running indefinitely (bounded by `minAcceptedEpoch`);
   accept a long-tail of unmigrated TVL.

---

## 8. When does which path apply?

| Change | Action | Migration? |
|---|---|---|
| Validator set rotation / epoch advance | `addCommittee` (permissionless) | No |
| Retire a compromised *retired* committee | `expireEpochsBelow` (proposer) | No |
| Block / header / signature-scheme change | `setLightClient` (§7.1) | No |
| `BurnEvent` payload schema change, **same** appIds | `setDecoder` (§7.2) | No |
| `fungibleApplicationId` **or** `bridgeApplicationId` rotation | New bridge + drain | **Yes** |
| Decoder interface widening (burns gain fields the bridge must act on) | New bridge + new decoder interface | **Yes** |
| Rotate `pauseGuardian` / `proposer` / `canceller` / `timelockDelay` | New bridge | **Yes** |
| Bug in `FungibleBridge` settlement logic | New bridge + drain | **Yes** |
| Exploitable bug in light client / decoder | `emergencyPause`, fix, swap after audit | Maybe |
| New ERC-20 token / new bridge type | New independent instance | No |

---

## 9. On-chain constraints (quick reference)

Enforced by the constructors / governance functions:

- Governance addresses (light client, guardian, proposer, canceller, decoder) must be **non-zero**.
- `proposer ≠ canceller`.
- `timelockDelay ∈ [1 day, 90 days]` (`[86400, 7776000]` s).
- Emergency pause duration `∈ (0, 14 days]`; auto-expires.
- A proposed light client must have a matching `adminChainId` ("same network").
- Only one light-client update and one decoder update may be pending at a time
  (cancel to replace).
- `deposit` rejects amounts above `u128::MAX` (the Linera side holds `U128`).
