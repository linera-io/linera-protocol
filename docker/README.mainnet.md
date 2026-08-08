# Linera Bridge — Mainnet Deployment Runbook

This describes how to provision and operate a `linera-bridge` relayer
on a single VM, bridging Base Sepolia (EVM) and Testnet Conway (Linera).

> **Scope.** This runbook targets a production VM with **GCP Secret Manager**
> and systemd. To run the relayer **locally without GCP**, you only need
> section 3 (provision via the deploy runbook) and the
> [Local / without GCP](#local--without-gcp) command in section 4 — skip the
> VM/Secret-Manager steps (1, 2, and the Production compose).

## Prerequisites

- A Linux VM with Docker + docker-compose
- Foundry (`forge`, `cast`) and a recent `linera` binary on the VM (or
  available inside docker images)
- Outbound network access to:
  - Base RPC (operator's provider)
  - Mainnet faucet (TBD)
- A funded Base account (private key) for contract deployment
  and `addBlock` signing

## Initial deployment (one-time)

### 1. Create directories with correct ownership

```bash
sudo useradd -r linera-bridge || true
sudo mkdir -p /var/lib/linera-bridge /etc/linera-bridge
sudo chown -R linera-bridge:linera-bridge /var/lib/linera-bridge
sudo chmod 750 /var/lib/linera-bridge
sudo chmod 755 /etc/linera-bridge
```

### 2. Drop the EVM signing key

```bash
sudo tee /etc/linera-bridge/.env.secret >/dev/null <<'EOF'
EVM_PRIVATE_KEY=0x...
EOF
sudo chmod 600 /etc/linera-bridge/.env.secret
sudo chown root:root /etc/linera-bridge/.env.secret
```

### 3. Provision contracts, Linera apps, wallet, env file

Provision the contracts, Linera apps, wallet, and env file by following the
Docker-based deployment runbook in
[`linera-bridge/deploy/`](../linera-bridge/deploy/README.md) — a sequence of
copy-pasteable commands run inside the project's pre-built images
(`examples/bridge-demo/setup.sh` is for local Docker development only and is
not reused here).

Working through that runbook writes everything this runbook needs into
`linera-bridge/deploy/out/<network>/`:

- `wallet/` — the Linera wallet that owns the bridge chain. Copy its
  `wallet.json`, `keystore.json`, `client.db` to `/var/lib/linera-bridge/`
  (the relay container reads them via the bind-mount at `/data`).
- `relayer.env` — the env file consumed by `bridge-entrypoint.sh`, already
  using `/data/...` paths. Copy it to `/etc/linera-bridge/.env`.

The runbook's final step emits `relayer.env` with the addresses/IDs filled in
and `EVM_PRIVATE_KEY` deliberately omitted (kept in `.env.secret` / Secret
Manager, see above). For the full command sequence and per-network notes, see the
[deploy README](../linera-bridge/deploy/README.md). The fields it fills in are:

```
# /etc/linera-bridge/.env
RPC_URL=
FAUCET_URL=
EVM_BRIDGE_ADDRESS=0x...
LINERA_BRIDGE_APP=...                 # evm-bridge app ID (64 hex)
LINERA_FUNGIBLE_APP=...               # wrapped-fungible app ID (64 hex)
LINERA_BRIDGE_CHAIN_ID=...            # 64 hex
LINERA_BRIDGE_CHAIN_OWNER=0x...       # AccountOwner that owns bridge chain
LINERA_WALLET=/data/wallet.json
LINERA_KEYSTORE=/data/keystore.json
LINERA_STORAGE=rocksdb:/data/client.db
MONITOR_SCAN_INTERVAL=30
MONITOR_START_BLOCK=...               # FungibleBridge deploy block
MAX_RETRIES=10
MAX_LOG_BLOCK_RANGE=2000              # eth_getLogs chunk; match your RPC's cap
PORT=3001
```

All of these EVM contracts (`LightClient`, `FungibleBridge`,
`LineraToken`-or-real-ERC20), the bridge chain, and the two Linera apps
(`evm-bridge`, `wrapped-fungible`) are deployed and cross-registered by the
runbook steps; `deploy/out/<network>/relayer.env` already contains the
filled-in values (it is the same file shown here).

For the EVM contract deployment itself — the `forge script` invocations,
their inputs, the governance (pause-guardian / proposer / canceller Safes
and timelock) wiring, post-deploy verification, and the upgrade / pause /
migration runbooks — see
[`linera-bridge/DEPLOYMENT.md`](../linera-bridge/DEPLOYMENT.md).

The `linera-bridge` container will read `/etc/linera-bridge/.env` and
`/etc/linera-bridge/.env.secret` at startup.

#### Optional: explorer verification on contract deploys

To publish verified contract source to a block explorer, append the explorer
flags to the `forge script` commands in the runbook, e.g. for Base Sepolia:

```bash
--verify --etherscan-api-key <KEY> --verifier-url https://api-sepolia.basescan.org/api
```

See the [deploy README](../linera-bridge/deploy/README.md#per-network-notes)
for per-network verifier URLs.

### 4. Start the relayer

The relayer runs from the `linera-bridge` image. Pick the path that matches your
environment.

#### Local / without GCP

Run the locally-built image directly against the deploy outputs — no Secret
Manager, no compose. With `$OUT` and `EVM_PRIVATE_KEY` set as in the deploy
runbook (or substitute the paths/key directly):

```bash
docker run -d --name linera-relay \
  --env-file "$OUT/relayer.env" \
  -e EVM_PRIVATE_KEY \
  -v "$OUT/wallet:/data" \
  -p 3001:3001 \
  linera-bridge
```

This is the recommended path for local and single-operator setups. See the
deploy runbook's [Run the relayer](../linera-bridge/deploy/README.md#run-the-relayer)
section for logs / health / stop.

#### Production (GCP VM)

`docker-compose.bridge-testnet.yml` adds a `fetch-evm-key` sidecar that pulls
`EVM_PRIVATE_KEY` from **GCP Secret Manager** and runs the published registry
image; it requires `GCP_PROJECT_ID` and the VM layout from steps 1–2. On the
provisioned VM:

```bash
docker compose -f docker/docker-compose.bridge-mainnet.yml up -d
```

Verify it came up healthy:

```bash
docker compose -f docker/docker-compose.bridge-mainnet.yml ps
# State should be 'running (healthy)' after ~60s.
curl -sI http://localhost:3001/health | head -1
# Expected: HTTP/1.1 200 OK
curl -s http://localhost:3001/metrics | grep '^linera_bridge_' | head -10
# Expected: a handful of linera_bridge_* metrics in Prometheus text format
```

## Routine operations

### Restart (after VM reboot or image update)

```bash
docker compose -f docker/docker-compose.bridge-mainnet.yml up -d
```

Idempotent. Wallet, chain IDs, contracts persist across restarts.

### Image update

```bash
docker compose -f docker/docker-compose.bridge-mainnet.yml pull
docker compose -f docker/docker-compose.bridge-mainnet.yml up -d
```

### Top up Base gas

The relayer signs `addBlock` transactions on Base. When ETH
runs low, bridging stalls. Send ETH to the relayer's address (the
public address of `EVM_PRIVATE_KEY`) — no relayer restart required.

To find the address from the secret:

```bash
sudo cast wallet address \
  --private-key "$(grep ^EVM_PRIVATE_KEY /etc/linera-bridge/.env.secret | cut -d= -f2-)"
```

### Inspect logs

```bash
journalctl CONTAINER_TAG=linera-bridge -f
# or
docker compose -f docker/docker-compose.bridge-mainnet.yml logs -f
```

### Inspect pending bridge requests

The relay splits live work and history into separate tables: `pending_*`
holds work the relay is currently chasing; `finished_*` holds completed
or permanently-failed entries with a `status` column.

```bash
# live work queue
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  'SELECT * FROM pending_deposits;'
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  'SELECT * FROM pending_burns;'

# permanent failures (status = 'failed' or 'completed')
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  "SELECT * FROM finished_deposits WHERE status = 'failed';"
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  "SELECT * FROM finished_burns WHERE status = 'failed';"
```

(Check the actual schema with `.schema` if column lists differ.)

## Backup

Critical: loss of `/var/lib/linera-bridge` means loss of the Linera
wallet that owns the bridge chain. The bridge becomes unusable and
**re-provisioning is the only recovery** (which means a fresh bridge,
new contract addresses, new chain, etc. — not a real recovery).

Nightly backup:

```bash
# SQLite needs the .backup pragma for a consistent snapshot
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  ".backup /tmp/bridge_relay.sqlite3.bak"
sudo rsync -a --delete /var/lib/linera-bridge/ \
  backup-host:/snapshots/linera-bridge-$(date +%F)/
sudo rsync /tmp/bridge_relay.sqlite3.bak \
  backup-host:/snapshots/linera-bridge-$(date +%F)/bridge_relay.sqlite3
sudo rm /tmp/bridge_relay.sqlite3.bak
```

Also back up `/etc/linera-bridge/` (both env files). Without those
the deployed artifacts are unrecoverable as well.

## Observability

The relayer exposes Prometheus metrics on `http://127.0.0.1:3001/metrics`.
Key metrics for Mainnet operations:

All metrics are namespaced `linera_bridge_*`:

| Metric                                                          | Type     | Use                                                   |
|-----------------------------------------------------------------|----------|-------------------------------------------------------|
| `linera_bridge_evm_balance_wei`                                 | Gauge    | Alert when low (e.g., `< 1e16` = 0.01 ETH)            |
| `linera_bridge_linera_balance_atto`                             | Gauge    | Alert when low (e.g., `< 1e18`)                       |
| `linera_bridge_deposits_pending`, `linera_bridge_burns_pending` | IntGauge | Should drain; if growing, check logs                  |
| `linera_bridge_deposits_failed`, `linera_bridge_burns_failed`   | IntGauge | Any > 0 → investigate via SQLite                      |
| `linera_bridge_deposits_detected`, `linera_bridge_burns_detected`     | Counter | Total seen by scanners (cumulative)             |
| `linera_bridge_deposits_completed`, `linera_bridge_burns_completed`   | Counter | Total successfully processed (cumulative)       |
| `linera_bridge_last_scanned_evm_block`                          | IntGauge | Should track Base head                        |
| `linera_bridge_last_scanned_linera_height`                      | IntGauge | Should track Linera bridge chain head                 |

Suggested alert rules (apply on the external Prometheus):

```yaml
- alert: LineraBridgeGasBalanceLow
  expr: linera_bridge_evm_balance_wei < 1e16
  for: 5m

- alert: LineraBridgeLineraBalanceLow
  expr: linera_bridge_linera_balance_atto < 1e18
  for: 5m

- alert: LineraBridgeDown
  expr: up{job="linera-bridge"} == 0
  for: 2m

# Permanent failure: relay marked items as terminally failed.
- alert: LineraBridgePermanentFailure
  expr: linera_bridge_deposits_failed > 0 or linera_bridge_burns_failed > 0
  for: 15m

# Throughput stall: pending work exists but nothing is being completed.
# Catches a stuck relay even when no item has been marked permanently failed yet.
- alert: LineraBridgePendingStuck
  expr: |
    (linera_bridge_deposits_pending > 0
     and rate(linera_bridge_deposits_completed[15m]) == 0)
    or
    (linera_bridge_burns_pending > 0
     and rate(linera_bridge_burns_completed[15m]) == 0)
  for: 15m
```

## Troubleshooting

| Symptom                                          | Likely cause                                   | Action                                                                          |
|--------------------------------------------------|------------------------------------------------|---------------------------------------------------------------------------------|
| Container restart-loop                           | Missing/malformed `/etc/linera-bridge/.env`    | Check `docker compose ... logs` for clap parse errors                            |
| `/health` returns connection refused             | Relayer process exited                         | `restart: unless-stopped` will re-launch on exit; check logs and `ps`            |
| Container "unhealthy" but listener still open    | Process hung (deadlock, RPC wedge)             | `docker compose restart relayer`; investigate hang in logs                       |
| `linera_bridge_evm_balance_wei` reads 0          | RPC unreachable, or wrong key                  | Check `RPC_URL`, verify key with `cast wallet address`                           |
| Pending deposits/burns stuck                     | EVM gas too low, or RPC errors                 | Top up gas; check logs for retry messages                                        |
| Slow startup, no metrics for minutes             | RocksDB cache rebuilding from chain history    | Wait; check `linera_bridge_last_scanned_linera_height` is climbing               |
| `eth_getLogs ... exceeds max block range`        | RPC caps the `getLogs` block range             | Lower `MAX_LOG_BLOCK_RANGE` (≤ the RPC's cap; 2000 for public Base Sepolia) and restart |
