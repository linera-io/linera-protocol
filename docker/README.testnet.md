# Linera Bridge — Testnet Deployment Runbook

This describes how to provision and operate a `linera-bridge` relayer
on a single VM, bridging Base Sepolia (EVM) and Testnet Conway (Linera).

For the full design rationale, see [`SPEC-linera-bridge-testnet-deployment.md`](../SPEC-linera-bridge-testnet-deployment.md)
in the repo root.

## Prerequisites

- A Linux VM with Docker + docker-compose
- Foundry (`forge`, `cast`) and a recent `linera` binary on the VM (or
  available inside docker images)
- Outbound network access to:
  - Base Sepolia RPC (`https://sepolia.base.org` or operator's provider)
  - Testnet Conway faucet (`https://faucet.testnet-conway.linera.net`)
- A funded Base Sepolia account (private key) for contract deployment
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

### 3. Provision contracts and Linera apps

Run setup.sh as the `linera-bridge` user so the wallet/keystore/storage
files are owned by the same uid that the container runs as. The
`--relayer-env` output goes to `/etc/linera-bridge/.env` which is root-
writable, so allow that one path to the linera-bridge user via sudo or
write the env file to a temporary location and `sudo cp` it into place.
The simplest path:

```bash
# Make /etc/linera-bridge writable by linera-bridge for this one provision
sudo chown linera-bridge:linera-bridge /etc/linera-bridge
sudo -u linera-bridge bash <<'EOF'
cd examples/bridge-demo
./setup.sh \
  --evm-rpc-url https://sepolia.base.org \
  --evm-chain-id 84532 \
  --evm-private-key "$EVM_PRIVATE_KEY" \
  --faucet-url https://faucet.testnet-conway.linera.net \
  --linera-wallet /var/lib/linera-bridge/wallet.json \
  --linera-keystore /var/lib/linera-bridge/keystore.json \
  --linera-storage rocksdb:/var/lib/linera-bridge/client.db \
  --shared-dir /var/lib/linera-bridge/shared \
  --relayer-env /etc/linera-bridge/.env
EOF
# Restore tighter ownership on /etc/linera-bridge
sudo chown root:root /etc/linera-bridge
```

This deploys `LightClient`, `MockERC20`, `FungibleBridge` on Base
Sepolia, claims a Linera bridge chain on Conway, publishes the
`evm-bridge` and `wrapped-fungible` apps, cross-registers their IDs,
and writes `/etc/linera-bridge/.env` plus the wallet/keystore/storage
under `/var/lib/linera-bridge/`. Provisioning artifacts (LightClient
constructor args, intermediate addresses) land in
`/var/lib/linera-bridge/shared/` for inspection.

`setup.sh` will run `linera wallet init --faucet` and `linera wallet
request-chain --faucet` automatically when the wallet doesn't yet
exist at `--linera-wallet`, so you do NOT need to pre-procure
`--linera-bridge-chain-id` or `--relay-owner`.

### 4. Start the relayer

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml up -d
```

Verify it came up healthy:

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml ps
# State should be 'running (healthy)' after ~60s.
curl -sI http://localhost:3001/health | head -1
# Expected: HTTP/1.1 200 OK
curl -s http://localhost:3001/metrics | grep '^linera_bridge_' | head -10
# Expected: a handful of linera_bridge_* metrics in Prometheus text format
```

## Routine operations

### Restart (after VM reboot or image update)

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml up -d
```

Idempotent. Wallet, chain IDs, contracts persist across restarts.

### Image update

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml pull
docker compose -f docker/docker-compose.bridge-testnet.yml up -d
```

### Top up Base Sepolia gas

The relayer signs `addBlock` transactions on Base Sepolia. When ETH
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
docker compose -f docker/docker-compose.bridge-testnet.yml logs -f
```

### Inspect pending bridge requests

```bash
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  'SELECT * FROM deposits WHERE status != "completed";'
sudo sqlite3 /var/lib/linera-bridge/bridge_relay.sqlite3 \
  'SELECT * FROM burns WHERE status != "completed";'
```

(Schema names are illustrative; check actual schema with `.schema`.)

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
Key metrics for testnet operations:

All metrics are namespaced `linera_bridge_*`:

| Metric                                                     | Type        | Use                                          |
|------------------------------------------------------------|-------------|----------------------------------------------|
| `linera_bridge_evm_balance_wei`                            | Gauge       | Alert when low (e.g., `< 1e16` = 0.01 ETH)   |
| `linera_bridge_linera_balance_atto`                        | Gauge       | Alert when low (e.g., `< 1e18`)              |
| `linera_bridge_deposits_pending`, `linera_bridge_burns_pending` | IntGauge | Should drain; if growing, check logs        |
| `linera_bridge_deposits_failed`, `linera_bridge_burns_failed`   | IntGauge | Any > 0 → investigate via SQLite            |
| `linera_bridge_last_scanned_evm_block`                     | IntGauge    | Should track Base Sepolia head               |
| `linera_bridge_last_scanned_linera_height`                 | IntGauge    | Should track Linera bridge chain head        |

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

- alert: LineraBridgePendingStuck
  expr: linera_bridge_deposits_failed > 0 or linera_bridge_burns_failed > 0
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
