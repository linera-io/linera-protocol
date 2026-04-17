# Linera Bridge — Testnet Deployment Runbook

This describes how to provision and operate a `linera-bridge` relayer
on a single VM, bridging Base Sepolia (EVM) and Testnet Conway (Linera).

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

```bash
cd examples/bridge-demo
./setup.sh \
  --evm-rpc-url https://sepolia.base.org \
  --evm-chain-id 84532 \
  --evm-private-key "$EVM_PRIVATE_KEY" \
  --faucet-url https://faucet.testnet-conway.linera.net \
  --linera-wallet /var/lib/linera-bridge/wallet.json \
  --linera-keystore /var/lib/linera-bridge/keystore.json \
  --linera-storage rocksdb:/var/lib/linera-bridge/client.db \
  --relayer-env /etc/linera-bridge/.env
```

This deploys `LightClient`, `MockERC20`, `FungibleBridge` on Base
Sepolia, claims a Linera bridge chain on Conway, publishes the
`evm-bridge` and `wrapped-fungible` apps, cross-registers their IDs,
and writes `/etc/linera-bridge/.env` plus the wallet/keystore/storage
under `/var/lib/linera-bridge/`.

### 4. Start the relayer

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml up -d
```

Verify it came up healthy:

```bash
docker compose -f docker/docker-compose.bridge-testnet.yml ps
# State should be 'running (healthy)' after ~60s.
curl -s http://localhost:3001/health
# Expected: 200 OK with empty body.
curl -s http://localhost:3001/metrics | head -20
# Expected: Prometheus text format with bridge_* metrics.
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

| Metric                          | Type        | Use                                          |
|---------------------------------|-------------|----------------------------------------------|
| `RELAYER_EVM_BALANCE_WEI`       | Gauge       | Alert when low (e.g., `< 1e16` = 0.01 ETH)   |
| `RELAYER_LINERA_BALANCE_ATTO`   | Gauge       | Alert when low (e.g., `< 1e18`)              |
| `DEPOSITS_PENDING`, `BURNS_PENDING` | IntGauge | Should drain; if growing, check logs        |
| `DEPOSITS_FAILED`, `BURNS_FAILED`   | IntGauge | Any > 0 → investigate via SQLite            |
| `LAST_SCANNED_EVM_BLOCK`        | IntGauge    | Should track Base Sepolia head               |
| `LAST_SCANNED_LINERA_HEIGHT`    | IntGauge    | Should track Linera bridge chain head        |

Suggested alert rules (apply on the external Prometheus):

```yaml
- alert: LineraBridgeGasBalanceLow
  expr: RELAYER_EVM_BALANCE_WEI < 1e16
  for: 5m

- alert: LineraBridgeLineraBalanceLow
  expr: RELAYER_LINERA_BALANCE_ATTO < 1e18
  for: 5m

- alert: LineraBridgeDown
  expr: up{job="linera-bridge"} == 0
  for: 2m

- alert: LineraBridgePendingStuck
  expr: DEPOSITS_FAILED > 0 OR BURNS_FAILED > 0
  for: 15m
```

## Troubleshooting

| Symptom                                 | Likely cause                                   | Action                                                    |
|-----------------------------------------|------------------------------------------------|-----------------------------------------------------------|
| Container restart-loop                  | Missing/malformed `/etc/linera-bridge/.env`    | Check `docker compose ... logs` for clap parse errors      |
| `/health` returns connection refused    | Relayer process exited                         | Check logs; container should auto-restart                  |
| `RELAYER_EVM_BALANCE_WEI` reads 0       | RPC unreachable, or wrong key                  | Check `RPC_URL`, verify key with `cast wallet address`    |
| Pending deposits/burns stuck            | EVM gas too low, or RPC errors                 | Top up gas; check logs for retry messages                  |
| Slow startup, no metrics for minutes    | RocksDB cache rebuilding from chain history    | Wait; check `LAST_SCANNED_LINERA_HEIGHT` is climbing       |
