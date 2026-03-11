# Bridge Demo

End-to-end EVM-to-Linera bridge demo with a browser frontend, MetaMask
integration, and a relay server that automates deposit proof generation and
block forwarding.

## Prerequisites

- Docker & Docker Compose
- [pnpm](https://pnpm.io/)
- [MetaMask](https://metamask.io/) browser extension

## Quick Start

From the repo root:

```bash
# Build images, start the network, deploy contracts & Linera apps
make -C linera-bridge demo

# Start the frontend dev server
make -C linera-bridge demo-frontend
```

Open **http://localhost:5173** in your browser.

## MetaMask Setup (Local Anvil)

The demo runs a local Anvil EVM node. MetaMask doesn't know about it by
default, so you need to configure three things:

### 1. Add the Anvil network

Open MetaMask → **Settings → Networks → Add Network → Add a network manually**:

| Field            | Value                   |
|------------------|-------------------------|
| Network name     | Anvil                   |
| RPC URL          | `http://localhost:8545`  |
| Chain ID         | `31337`                 |
| Currency symbol  | ETH                     |

Save and switch to the new network.

### 2. Import the deployer account

The setup script deploys the MockERC20 token from Anvil's default account
(account 0), which holds the entire token supply (1000 TT). You need to
import this account into MetaMask to have tokens available for deposits.

Open MetaMask → **Account menu → Import account → Paste private key**:

```
0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

This is Anvil's well-known dev key — never use it on a real network.

### 3. Import the ERC-20 token

After setup completes, the token contract address is printed in the terminal
and written to `.env.local` as `LINERA_TOKEN_ADDRESS`. To see your balance in
MetaMask:

Open MetaMask → **Tokens → Import tokens**:

| Field           | Value                                      |
|-----------------|--------------------------------------------|
| Token address   | the `LINERA_TOKEN_ADDRESS` from `.env.local` |
| Token symbol    | TT                                         |
| Decimals        | 18                                         |

You should see a balance of **1000 TT**.

## Usage

1. Connect MetaMask in the frontend (it will prompt to switch to Anvil).
2. Enter an amount and click **Deposit to Linera**.
3. MetaMask will ask you to approve the ERC-20 spend, then confirm the
   deposit transaction.
4. The relay picks up the deposit event, generates an MPT proof, and submits
   it to the bridge chain on Linera. Watch relay logs with:
   ```bash
   make -C linera-bridge demo-logs
   ```

## Useful Commands

```bash
make -C linera-bridge demo            # full build + deploy
make -C linera-bridge demo-frontend   # start Vite dev server
make -C linera-bridge demo-logs       # tail relay logs
make -C linera-bridge down            # stop everything and remove volumes
```

## Troubleshooting

- **MetaMask shows 0 ETH**: make sure you switched to the Anvil network
  and imported the correct private key.
- **Token balance is 0**: double-check that you imported the token using the
  address from `.env.local`, and that you are on the deployer account.
- **Deposit hangs**: check relay logs — the relay needs to be running and
  healthy before submitting deposits.
