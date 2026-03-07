# Wallets

As in traditional blockchains, Linera wallets are in charge of holding user
private keys. However, instead of signing transactions, Linera wallets are meant
to sign blocks and propose them to extend the chains owned by their users.

In practice, wallets include a node which tracks a subset of Linera chains. We
will see in the [next section](node_service.md) how a Linera wallet can run a
GraphQL service to expose the state of its chains to web frontends.

> The command-line tool `linera` is the main way for developers to interact with
> a Linera network and manage the developer wallets present locally on the
> system.

Note that this command-line tool is intended mainly for development purposes.
Our goal is that end users eventually manage their wallets in a browser
extension.

## Creating a developer wallet

The simplest way to obtain a wallet with the `linera` CLI tool is to run the
following command:

```bash
linera wallet init --faucet $FAUCET_URL
linera wallet request-chain --faucet $FAUCET_URL
```

where `$FAUCET_URL` represents the URL of the network's faucet (see
[previous section](../getting_started/hello_linera.html))

## Selecting a wallet

The private state of a wallet is conventionally stored in a file `wallet.json`,
keys are stored in `keystore.db`, while the state of its node is stored in a
file `wallet.db`.

To switch between wallets, you may use the `--wallet`, `--keystore`, and
`--storage` options of the `linera` tool, e.g. as in
`linera --wallet wallet2.json --keystore keystore2.json --storage rocksdb:wallet2.db:runtime:default`.

You may also define the environment variables `LINERA_STORAGE`,
`LINERA_KEYSTORE`, and `LINERA_WALLET` to the same effect. E.g.
`LINERA_STORAGE=rocksdb:$PWD/wallet2.db:runtime:default` and
`LINERA_WALLET=$PWD/wallet2.json`.

Finally, if `LINERA_STORAGE_$I`, `LINERA_KEYSTORE_$I`, and `LINERA_WALLET_$I`
are defined for some number `I`, you may call `linera --with-wallet $I` (or
`linera -w $I` for short).

## Chain management

### Listing chains

To list the chains present in your wallet, you may use the command `show`:

```bash
linera wallet show
╭──────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────╮
│ Chain ID                                                         ┆ Latest Block                                                                         │
╞══════════════════════════════════════════════════════════════════╪══════════════════════════════════════════════════════════════════════════════════════╡
│ 668774d6f49d0426f610ad0bfa22d2a06f5f5b7b5c045b84a26286ba6bce93b4 ┆ Public Key:         3812c2bf764e905a3b130a754e7709fe2fc725c0ee346cb15d6d261e4f30b8f1 │
│                                                                  ┆ Owner:              c9a538585667076981abfe99902bac9f4be93714854281b652d07bb6d444cb76 │
│                                                                  ┆ Block Hash:         -                                                                │
│                                                                  ┆ Timestamp:          2023-04-10 13:52:20.820840                                       │
│                                                                  ┆ Next Block Height:  0                                                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 91c7b394ef500cd000e365807b770d5b76a6e8c9c2f2af8e58c205e521b5f646 ┆ Public Key:         29c19718a26cb0d5c1d28102a2836442f53e3184f33b619ff653447280ccba1a │
│                                                                  ┆ Owner:              efe0f66451f2f15c33a409dfecdf76941cf1e215c5482d632c84a2573a1474e8 │
│                                                                  ┆ Block Hash:         51605cad3f6a210183ac99f7f6ef507d0870d0c3a3858058034cfc0e3e541c13 │
│                                                                  ┆ Timestamp:          2023-04-10 13:52:21.885221                                       │
│                                                                  ┆ Next Block Height:  1                                                                │
╰──────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────╯

```

Each row represents a chain present in the wallet. On the left is the unique
identifier on the chain, and on the right is metadata for that chain associated
with the latest block.

### Default chain

Each wallet has a default chain that all commands apply to unless you specify
another `--chain` on the command line.

The default chain is set initially, when the first chain is added to the wallet.
You can check the default chain for your wallet by running:

```bash
linera wallet show
```

The chain ID which is in green text instead of white text is your default chain.

To change the default chain for your wallet, use the `set-default` command:

```bash
linera wallet set-default <chain-id>
```

### Creating chains

In the Linera protocol, chains are generally created using a transaction from an
existing chain.

#### Create a chain from an existing one for your own wallet

To create a new chain from the default chain of your wallet, you can use the
`open-chain` command:

```bash
linera open-chain
```

This will create a new chain and add it to the wallet. Use the `wallet show`
command to see your existing chains.

#### Create a new chain from an existing one for another wallet

Creating a chain for another `wallet` requires an extra two steps. Let's
initialize a second wallet:

```bash
linera --wallet wallet2.json --storage rocksdb:linera2.db wallet init --faucet $FAUCET_URL
```

First `wallet2` must create an unassigned keypair. The public part of that
keypair is then sent to the `wallet` who is the chain creator.

```bash
linera --wallet wallet2.json keygen
6443634d872afbbfcc3059ac87992c4029fa88e8feb0fff0723ac6c914088888 # this is the public key for the unassigned keypair
```

Next, using the public key, `wallet` can open a chain for `wallet2`.

```bash
linera open-chain --to-public-key 6443634d872afbbfcc3059ac87992c4029fa88e8feb0fff0723ac6c914088888
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000000000000
fc9384defb0bcd8f6e206ffda32599e24ba715f45ec88d4ac81ec47eb84fa111
```

The first line is the message ID specifying the cross-chain message that creates
the new chain. The second line is the new chain's ID.

Finally, to add the chain to `wallet2` for the given unassigned key we use the
`assign` command:

```bash
 linera --wallet wallet2.json assign --key 6443634d872afbbfcc3059ac87992c4029fa88e8feb0fff0723ac6c914088888 --message-id e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000000000000
```

Note that in the case of a test network with a faucet, the new wallet and the
new chain could also have been created from the faucet directly using:

```bash
linera --wallet wallet2.json --storage rocksdb:linera2.db wallet init --faucet $FAUCET_URL
linera --wallet wallet2.json --storage rocksdb:linera2.db wallet request-chain --faucet $FAUCET_URL
```

#### Opening a chain with multiple users

The `open-chain` command is a simplified version of `open-multi-owner-chain`,
which gives you fine-grained control over the set and kinds of owners and rounds
for the new chain, and the timeout settings for the rounds. E.g. this creates a
chain with two owners and two multi-leader rounds.

```bash
linera open-multi-owner-chain \
    --chain-id e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000000000000 \
    --owner-public-keys 6443634d872afbbfcc3059ac87992c4029fa88e8feb0fff0723ac6c914088888 \
                        ca909dcf60df014c166be17eb4a9f6e2f9383314a57510206a54cd841ade455e \
    --multi-leader-rounds 2
```

The `change-ownership` command offers the same options to add or remove owners
and change round settings for an existing chain.
