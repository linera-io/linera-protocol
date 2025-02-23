# Non-Fungible Token Example Application

This example application implements non-fungible tokens (NFTs), showcasing the creation and management of unique digital assets. It highlights cross-chain messages, demonstrating how NFTs can be minted, transferred, and claimed across different chains, emphasizing the instantiation and auto-deployment of applications within the Linera blockchain ecosystem.

Once this application's bytecode is published on a Linera chain, that application will contain the registry of the different NFTs.

Some portions of this are very similar to the `fungible` README, and we'll refer to it throughout. Bash commands will always be included here for testing purposes.

## How It Works

Each chain maintains a subset of NFTs, represented as unique token identifiers. NFT ownership is tracked across one or multiple chains, allowing for rich, cross-chain interactions.

The application supports three primary operations: `Mint`, `Transfer`, and `Claim`.

`Mint` creates a new NFT within the application, assigning it to the minter.
`Transfer` changes the ownership of an NFT from one account to another, either within the same chain or across chains.
`Claim` sends a cross-chain message to transfer ownership of an NFT from a remote chain to the current chain.

NFTs can be transferred to various destinations, including:

- Other accounts on the same chain.
- The same account on a different chain.
- Other accounts on different chains.

## Usage

### Setting Up

Before getting started, make sure that the binary tools `linera*` corresponding to
your version of `linera-sdk` are in your PATH. For scripting purposes, we also assume
that the BASH function `linera_spawn` is defined.

From the root of Linera repository, this can be achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Next, start the local Linera network and run a faucet:

```bash
FAUCET_PORT=8079
FAUCET_URL=http://localhost:$FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallet and add chains to it:

```bash
export LINERA_WALLET="$LINERA_TMP_DIR/wallet.json"
export LINERA_STORAGE="rocksdb:$LINERA_TMP_DIR/client.db"

linera wallet init --faucet $FAUCET_URL

INFO_1=($(linera wallet request-chain --faucet $FAUCET_URL))
INFO_2=($(linera wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[3]}"
OWNER_2="${INFO_2[3]}"
```

```bash
(cd examples/non-fungible && cargo build --release --target wasm32-unknown-unknown)

BYTECODE_ID=$(linera publish-bytecode \
    examples/target/wasm32-unknown-unknown/release/non_fungible_{contract,service}.wasm)
```

Here, we stored the new bytecode ID in a variable `BYTECODE_ID` to be reused later.

### Creating an NFT

Unlike fungible tokens, each NFT is unique and identified by a unique token ID. Also unlike fungible tokens, when creating the NFT application you don't need to specify an initial state or parameters. NFTs will be minted later.

Refer to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#creating-a-token) to figure out how to list the chains created for the test in the default wallet, as well as defining some variables corresponding to these values.

To create the NFT application, run the command below:

```bash
APP_ID=$(linera create-application $BYTECODE_ID)
```

This will store the application ID in a new variable `APP_ID`.

### Using the NFT Application

Operations such as minting NFTs, transferring NFTs, and claiming NFTs from other chains follow a similar approach to fungible tokens, with adjustments for the unique nature of NFTs.

First, a node service for the current wallet has to be started:

```bash
PORT=8080
linera service --port $PORT &
```

#### Using GraphiQL

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

- Navigate to `http://localhost:8080/`.
- To publish a blob, run the mutation:

```gql,uri=http://localhost:8080/
mutation {
  publishDataBlob(
    chainId: "$CHAIN_1",
    bytes: [1, 2, 3, 4]
  )
}
```

Set the `QUERY_RESULT` variable to have the result returned by the previous query, and `BLOB_HASH` will be properly set for you.
Alternatively you can set the `BLOB_HASH` variable to the hash returned by the previous query yourself.

```bash
BLOB_HASH=$(echo "$QUERY_RESULT" | jq -r '.publishDataBlob')
```

- Run `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID"` to print the URL to navigate to.
- To mint an NFT, run the mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
mutation {
  mint(
    minter: "User:$OWNER_1",
    name: "nft1",
    blobHash: "$BLOB_HASH",
  )
}
```

- To check that it's assigned to the owner, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  ownedNfts(owner: "User:$OWNER_1")
}
```

Set the `QUERY_RESULT` variable to have the result returned by the previous query, and `TOKEN_ID` will be properly set for you.
Alternatively you can set the `TOKEN_ID` variable to the `tokenId` value returned by the previous query yourself.

```bash
TOKEN_ID=$(echo "$QUERY_RESULT" | jq -r '.ownedNfts[].tokenId')
```

- To check that it's there, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  nft(tokenId: "$TOKEN_ID") {
    tokenId,
    owner,
    name,
    minter,
    payload
  }
}
```

- To check everything that it's there, run the query:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
query {
  nfts
}
```

- To transfer the NFT to user `$OWNER_2`, still on chain `$CHAIN_1`, run the mutation:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
mutation {
  transfer(
    sourceOwner: "User:$OWNER_1",
    tokenId: "$TOKEN_ID",
    targetAccount: {
      chainId: "$CHAIN_1",
      owner: "User:$OWNER_2"
    }
  )
}
```

#### Using Web Frontend

Installing and starting the web server:

```bash
cd examples/non-fungible/web-frontend
npm install --no-save

# Start the server but not open the web page right away.
BROWSER=none npm start &
```

```bash
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_1&port=$PORT"
echo "http://localhost:3000/$CHAIN_1?app=$APP_ID&owner=$OWNER_2&port=$PORT"
```

For the final part, refer to [Fungible Token Example Application - Using web frontend](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#using-web-frontend).
