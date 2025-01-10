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

Most of this can be referred to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#setting-up), except for at the end when compiling and publishing the bytecode, what you'll need to do will be slightly different.

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"

linera_spawn_and_read_wallet_variables linera net up --testing-prng-seed 37
```

Compile the `non-fungible` application WebAssembly binaries, and publish them as an application bytecode:

```bash
(cd examples/non-fungible && cargo build --release --target wasm32-unknown-unknown)

BYTECODE_ID=$(linera publish-bytecode \
    examples/target/wasm32-unknown-unknown/release/non_fungible_{contract,service}.wasm)
```

Here, we stored the new bytecode ID in a variable `BYTECODE_ID` to be reused later.

### Creating an NFT

Unlike fungible tokens, each NFT is unique and identified by a unique token ID. Also unlike fungible tokens, when creating the NFT application you don't need to specify an initial state or parameters. NFTs will be minted later.

Refer to the [fungible app README](https://github.com/linera-io/linera-protocol/blob/main/examples/fungible/README.md#creating-a-token) to figure out how to list the chains created for the test in the default wallet, as well as defining some variables corresponding to these values.

```bash
linera wallet show

CHAIN_1=e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65  # default chain for the wallet
OWNER_1=7136460f0c87ae46f966f898d494c4b40c4ae8c527f4d1c0b1fa0f7cff91d20f  # owner of chain 1
CHAIN_2=256e1dbc00482ddd619c293cc0df94d366afe7980022bb22d99e33036fd465dd  # another chain in the wallet
OWNER_2=598d18f67709fe76ed6a36b75a7c9889012d30b896800dfd027ee10e1afd49a3  # owner of chain 2
```

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
