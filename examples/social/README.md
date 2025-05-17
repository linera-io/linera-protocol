# A Social Media Example Application

This example illustrates how to use event streams.

For simplicity, each microchain represents one user—its owner. They can subscribe to other
users and make text posts that get sent to their subscribers.

## How it Works

The application's state on every microchain contains a set of posts created by this chain
owner, and a set of posts received from other chains that it has subscribed to. The
received posts are indexed by timestamp, sender and index.

There are `Subscribe` and `Unsubscribe` operations: If a chain owner includes these in a
new block, they can subscribe to or unsubscribe from another chain.

There is also a `Post` operation, which creates a new post and sends it to a stream so that all subscribers receive it.
Additionally, there are two operations: `Like` and `Comment`. When a subscriber likes or comments on a post, the action is sent to the post's author(ChainId), who processes it accordingly.

There are corresponding `Subscribe`, `Unsubscribe` and `Posts` cross-chain
message variants that are created when these operations are handled. The first two are
sent directly to the chain we want to subscribe to or unsubscribe from. The latter goes
to the stream.

<!--
TODO the following documentation involves `sleep`ing to avoid some race conditions. See:
 - https://github.com/linera-io/linera-protocol/issues/1176
 - https://github.com/linera-io/linera-protocol/issues/1177
-->

## Usage

Set up the path and the helper function. From the root of Linera repository, this can be
achieved as follows:

```bash
export PATH="$PWD/target/debug:$PATH"
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Start the local Linera network and run a faucet:

```bash
FAUCET_PORT=8079
FAUCET_URL=http://localhost:$FAUCET_PORT
linera_spawn linera net up --with-faucet --faucet-port $FAUCET_PORT

# If you're using a testnet, run this instead:
#   LINERA_TMP_DIR=$(mktemp -d)
#   FAUCET_URL=https://faucet.testnet-XXX.linera.net  # for some value XXX
```

Create the user wallets and add chains to them:

```bash
export LINERA_WALLET_1="$LINERA_TMP_DIR/wallet_1.json"
export LINERA_KEYSTORE_1="$LINERA_TMP_DIR/keystore_1.json"
export LINERA_STORAGE_1="rocksdb:$LINERA_TMP_DIR/client_1.db"
export LINERA_WALLET_2="$LINERA_TMP_DIR/wallet_2.json"
export LINERA_KEYSTORE_2="$LINERA_TMP_DIR/keystore_2.json"
export LINERA_STORAGE_2="rocksdb:$LINERA_TMP_DIR/client_2.db"

linera --with-wallet 1 wallet init --faucet $FAUCET_URL
linera --with-wallet 2 wallet init --faucet $FAUCET_URL

INFO_1=($(linera --with-wallet 1 wallet request-chain --faucet $FAUCET_URL))
INFO_2=($(linera --with-wallet 2 wallet request-chain --faucet $FAUCET_URL))
CHAIN_1="${INFO_1[0]}"
CHAIN_2="${INFO_2[0]}"
OWNER_1="${INFO_1[3]}"
OWNER_2="${INFO_2[3]}"
```

Note that `linera --with-wallet 1` is equivalent to `linera --wallet "$LINERA_WALLET_1" --keystore "$LINERA_KEYSTORE_1"
--storage "$LINERA_STORAGE_1"`.

Compile the `social` example and create an application with it:

```bash
APP_ID=$(linera --with-wallet 1 project publish-and-create examples/social)
```

Now start a node service for each wallet, using two different ports:

```bash
linera --with-wallet 1 service --port 8080 &

# Wait for it to complete
sleep 2

linera --with-wallet 2 service --port 8081 &

# Wait for it to complete
sleep 2
```

Open both URLs under the entry `link`. Now you can use the application on each chain.
For the 8081 tab, you can run `echo "http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID"`
to print the URL to navigate to, then subscribe to the other chain using the following query:

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID
mutation {
  subscribe(
    chainId: "$CHAIN_1"
  )
}
```

Run `echo "http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID"` to print the URL to navigate to, then make a post:

```gql,uri=http://localhost:8080/chains/$CHAIN_1/applications/$APP_ID
mutation {
  post(
    text: "Linera Social is the new Mastodon!"
    imageUrl: "https://linera.org/img/logo.svg" # optional
  )
}
```

Since 8081 is a subscriber. Let's see if it received any posts: # You can see the post on running the [web-frontend](./web-frontend/), or follow the steps below.

```gql,uri=http://localhost:8081/chains/$CHAIN_2/applications/$APP_ID
query { receivedPosts { keys { timestamp author index } } }
```

This should now list one entry, with a timestamp, author and an index. If we view that
entry, we can see the posted text as well as other values:

```gql
query {
  receivedPosts {
    entry(key: { timestamp: 1705504131018960, author: "$CHAIN_1", index: 0 }) {
      value {
        key {
          timestamp
          author
          index
        }
        text
        imageUrl
        comments {
          text
          chainId
        }
        likes
      }
    }
  }
}
```

```json
{
  "data": {
    "receivedPosts": {
      "entry": {
        "value": {
          "key": {
            "timestamp": 1705504131018960,
            "author": "$CHAIN_1",
            "index": 0
          },
          "text": "Linera Social is the new Mastodon!",
          "imageUrl": "https://linera.org/img/logo.svg",
          "comments": [],
          "likes": 0
        }
      }
    }
  }
}
```
