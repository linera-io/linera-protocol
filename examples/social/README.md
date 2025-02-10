# A Social Media Example Application

This example illustrates how to use channels for cross-chain messages.

For simplicity, each microchain represents one user—its owner. They can subscribe to other
users and make text posts that get sent to their subscribers.

## How it Works

The application's state on every microchain contains a set of posts created by this chain
owner, and a set of posts received from other chains that it has subscribed to. The
received posts are indexed by timestamp, sender and index.

There are `Subscribe` and `Unsubscribe` operations: If a chain owner includes these in a
new block, they can subscribe to or unsubscribe from another chain.

There is also a `Post` operation: It creates a new post and sends it to a channel, so that
it reaches all subscribers.

There are corresponding `Subscribe`, `Unsubscribe` and `Posts` cross-chain
message variants that are created when these operations are handled. The first two are
sent directly to the chain we want to subscribe to or unsubscribe from. The latter goes
to the channel.

<!--
TODO the following documentation involves `sleep`ing to avoid some race conditions. See:
 - https://github.com/linera-io/linera-protocol/issues/1176
 - https://github.com/linera-io/linera-protocol/issues/1177
-->

## Usage

Set up the path and the helper function.

```bash
PATH=$PWD/target/debug:$PATH
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"
```

Then, using the helper function defined by `linera net helper`, set up a local network
with two wallets and define variables holding their wallet paths (`$LINERA_WALLET_0`,
`$LINERA_WALLET_1`) and storage paths (`$LINERA_STORAGE_0`, `$LINERA_STORAGE_1`).

```bash
linera_spawn_and_read_wallet_variables \
    linera net up \
        --extra-wallets 1
```

Compile the `social` example and create an application with it:

```bash
APP_ID=$(linera --with-wallet 0 project publish-and-create examples/social)
```

This will output the new application ID, e.g.:

```rust
20dc1ed01670501414eb59defe71f9a7b817fe2da18fcdd5e704bbac32f5bd97a020afcb645167bcaacd1d8cee9083605f42a95de24e056f3a958c7ca91f9f728ac320aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8020000000000000000000000
```

With the `wallet show` command you can find the ID of the application creator's chain:

```bash
linera --with-wallet 0 wallet show

CHAIN_1=582843bc9322ed1928239ce3f6a855f6cd9ea94c8690907f113d6d7a8296a119
CHAIN_2=aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8
```

```rust
aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8
```

Now start a node service for each wallet, using two different ports:

```bash
linera --with-wallet 0 service --port 8080 &

# Wait for it to complete
sleep 2

linera --with-wallet 1 service --port 8081 &

# Wait for it to complete
sleep 2
```

Type each of these in the GraphiQL interface and substitute the env variables with their actual values that we've defined above.

Point your browser to http://localhost:8081. This is the wallet that didn't create the
application, so we have to request it from the creator chain. As the chain ID specifies the
one of the chain where it isn't registered yet:

```gql,uri=http://localhost:8081
mutation {
  requestApplication(
    chainId: "$CHAIN_1",
    applicationId: "$APP_ID"
  )
}
```

Now in both http://localhost:8080 and http://localhost:8081, this should list the
application and provide a link to its GraphQL API. Remember to use each wallet's chain ID:

```gql,uri=http://localhost:8081
query {
  applications(
    chainId: "$CHAIN_1"
  ) {
    id
    link
  }
}
```

Open both URLs under the entry `link`. Now you can use the application on each chain.
For the 8081 tab, you can run `echo "http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID"`
to print the URL to navigate to, then subscribe to the other chain using the following query:

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID
mutation {
  subscribe(
    chainId: "$CHAIN_2"
  )
}
```

Run `echo "http://localhost:8080/chains/$CHAIN_2/applications/$APP_ID"` to print the URL to navigate to, then make a post:

```gql,uri=http://localhost:8080/chains/$CHAIN_2/applications/$APP_ID
mutation {
  post(
    text: "Linera Social is the new Mastodon!"
    imageUrl: "https://linera.org/img/logo.svg" # optional
  )
}
```

Since 8081 is a subscriber. Let's see if it received any posts: # You can see the post on running the [web-frontend](./web-frontend/), or follow the steps below.

```gql,uri=http://localhost:8081/chains/$CHAIN_1/applications/$APP_ID
query { receivedPosts { keys { timestamp author index } } }
```

This should now list one entry, with a timestamp, author and an index. If we view that
entry, we can see the posted text as well as other values:

```gql
query {
  receivedPosts {
    entry(key: { timestamp: 1705504131018960, author: "$CHAIN_2", index: 0 }) {
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
            "author": "$CHAIN_2",
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
