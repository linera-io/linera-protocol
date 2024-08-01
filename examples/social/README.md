<!-- cargo-rdme start -->

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

## Usage

To try it out, build Linera and add it to your path:

```bash
cargo build
PATH=$PWD/target/debug:$PATH
```

Then, using the helper function defined by `linera net helper`, set up a local network
with two wallets and define variables holding their wallet paths (`$LINERA_WALLET_0`,
`$LINERA_WALLET_1`) and storage paths (`$LINERA_STORAGE_0`, `$LINERA_STORAGE_1`).

```bash
eval "$(linera net helper)"
linera_spawn_and_read_wallet_variables \
    linera net up \
        --extra-wallets 1
```

Compile the `social` example and create an application with it:

```bash
linera --with-wallet 0 project publish-and-create examples/social
```

This will output the new application ID, e.g.:

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
```

With the `wallet show` command you can find the ID of the application creator's chain:

```bash
linera --with-wallet 0 wallet show
```

```rust
e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
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

Point your browser to http://localhost:8081. This is the wallet that didn't create the
application, so we have to request it from the creator chain. As the chain ID specify the
one of the chain where it isn't registered yet:

```gql,uri=http://localhost:8081
mutation {
    requestApplication(
        chainId: "1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03",
        applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000"
    )
}
```

Now in both http://localhost:8080 and http://localhost:8081, this should list the
application and provide a link to its GraphQL API. Remember to use each wallet's chain ID:

```gql,uri=http://localhost:8081
query {
    applications(
        chainId: "1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03"
    ) {
        id
        link
    }
}
```

Open both URLs under the entry `link`. Now you can use the application on each chain.
E.g. [in the 8081 tab](http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000)
subscribe to the other chain:

```gql,uri=http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
mutation {
    subscribe(
        chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
    )
}
```

Now make a post [in the 8080 tab](http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000):

```gql,uri=http://localhost:8080/chains/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
mutation {
    post(
        text: "Linera Social is the new Mastodon!"
        imageUrl: "https://linera.org/img/logo.svg" # optional
    )
}
```

Since 8081 is a subscriber. Let's see if it received any posts: # You can see the post on running the [web-frontend](./web-frontend/), or follow the steps below.

```gql,uri=http://localhost:8081/chains/1db1936dad0717597a7743a8353c9c0191c14c3a129b258e9743aec2b4f05d03/applications/e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
query { receivedPosts { keys { timestamp author index } } }
```

This should now list one entry, with timestamp, author and an index. If we view that
entry, we can see the posted text as well as other values:

```gql
query {
  receivedPosts {
    entry(
      key: {
        timestamp: 1705504131018960
        author: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
        index: 0
      }
    ) {
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
            "author": "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
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

<!-- cargo-rdme end -->
