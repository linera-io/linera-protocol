## Connecting with external wallet providers

The Linera client library allows you to sign transactions with anything that
satisfies
[the `Signer` interface](https://github.com/linera-io/linera-protocol/tree/testnet_conway/web/%40linera/client/docs/interfaces).
This means you can integrate with external software wallets, hardware wallets,
Internet-connected wallet services… the only limit is your imagination!

To get started building your own signer implementation, have a look at
[our sample in-memory implementation](https://github.com/linera-io/linera-protocol/blob/testnet_conway/web/%40linera/client/src/signer/PrivateKey.ts).
Alternatively, you can use a pre-built wallet integration provided by Linera or
our partners.

### MetaMask

MetaMask is the most popular crypto wallet on the Web today. Though oriented
primarily at Ethereum, it's flexible enough to allow signing other types of data
too.

We provide an implementation using MetaMask's blind-signing capabilities to sign
Linera transactions in the
[`@linera/metamask`](https://www.npmjs.com/package/@linera/metamask) package on
npm. Our counter demo also sports a
[MetaMask-based frontend](https://github.com/linera-io/linera-protocol/tree/testnet_conway/examples/counter/metamask)
that exemplifies signing application transactions with MetaMask.

### Dynamic

[Dynamic](https://dynamic.xyz) provide a production-quality embedded wallet that
is fully compatible with Linera, and can be used to sign in securely using a
wide range of Web2 and Web3 identity providers.

Dynamic have made available both a
[recipe for Linera integration](https://www.dynamic.xyz/docs/guides/chains/linera)
and a
[fully-featured frontend](https://github.com/dynamic-labs/examples/tree/main/examples/vite-linera-counter),
based on the counter demo developed in this manual, that uses Dynamic to sign
transactions.
