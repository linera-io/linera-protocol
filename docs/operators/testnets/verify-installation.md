# Verifying installation

To verify the installation, you can use the `linera query-validator` command.
For example:

```bash
$ linera wallet init --faucet https://faucet.{{#include ../../RELEASE_DOMAIN}}.linera.net
$ linera wallet request-chain --faucet https://faucet.{{#include ../../RELEASE_DOMAIN}}.linera.net
$ linera query-validator grpcs:my-domain.com:443

RPC API hash: kd/Ru73B4ZZjXYkFqqSzoWzqpWi+NX+8IJLXOODjSko
GraphQL API hash: eZqzuBlLT0bcoQUjOCPf2j22NfZUWG95id4pdlUmhgs
WIT API hash: 4/gsw8G+47OUoEWK6hJRGt9R69RanU/OidmX7OKhqfk
Source code: https://github.com/linera-io/linera-protocol/tree/

0cd20d06af5262540535347d4cc6e5952a921d1a6a7f6dd0982159c9311cfb3e
```

The last line is the hash of the network's genesis configuration. If this
command exits successfully your validator is now operational and ready to be
on-boarded.
