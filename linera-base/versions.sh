#!/usr/bin/env bash

cd $(git rev-parse --show-toplevel)

# git commit
git=$(git rev-parse @)
git diff-index --quiet @ || git="$git-dirty"
echo GIT=$git

# crates version
echo -n VER=
cargo metadata --format-version 1 | jq -r '(.workspace_members[0] / " ")[1]'

{
    # GraphQL API hash
    echo -n GQL=
    cat linera-service-graphql-client/gql/*.graphql | sha256sum

    # WIT API hash
    echo -n WIT=
    cat linera-sdk/*.wit | sha256sum

    # RPC API hash
    echo -n RPC=
    sha256sum linera-rpc/tests/staged/formats.yaml
} | awk '{print $1}'
