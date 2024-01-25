#!/usr/bin/env bash

cd $(git rev-parse --show-toplevel)

if type -P sha256sum &>/dev/null
then
    hash=sha256sum
elif type -P shasum &>/dev/null
then
    hash="shasum -a 256"
else
    >&2 echo "No SHA256-sum implementation found"
    exit 1
fi

# git commit
git=$(git rev-parse @)
git diff-index --quiet @ || git="$git-dirty"
echo GIT_COMMIT=$git

{
    # GraphQL API hash
    echo -n GRAPHQL_HASH=
    cat linera-service-graphql-client/gql/*.graphql | $hash

    # WIT API hash
    echo -n WIT_HASH=
    cat linera-sdk/*.wit | $hash

    # RPC API hash
    echo -n RPC_HASH=
    $hash linera-rpc/tests/staged/formats.yaml
} | awk '{print $1}'
