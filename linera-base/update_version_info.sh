#!/usr/bin/env bash

set -e

cd "$(dirname $(cargo locate-project --workspace --message-format plain))"

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

#if [ -n "$GIT_COMMIT" ]
#then
#    echo GIT_COMMIT=$GIT_COMMIT
#else
#    # git commit
#    git=$(git rev-parse @)
#    git diff-index --quiet @ || git="$git-dirty"
#    echo -n $git > linera-base/git_commit.txt
#fi


# GraphQL API hash
cat linera-service-graphql-client/gql/*.graphql | $hash | awk '{print $1}' | tr -d "\n" > linera-base/graphql_hash.txt

# WIT API hash
cat linera-sdk/*.wit | $hash | awk '{print $1}' | tr -d "\n" > linera-base/wit_hash.txt

# RPC API hash
$hash linera-rpc/tests/staged/formats.yaml | awk '{print $1}' | tr -d "\n" > linera-base/rpc_hash.txt
