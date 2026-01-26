# Build arguments:
#
# - `git_commit` is the hash of the current git commit — used for
#   versioning information inside the binaries.
# - `build_date` is the date and time of when the docker image was
#   built
# - `binaries` is the path to the directory containing the Linera
#   binaries. Leave unset to build the binaries from scratch.
# - `build_flag` is either "--release" or an empty string.
# - `build_folder` is either "release" or "debug".
# - `build_features` is a comma-separated list of features to build the binaries with.

ARG git_commit
ARG build_date
ARG binaries=
ARG copy=${binaries:+_copy}
ARG build_flag=--release
ARG build_folder=release
ARG build_features=scylladb,metrics,memory-profiling
ARG rustflags="-C force-frame-pointers=yes"

FROM rust:1.74-slim-bookworm AS builder
ARG git_commit
ARG build_flag
ARG build_folder
ARG build_features
ARG rustflags

RUN apt-get update && apt-get install -y \
    pkg-config \
    protobuf-compiler \
    clang \
    make

COPY examples examples
COPY linera-base linera-base
COPY linera-chain linera-chain
COPY linera-client linera-client
COPY linera-core linera-core
COPY linera-ethereum linera-ethereum
COPY linera-execution linera-execution
COPY linera-explorer linera-explorer
COPY linera-faucet linera-faucet
COPY linera-indexer linera-indexer
COPY linera-metrics linera-metrics
COPY linera-rpc linera-rpc
COPY linera-sdk linera-sdk
COPY linera-sdk-derive linera-sdk-derive
COPY linera-service linera-service
COPY linera-service-graphql-client linera-service-graphql-client
COPY linera-storage linera-storage
COPY linera-storage-service linera-storage-service
COPY linera-summary linera-summary
COPY linera-persistent linera-persistent
COPY linera-version linera-version
COPY linera-views linera-views
COPY linera-views-derive linera-views-derive
COPY linera-witty linera-witty
COPY linera-witty-macros linera-witty-macros
COPY scripts scripts
COPY web web
COPY rust-toolchain* Cargo.* ./

ENV GIT_COMMIT=${git_commit}
ENV RUSTFLAGS=${rustflags}

RUN cargo build ${build_flag:+"$build_flag"} \
    --bin linera-proxy \
    --bin linera-server \
    --bin linera \
    --features $build_features

RUN mv \
    target/"$build_folder"/linera \
    target/"$build_folder"/linera-proxy \
    target/"$build_folder"/linera-server \
    ./

# Optionally copy binaries instead of using the build images above
FROM scratch AS builder_copy
ARG binaries
COPY \
    "$binaries"/linera \
    "$binaries"/linera-server \
    "$binaries"/linera-proxy \
    ./

FROM builder$copy AS binaries

# Setup running environment for container
FROM debian:latest

ARG git_commit
LABEL git_commit=$git_commit

ARG build_date
LABEL build_date=$build_date

RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl
RUN update-ca-certificates

COPY --from=binaries \
    linera \
    linera-server \
    linera-proxy \
    ./
