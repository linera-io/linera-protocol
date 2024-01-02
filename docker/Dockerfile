# Build arguments:
#
# - `binaries` is the path to the directory containing the Linera
#   binaries.  Leave unset to build the binaries from scratch.
# - `environment` is the name of a runtime environment, and used to
#   select the configuration.  The name of a directory in
#   `/configuration`.
# - `target` is a Rust target quadruple.  Currently known to be
#   supported are `x86_64-unknown-linux-gnu` and
#   `aarch64-unknown-linux-gnu`.

# Stage 1 - Generate recipe file for dependencies

ARG environment=k8s-local
# Modern Docker provides TARGETPLATFORM and friends, but these do not
# provide enough information to reconstruct the whole Rust target and
# switching on them to map to a subset of targets seems unwise
ARG target=x86_64-unknown-linux-gnu
ARG binaries=
ARG copy=${binaries:+_copy}

FROM lukemathwalker/cargo-chef:latest-rust-slim AS chef

FROM chef as planner

COPY examples examples
COPY linera-base linera-base
COPY linera-chain linera-chain
COPY linera-core linera-core
COPY linera-execution linera-execution
COPY linera-explorer linera-explorer
COPY linera-indexer linera-indexer
COPY linera-rpc linera-rpc
COPY linera-sdk linera-sdk
COPY linera-sdk-derive linera-sdk-derive
COPY linera-service linera-service
COPY linera-service-graphql-client linera-service-graphql-client
COPY linera-storage linera-storage
COPY linera-views linera-views
COPY linera-views-derive linera-views-derive
COPY linera-witty linera-witty
COPY linera-witty-macros linera-witty-macros
COPY scripts scripts
COPY rust-toolchain* Cargo.* ./

RUN cargo chef prepare --recipe-path recipe.json

# Stage 2 - Build dependencies
FROM chef AS cacher
ARG target

RUN apt-get update && apt-get install -y \
    pkg-config \
    protobuf-compiler \
    clang

COPY --from=planner . .

RUN cargo chef cook --release --recipe-path recipe.json --target "$target"

# Stage 3 - Do actual build
FROM chef as builder
ARG target

RUN apt-get update && apt-get install -y \
    pkg-config \
    protobuf-compiler \
    clang

COPY --from=planner . .
COPY --from=cacher target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo

RUN cargo build --release \
    --target "$target" \
    --bin linera \
    --bin linera-proxy \
    --bin linera-server \
    --bin linera-db \
    --features scylladb

RUN mv \
    target/"$target"/release/linera \
    target/"$target"/release/linera-proxy \
    target/"$target"/release/linera-server \
    target/"$target"/release/linera-db \
    ./

RUN strip linera linera-proxy linera-server linera-db

# Stage 3.5 - Optionally copy binaries instead of using the build
# images above
FROM scratch as builder_copy
ARG binaries
COPY \
    "$binaries"/linera \
    "$binaries"/linera-server \
    "$binaries"/linera-proxy \
    "$binaries"/linera-db \
    ./

FROM builder$copy as binaries

# Stage 4 - Setup running environment for container
FROM debian:latest

ARG environment
ARG target

COPY --from=binaries \
    linera \
    linera-server \
    linera-proxy \
    linera-db \
    ./

COPY --chmod=755 \
    docker/server-entrypoint.sh \
    docker/server-init.sh \
    ./
