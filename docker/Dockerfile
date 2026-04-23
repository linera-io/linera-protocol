# syntax=docker/dockerfile:1.6
#
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
#
# Requires BuildKit for `RUN --mount=type=cache` (DOCKER_BUILDKIT=1, `docker buildx`,
# or recent Docker Desktop/Engine).

ARG git_commit
ARG build_date
ARG binaries=
ARG copy=${binaries:+_copy}
ARG build_flag=--release
ARG build_folder=release
ARG build_features=scylladb,metrics,opentelemetry,jemalloc
ARG rustflags="-C force-frame-pointers=yes -L /usr/lib/x86_64-linux-gnu"

# ── Stage 0: chef base (rust + cargo-chef + system deps) ──
FROM rust:1.86-slim-bookworm AS chef

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install --no-install-recommends -y \
    pkg-config \
    protobuf-compiler \
    clang \
    make \
    libunwind-dev

WORKDIR /app

# Pre-install the toolchain matching rust-toolchain.toml so planner/builder
# stages don't trigger rustup downloads. The layer caches on the file's hash.
COPY rust-toolchain.toml rust-toolchain.toml
RUN rustup show

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo install cargo-chef --version 0.1.71 --locked

# ── Stage 1: planner — generate dependency recipe from full source ──

FROM chef AS planner
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo chef prepare --recipe-path recipe.json

# The examples/ workspace is separate from the root workspace, but some
# examples are dev-dependencies of root crates so Cargo must resolve them.
# Build minimal stubs (Cargo.toml + dummy sources) for every example member.
RUN set -e && \
    mkdir /examples-stubs && \
    cp examples/Cargo.toml /examples-stubs/Cargo.toml && \
    find examples -mindepth 2 -name Cargo.toml | while read -r toml; do \
      rel=$(dirname "${toml#examples/}"); \
      mkdir -p "/examples-stubs/$rel/src"; \
      cp "$toml" "/examples-stubs/$rel/Cargo.toml"; \
      touch "/examples-stubs/$rel/src/lib.rs"; \
      grep -A3 '^\[\[bin\]\]' "$toml" 2>/dev/null | \
        grep 'path' | sed 's/.*= *"\(.*\)"/\1/' | while read -r bin_path; do \
          mkdir -p "/examples-stubs/$rel/$(dirname "$bin_path")"; \
          printf 'fn main() {}\n' > "/examples-stubs/$rel/$bin_path"; \
        done; \
    done

# ── Stage 2: builder — cook deps (cached), then build source ──

FROM chef AS builder_src
ARG git_commit
ARG build_flag
ARG build_folder
ARG build_features
ARG rustflags

COPY --from=planner /app/recipe.json recipe.json

# linera-version's build.rs uses include!() for type definitions and hashes
# files from other crates at build-script runtime. These must exist before
# `cargo chef cook` because cook triggers the build script.
COPY linera-version/src/serde_pretty linera-version/src/serde_pretty
COPY linera-version/src/version_info linera-version/src/version_info
COPY linera-version/api-hashes.json linera-version/api-hashes.json
COPY linera-rpc/tests/snapshots linera-rpc/tests/snapshots
COPY linera-service-graphql-client/gql linera-service-graphql-client/gql
COPY linera-sdk/wit linera-sdk/wit

# Examples workspace stubs for dev-dep resolution during cook.
COPY --from=planner /examples-stubs examples

ENV GIT_COMMIT=${git_commit}
ENV RUSTFLAGS=${rustflags}

# Cook dependencies — cached until Cargo.toml / Cargo.lock change.
# The three cache mounts persist cargo's registry, git deps, and the
# compilation target/ across builds so unchanged deps never recompile.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook ${build_flag:+"$build_flag"} \
    --recipe-path recipe.json \
    --bin linera-proxy \
    --bin linera-server \
    --bin linera \
    --features $build_features

# Copy full source and build (only changed crates recompile).
COPY . .
# target/ is a cache mount — build + copy binaries OUT of it in the same RUN
# so they persist in the image layer (cache mounts don't survive the RUN).
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build ${build_flag:+"$build_flag"} \
        --bin linera-proxy \
        --bin linera-server \
        --bin linera \
        --features $build_features \
    && cp \
        target/"$build_folder"/linera \
        target/"$build_folder"/linera-proxy \
        target/"$build_folder"/linera-server \
        /

# Optionally copy binaries instead of using the build stage above
FROM scratch AS builder_src_copy
ARG binaries
COPY \
    "$binaries"/linera \
    "$binaries"/linera-server \
    "$binaries"/linera-proxy \
    /

FROM builder_src$copy AS binaries

# ── Stage 3: runtime ──
FROM debian:bookworm-slim

ARG git_commit
LABEL git_commit=$git_commit

ARG build_date
LABEL build_date=$build_date

RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    libunwind8 \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=binaries \
    /linera \
    /linera-server \
    /linera-proxy \
    ./
