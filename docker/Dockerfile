# syntax=docker/dockerfile:1.6
#
# Build arguments:
#
# - `git_commit` is the hash of the current git commit — used for
#   versioning information inside the binaries.
# - `build_date` is the date and time of when the docker image was built.
# - `build_flag` is either "--release" or an empty string.
# - `build_folder` is either "release" or "debug".
# - `build_features` is the feature set for EVERY target. Built WITHOUT
#   --no-default-features, so the crate defaults (wasmer, rocksdb,
#   storage-service) are added on top, exactly as before this file was split.
#
# This split changes WHICH BINARIES each image carries — deliberately NOT which
# features they are built with. Curating features per image (dropping
# storage-service / rocksdb from the validator) is a separate follow-up with its
# own runtime gate, so a regression can never be ambiguous between the two.
#
# Targets — ALWAYS pass --target. A bare `docker build` with no --target hits
# the `default` guard stage and fails fast (see the end of this file). Before
# the guard existed, `tests` was the last stage and therefore the implicit
# default, so most call sites silently paid `cargo test --no-run` of
# linera_net_tests:
#
#   validator   linera-server + linera-proxy   -> linera-validator
#   client      linera                         -> linera-client
#   tests       all three + remote-net test binary + linera-benchmark + example wasm
#
# There is no combined image. Every consumer takes exactly one of the two:
# anything running the client (the validator chart's *-initializer init
# containers, the faucet, PM workers, CLI users) takes linera-client; anything
# running server/proxy takes linera-validator. `tests` is the sole place needing
# all three in one filesystem, because `linera net up` spawns linera-server and
# linera-proxy as child processes (cli_wrappers/local_net.rs command_for_binary).
#
# `--target X` builds only X's FROM-chain, so a validator build never compiles
# the client binary or the tests stage, and vice versa.
#
# Requires BuildKit for `RUN --mount=type=cache` (DOCKER_BUILDKIT=1, `docker buildx`,
# or recent Docker Desktop/Engine).

ARG git_commit
ARG build_date
ARG build_flag=--release
ARG build_folder=release
ARG build_features=scylladb,metrics,opentelemetry,jemalloc
# The `-L` lets the linker find libunwind, jemalloc's stack-trace backend (#5602).
# Debian's multiarch lib dir is arch-dependent (x86_64-linux-gnu vs
# aarch64-linux-gnu), so this points at a stable symlink that cook_base resolves
# from the build machine. Hardcoding one arch would silently drop libunwind on
# the other.
ARG rustflags="-C force-frame-pointers=yes -L /usr/lib/deb-multiarch"

# ── Stage 0: chef base (rust + cargo-chef + system deps) ──
FROM rust:1.86-slim-bookworm AS chef

# `rm -f /etc/apt/apt.conf.d/docker-clean`: Debian's slim images ship that file
# with `APT::Keep-Downloaded-Packages "false";`, which makes apt delete
# downloaded `.deb` files right after install. That defeats the cache mount on
# `/var/cache/apt` — every rebuild re-downloads. Remove the config so packages
# stay in the cache mount across builds.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get update && apt-get install --no-install-recommends -y \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    clang \
    make \
    libunwind-dev \
    jq

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

# ── Stage 2: cook_base — shared pre-cook setup ──
# Everything the per-binary builder stages need BEFORE `cargo chef cook`, so the
# fragile bits (build.rs include!() inputs, examples stubs, env) are written
# ONCE and shared by every builder. Only the cook/build RUN lines differ per
# binary set.

FROM chef AS cook_base
ARG git_commit
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

# Resolve the multiarch lib dir from the machine actually doing the build, so
# `rustflags` above stays arch-agnostic and amd64/arm64 need no separate value.
RUN ln -s "/usr/lib/$(uname -m)-linux-gnu" /usr/lib/deb-multiarch
ENV RUSTFLAGS=${rustflags}

# ── Stage 3a: builder_src — all three binaries, for the `tests` image only ──
# `tests` needs the client AND server/proxy in one filesystem (linera net up
# spawns them), so it builds them together here rather than composing the two
# split images, which would mean three separate cargo builds for one image.

FROM cook_base AS builder_src
ARG build_flag
ARG build_folder
ARG build_features

# Cook dependencies — cached until Cargo.toml / Cargo.lock change.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook ${build_flag:+"$build_flag"} \
    --recipe-path recipe.json \
    --bin linera-proxy \
    --bin linera-server \
    --bin linera \
    ${build_features:+--features $build_features}

# Copy full source and build (only changed crates recompile). target/ is a
# cache mount — build + copy binaries OUT of it in the same RUN so they persist
# in the image layer (cache mounts don't survive the RUN).
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build ${build_flag:+"$build_flag"} \
        --bin linera-proxy \
        --bin linera-server \
        --bin linera \
        ${build_features:+--features $build_features} \
    && cp \
        target/"$build_folder"/linera \
        target/"$build_folder"/linera-proxy \
        target/"$build_folder"/linera-server \
        /

# ── Stage 3b: validator_builder — the two validator binaries ──
# Same feature set as every other stage. This PR splits WHICH BINARIES each
# image carries, deliberately not which features they are built with: curating
# features (dropping storage-service / rocksdb, adding --no-default-features)
# is a separate change with its own runtime gate, so that if something breaks
# it is never ambiguous whether the split or a feature did it.
# The win here is that `--target validator` never builds the `linera` binary,
# nor the tests stage.

FROM cook_base AS validator_builder
ARG build_flag
ARG build_folder
ARG build_features

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook ${build_flag:+"$build_flag"} \
    --recipe-path recipe.json \
    --bin linera-proxy \
    --bin linera-server \
    ${build_features:+--features $build_features}

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build ${build_flag:+"$build_flag"} \
        --bin linera-proxy \
        --bin linera-server \
        ${build_features:+--features $build_features} \
    && cp \
        target/"$build_folder"/linera-proxy \
        target/"$build_folder"/linera-server \
        /

# ── Stage 3c: client_builder — the client binary only ──
# Same features as everything else (see the note on validator_builder).
# `--target client` never compiles linera-server or linera-proxy.

FROM cook_base AS client_builder
ARG build_flag
ARG build_folder
ARG build_features

# `[profile.release] debug = true` (root Cargo.toml) is there so production
# binaries can be profiled, and it costs a LOT: the client links to 1693 MB with
# debuginfo versus 123 MB without, while keeping ~212k symbols either way, so
# panic backtraces still symbolicate. Linking that with thin LTO does not fit in
# a 16 GB arm64 runner. The validator keeps debuginfo — it is the one being
# profiled — so this is set on the client stage only, not in cook_base.
ENV CARGO_PROFILE_RELEASE_DEBUG=false

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook ${build_flag:+"$build_flag"} \
    --recipe-path recipe.json \
    --bin linera \
    ${build_features:+--features $build_features}

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build ${build_flag:+"$build_flag"} \
        --bin linera \
        ${build_features:+--features $build_features} \
    && cp \
        target/"$build_folder"/linera \
        /

# ── Stage 4: runtime_base — OS deps shared by every runtime image ──
FROM debian:bookworm-slim AS runtime_base

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

# ── Stage 4a: validator runtime — linera-server + linera-proxy ──
# Published as `linera-validator`. Note the validator Helm chart also invokes
# the CLIENT binary (`linera storage check-existence` / `initialize`) — but only
# from init containers (linera-server-initializer, linera-proxy-initializer,
# linera-exporter-initializer), which take their own image. Those move to
# `linera-client`; this image does not need the client binary.
FROM runtime_base AS validator
COPY --from=validator_builder \
    /linera-server \
    /linera-proxy \
    ./

# ── Stage 4b: client runtime — the linera client binary only ──
# Published as `linera-client`.
FROM runtime_base AS client
COPY --from=client_builder \
    /linera \
    ./

# ── Stage 4c: nothing. There is no all-three image.
# Every consumer maps to exactly one of the two above: anything running the
# client (validator *-initializer init containers, the faucet, PM workers, CLI
# users) takes linera-client; anything running server/proxy takes
# linera-validator. `tests` is the one place that needs all three in a single
# filesystem — `linera net up` spawns linera-server/linera-proxy as child
# processes (cli_wrappers/local_net.rs command_for_binary) — and it gets them by
# copying from both builders below, not from a published combined image.

# ── Stage 5: tests-builder — remote-net test binary + benchmark + example wasm ──
# Builds /linera_net_tests and /linera-benchmark; also pre-compiles all example
# Wasm contracts so the test binary can publish them without needing cargo or
# source at runtime. The test binary lives in /app/target/release/deps/ (a cache
# mount) so it must be copied out in the same RUN.

FROM builder_src AS tests-builder
ARG build_flag
ARG build_folder

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build ${build_flag:+"$build_flag"} --bin linera-benchmark \
    && cargo test --no-run ${build_flag:+"$build_flag"} \
        -p linera-service \
        --features remote-net \
        --test linera_net_tests \
        --message-format=json-render-diagnostics > /tmp/cargo-tests.json \
    && jq -r 'select(.executable != null) | .executable' /tmp/cargo-tests.json \
        | grep '/linera_net_tests-' \
        | head -1 \
        | xargs -I{} cp {} /linera_net_tests \
    && cp target/"${build_folder}"/linera-benchmark /linera-benchmark

# Build every example contract exactly the way CI does (.github/workflows/
# scylladb.yml: `cd examples && cargo build --locked --release --target
# wasm32-unknown-unknown`). Building the whole examples workspace guarantees
# every `<name>_contract.wasm` / `<name>_service.wasm` the tests publish is
# produced — an explicit `-p` list silently misses contracts.
#
# No cache mount on examples/target: the wasm must persist into this image
# layer for the `tests` stage to copy, and building into a real dir (as CI
# does) avoids any cache-mount visibility ambiguity.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cd examples \
    && cargo build --locked --release --target wasm32-unknown-unknown \
    && mkdir -p /wasm-examples \
    && find target/wasm32-unknown-unknown/release \
        -maxdepth 1 -name "*.wasm" \
        -exec cp {} /wasm-examples/ \;

# ── Stage 6: tests runtime — runtime image + remote-net test binary ──
# Used by linera-infra's network-health-check CronJob. The test binary spawns
# the linera CLI via PATH, so /linera is symlinked into /usr/local/bin.

FROM runtime_base AS tests
COPY --from=builder_src \
    /linera \
    /linera-server \
    /linera-proxy \
    ./
RUN ln -s /linera /usr/local/bin/linera
COPY --from=tests-builder /linera_net_tests /usr/local/bin/linera-net-tests
COPY --from=tests-builder /linera-benchmark /usr/local/bin/linera-benchmark
# Stub cargo so build_application() succeeds without a Rust toolchain: it calls
# `cargo build`, the stub exits 0, then reads the pre-built .wasm files below.
RUN printf '#!/bin/sh\nexit 0\n' > /usr/local/bin/cargo && chmod +x /usr/local/bin/cargo
# The remote-net tests run from `/` and treat `/examples` as a linera-protocol
# checkout: they read contract sources and snapshot fixtures (e.g.
# examples/counter/tests/snapshots/format__format.snap), resolve each contract
# via `…/examples/<name>/../target/…/release/<name>_contract.wasm` (needs the
# real `/examples/<name>` dir to exist for the `..` to collapse), and
# linera-benchmark canonicalize()s `/examples/fungible`. Ship the examples
# source tree (.dockerignore strips target/) and overlay the pre-built wasm.
COPY examples /examples
COPY --from=tests-builder /wasm-examples/ /examples/target/wasm32-unknown-unknown/release/

# ── Final stage: guard ──
# Building with no --target used to silently build the expensive `tests` image.
# There is no "build everything" production image — every caller must choose a
# target. Placed last so a bare `docker build` resolves here and fails fast
# (this stage has no builder ancestors, so chef/planner/cargo never run).
FROM debian:bookworm-slim AS default
RUN echo "ERROR: this Dockerfile requires an explicit --target (validator | client | tests)" >&2 && exit 1
