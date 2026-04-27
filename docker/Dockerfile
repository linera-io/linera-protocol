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
ARG build_features=scylladb,metrics,jemalloc
ARG rustflags="-C force-frame-pointers=yes -L /usr/lib/x86_64-linux-gnu"

FROM rust:1.86-slim-bookworm AS builder
ARG git_commit
ARG build_flag
ARG build_folder
ARG build_features
ARG rustflags

RUN apt-get update && apt-get install -y \
    pkg-config \
    protobuf-compiler \
    clang \
    make \
    libunwind-dev

COPY . .

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
FROM debian:bookworm-slim

ARG git_commit
LABEL git_commit=$git_commit

ARG build_date
LABEL build_date=$build_date

RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    libunwind8
RUN update-ca-certificates

COPY --from=binaries \
    linera \
    linera-server \
    linera-proxy \
    ./
