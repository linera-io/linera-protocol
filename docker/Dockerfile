##############################################################################
################## Meant to be run with Google Cloud Build ###################
##############################################################################

# From the root of the repo, use the following command to run:
#   gcloud builds submit --tag us-docker.pkg.dev/linera-io-dev/linera-docker-repo/<PACKAGE_NAME>:<VERSION_TAG> --timeout="3h" --machine-type=e2-highcpu-32
# The package name needs that prefix so it stores it in the proper Docker container registry on GCP (Google Cloud Platform).
# Make sure you specify the <PACKAGE_NAME> and <VERSION_TAG> you want though.
# The --timeout and --machine-type flags are optional, but building with the default machine type
# takes considerably longer. The default timeout is 1h, which you'll likely hit if you run with
# the default machine type.

# Build arguments:

# - `environment` is the name of a runtime environment, and used to
#   select the configuration.  The name of a directory in
#   `/configuration`.
# - `target` is a Rust target quadruple.  Currently known to be
#   supported are `x86_64-unknown-linux-gnu` and
#   `aarch64-unknown-linux-gnu`.

# Stage 1 - Generate recipe file for dependencies
FROM lukemathwalker/cargo-chef:latest-rust-slim AS chef

ARG environment=k8s-local
# Modern Docker provides TARGETPLATFORM and friends, but these do not
# provide enough information to reconstruct the whole Rust target and
# switching on them to map to a subset of targets seems unwise
ARG target=x86_64-unknown-linux-gnu

RUN mkdir -p /opt/linera/build
WORKDIR /opt/linera/build

FROM chef as planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2 - Build dependencies
FROM chef AS cacher

COPY . .
COPY --from=planner /opt/linera/build/recipe.json recipe.json

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang

RUN cargo chef cook --release --recipe-path recipe.json --target "$target"

# Stage 3 - Do actual build
FROM chef as builder
ARG environment
ARG target

COPY . .

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang

COPY --from=cacher /opt/linera/build/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo

RUN cargo build --release \
    --target "$target" \
    --bin linera \
    --bin linera-proxy \
    --bin linera-server \
    --bin linera-db \
    --features scylladb \
    && strip target/"$target"/release/linera \
    && strip target/"$target"/release/linera-proxy \
    && strip target/"$target"/release/linera-server \
    && strip target/"$target"/release/linera-db

# Stage 4 - Setup running environment for container
FROM debian:latest

ARG environment
ARG target

WORKDIR /opt/linera

RUN apt-get update && apt-get install -y libssl-dev

ENV BUILD_PATH=/opt/linera/build

# Copying binaries
COPY --from=builder $BUILD_PATH/target/$target/release/linera ./
COPY --from=builder $BUILD_PATH/target/$target/release/linera-server ./
COPY --from=builder $BUILD_PATH/target/$target/release/linera-proxy ./
COPY --from=builder $BUILD_PATH/target/$target/release/linera-db ./

# Copying configuration files
COPY --from=builder $BUILD_PATH/configuration/$environment/validator_1.toml ./

# Copy entrypoint
COPY docker/server-entrypoint.sh .
RUN chmod +x server-entrypoint.sh

# Copy init
COPY docker/server-init.sh .
RUN chmod +x server-init.sh

# Create configuration files for the validator according to the validator's config file.
# * Private server states are stored in `server*.json`.
# * `committee.json` is the public description of the Linera committee.
RUN ./linera-server generate --validators validator_1.toml --committee committee.json

# Create configuration files for 1000 user chains.
# * Private chain states are stored in one local wallet `wallet.json`.
# * `genesis.json` will contain the initial balances of chains as well as the initial committee.
RUN ./linera \
    --wallet wallet.json \
    --storage rocksdb:linera.db \
    create-genesis-config 1000 \
    --genesis genesis.json \
    --initial-funding 100 \
    --committee committee.json
