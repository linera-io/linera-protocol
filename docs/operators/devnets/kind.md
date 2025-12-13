# Running devnets with `kind`

In this section, we use `kind` to run a full devnet (network of validators)
locally.

Kind (Kubernetes in Docker) is a tool for running local Kubernetes clusters
using Docker container nodes. Kind uses Docker to create a cluster of containers
that simulate the Kubernetes control plane and worker nodes, allowing developers
to easily create, manage, and test multi-node clusters on their local machine.

## Installation

This section covers everything you need to install to run a Linera network with
`kind.`

### Linera Toolchain Requirements

The operating systems currently supported by the Linera toolchain can be
summarized as follows:

| Linux x86 64-bit | Mac OS (M1 / M2) | Mac OS (x86) | Windows  |
| ---------------- | ---------------- | ------------ | -------- |
| ✓ Main platform  | ✓ Working        | ✓ Working    | Untested |

The main prerequisites to install the Linera toolchain are Rust, Wasm, and
Protoc. They can be installed as follows on Linux:

- Rust and Wasm
  - `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
  - `rustup target add wasm32-unknown-unknown`

- Protoc
  - `curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v21.11/protoc-21.11-linux-x86_64.zip`
  - `unzip protoc-21.11-linux-x86_64.zip -d $HOME/.local`
  - If `~/.local` is not in your path, add it:
    `export PATH="$PATH:$HOME/.local/bin"`

- On certain Linux distributions, you may have to install development packages
  such as `g++`, `libclang-dev` and `libssl-dev`.

For MacOS support see the installation section on
[GitHub](https://github.com/linera-io/linera-protocol/blob/main/INSTALL.md).

This manual was tested with the following Rust toolchain:

```text
{{#include ../../../rust-toolchain.toml}}
```

### Local Kubernetes Requirements

To run `kind` locally, you also need the following dependencies:

1. [`kind`](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
2. [`kubectl`](https://kubernetes.io/docs/tasks/tools/)
3. [`docker`](https://docs.docker.com/get-docker/)
4. [`helm`](https://helm.sh/docs/intro/install/)
5. [`helm-diff`](https://github.com/databus23/helm-diff)
6. [`helmfile`](https://github.com/helmfile/helmfile?tab=readme-ov-file#installation)

### Installing the Linera Toolchain

To install the `Linera` toolchain, download the Linera source from
[GitHub](https://github.com/linera-io/linera-protocol):

```bash
git clone https://github.com/linera-io/linera-protocol.git
cd linera-protocol
git checkout -t origin/{{#include ../../RELEASE_BRANCH}}  # Current release branch
```

and to install the Linera toolchain:

```bash
cargo install --locked --path linera-service --features kubernetes
```

## Running with `kind`

To run a local devnet with `kind`, navigate to the root of the `linera-protocol`
repository and run:

```bash
linera net up --kubernetes
```

This will take some time as Docker images are built from the Linera source. When
the cluster is ready, some text is written to the process output containing the
exports required to configure your wallet for the devnet - something like:

```bash
export LINERA_WALLET="/tmp/.tmpIOelqk/wallet_0.json"
export LINERA_KEYSTORE="/tmp/.tmpIOelqk/keystore_0.json"
export LINERA_STORAGE="rocksdb:/tmp/.tmpIOelqk/client_0.db"
```

Exporting these variables in a new terminal will enable you to interact with the
devnet:

```bash
$ linera sync-balance
2024-05-21T22:30:12.061199Z  INFO linera: Synchronizing chain information and querying the local balance
2024-05-21T22:30:12.061218Z  WARN linera: This command is deprecated. Use `linera sync && linera query-balance` instead.
2024-05-21T22:30:12.065787Z  INFO linera::client_context: Saved user chain states
2024-05-21T22:30:12.065792Z  INFO linera: Operation confirmed after 4 ms
1000000.
```
