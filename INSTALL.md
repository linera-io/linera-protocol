# Installation for Mac OS (Intel / M1 / M2)

## Basics

* Xcode command line tools
    - `xcode-select --install`
* Homebrew
    - If possible, install in user land e.g. in `$HOME/git/homebrew`
    - See https://docs.brew.sh/Installation#untar-anywhere
* Rust
    - `brew install rustup-init`
    - `rustup target add wasm32-unknown-unknown`
* Protoc
    - `brew install protobuf`

## Services

* https://aws.amazon.com/cli/
* https://docs.docker.com/desktop/mac/install/

## Additional tooling required by tests

* `brew install jq`
* `cargo install cargo-rdme`
* `cargo install cargo-sort`
* `cargo install cargo-all-features`
* `cargo install cargo-machete`

# Installation for Linux

## Basics

* Rust
    - `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    - `rustup target add wasm32-unknown-unknown`
* Protoc
    - `curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v21.11/protoc-21.11-linux-x86_64.zip`
    - `unzip protoc-21.11-linux-x86_64.zip -d $HOME/.local`
    - If `~/.local` is not in your path, add it: `export PATH="$PATH:$HOME/.local/bin"`

Alternatively, we have added experimental Nix support (see `flake.nix`).

## Services

* https://aws.amazon.com/cli/
* https://docs.docker.com/engine/install/

## Additional tooling

* `sudo apt-get install jq`
* `cargo install cargo-rdme`
* `cargo install cargo-sort`
* `cargo install cargo-all-features`
* `cargo install cargo-machete`
