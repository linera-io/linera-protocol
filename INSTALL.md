# Installation for Mac OS X

## Basics

* Homebrew
    - If possible, in user land e.g. in `$HOME/git/homebrew`
    - See https://docs.brew.sh/Installation#untar-anywhere
* `brew install rustup-init`
* Protoc
    - `curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v21.11/protoc-21.11-osx-x86_64.zip`
    - `unzip protoc-21.11-osx-x86_64.zip -d $HOME/.local`
    - If `~/.local` is not in your path, add it: `export PATH="$PATH:$HOME/.local/bin"`
## Service

* `brew install python3`
* `python3 -mpip install localstack`
* https://aws.amazon.com/cli/
* https://docs.docker.com/desktop/mac/install/

## Additional tooling

* `brew install gh`
* `cargo install mdbook`

# Installation for Linux

## Basics

* `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
* Protoc
    - `curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v21.11/protoc-21.11-linux-x86_64.zip`
    - `unzip protoc-21.11-linux-x86_64.zip -d $HOME/.local`
    - If `~/.local` is not in your path, add it: `export PATH="$PATH:$HOME/.local/bin"`

## Service

* https://aws.amazon.com/cli/
* https://docs.docker.com/engine/install/

## Additional Tooling

* `cargo install mdbook`