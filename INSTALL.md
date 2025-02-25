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
* Clang/LLVM
    - `brew install llvm@18` then make sure to update your `PATH` as instructed.
* Protoc
    - `brew install protobuf`

## Services

* https://aws.amazon.com/cli/
* https://docs.docker.com/desktop/mac/install/

## Additional tooling required by tests

* `brew install jq`
* `cargo install cargo-rdme`
* `cargo install taplo-cli`
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
* `cargo install taplo-cli`
* `cargo install cargo-all-features`
* `cargo install cargo-machete`

# Installation using Nix

Alternatively, this repository contains a Nix flake that can be used
to prepare a reproducible development environment on Nix-enabled
systems.

Nix runs on Linux systems (including the Windows Subsystem for Linux)
and macOS.

If you don't have Nix installed, we recommend using the [Determinate
Nix installer](https://github.com/DeterminateSystems/nix-installer) to
easily set up Nix with flakes support.

Alternatively, you can use [the standard
installer](https://nixos.org/download/) and follow [the
documentation](https://nixos.wiki/wiki/Flakes) to enable flakes
manually.

Once Nix is installed, from the repository root, simply run

```shellsession
$ nix develop
```

to enter a development environment, and then run `cargo build` or
`cargo install` normally.

If you have [direnv](https://direnv.net/) installed, there is no need
to manually run `nix develop`: instead, run `direnv allow` to
automatically drop into the build environment when you enter the
project directory.
