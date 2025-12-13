# Linera Developer Documentation

Repository for the developer documentation of Linera hosted on https://linera.dev

## Installing mdbook

```
cargo install mdbook@0.4.48 mdbook-linkcheck mdbook-mermaid mdbook-admonish
```

Because we override the default template, the exact version matters. To upgrade `mdbook`,
you will have to run `mdbook init --theme` and do a 3-way merge by hand.

## Making a release

The complete workflow may look like this:

```bash
# Update the files `RELEASE_BRANCH` and `RELEASE_VERSION` to desired release branch and
# release version of linera-protocol:
#   echo devnet_YYYY_MM_DD > RELEASE_BRANCH
#   echo 0.M.N > RELEASE_VERSION
#   cat RELEASE_BRANCH | sed 's/_/-/g' > RELEASE_DOMAIN

REMOTE_BRANCH="origin/$(cat RELEASE_BRANCH)"

cd linera-protocol
git fetch origin
git checkout $(git rev-parse $REMOTE_BRANCH)

cargo clean
cargo clippy --locked -p linera-sdk --features test,wasmer
DUPLICATE_LIBS=(target/debug/deps/libserde-* target/debug/deps/liblinera_sdk-*)
cargo build --locked -p linera-sdk --features test,wasmer
# mdbook test wants only one library but build dependencies can create duplicates.
rm "${DUPLICATE_LIBS[@]}"

cd ..
mdbook test -L linera-protocol/target/debug/deps
git commit -a
```

NOTE: mdbook doesn't use `rust-toolchain.toml`. Make sure to set the appropriate version
of Rust with Rustup before calling `mdbook test`.

## Browsing the developer docs locally (including local changes)

```
mdbook serve
```
Then, open the URL as instructed.

## Formatting

This repository is formatted with prettier. To install prettier run `npm install -g
prettier`. To use prettier run `prettier --write src`. The repository is automatically
checked for formatting in CI.
