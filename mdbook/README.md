# Documentation page generator

This directory contains the configuration used `mdbook` to generate the documentation of
Linera from the content of `docs/`.

## Installing mdbook

```
cargo install mdbook@0.4.48 mdbook-linkcheck mdbook-mermaid mdbook-admonish
```

Because we override the default template, the exact version matters. To upgrade `mdbook`,
you will have to run `mdbook init --theme` and do a 3-way merge by hand.

## Browsing the developer docs locally (including local changes)

```
cd mdbook
mdbook serve
```
Then, open the URL as instructed.

## Testing

```bash
cargo clean

cargo clippy --locked -p linera-sdk --features test,wasmer
DUPLICATE_LIBS=(target/debug/deps/libserde-* target/debug/deps/liblinera_sdk-*)
cargo build --locked -p linera-sdk --features test,wasmer
# mdbook test wants only one library but build dependencies can create duplicates.
rm "${DUPLICATE_LIBS[@]}"

cd mdbook
mdbook test -L ../target/debug/deps
```

NOTE: mdbook doesn't use `rust-toolchain.toml`. Make sure to set the appropriate version
of Rust with Rustup before calling `mdbook test`.
