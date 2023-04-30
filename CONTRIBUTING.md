# Contributing to the Linera protocol

## Issues

We use GitHub issues to track planned improvements, feature requests, and bug reports.

* If you plan to contribute a new feature, please discuss the feature in an issue beforehand to
ensure that your contributions will be accepted.

* If your send a bug report, please ensure that your description is clear and has sufficient
instructions to be able to reproduce the issue.

## Pull Requests

To make a contribution to the code after discussing it in a GitHub issue,

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes with `cargo test && cargo clippy --all-targets`.
5. Run `cargo +nightly fmt` to automatically format your changes (CI will let you know if you missed this).

This repository enforces a linear commit history. To remove merge commits, consider using `git filter-branch`
as explained [here](https://stackoverflow.com/questions/17988099/how-do-i-trivially-linearize-my-git-history).

Editing a clean commit history with `git rebase -i` and `git push -f your_PR_branch` is
encouraged to help the work of reviewers. Commits that land on the main branch without
being squashed should compile correctly and contain the PR number.

Only commits in a PR accepted by at least one team member should be pushed to the main branch.

Please also make yourself familiar with the rest of the guidelines below.

## Naming conventions

As usual in Rust, type names (struct, enum, traits) are compound words in [camel
case](https://en.wikipedia.org/wiki/Camel_case), e.g. `GarageDoorOption`.

In general, local variables should try to mirror the name of their type except that

1. they are in [snake case](https://en.wikipedia.org/wiki/Snake_case) instead of camel
   case: `garage_door_option`

2. we make them shorter by keeping the main part (at the end in English) when there is no
   ambiguity `let option = GarageDoorOption::default()`.

Accepted abbreviations in the entire codebase should only concern a very small number of
English words (e.g. `database` -> `db`). Abbreviations are never a single letter.

However, single letter variables `i`, `x`, `n` are accepted for short-lived integer types (e.g. a for-loop).

Prefer plural names for collections of objects: `let values = vec![1, 2, 3];`.

## API coding guidelines

Contributions should generally follow the [Rust API guidelines](https://rust-lang.github.io/api-guidelines/checklist.html) whenever possible.

## Additional code style guidelines

* Type annotations (such as `let x : t = ...;`, `Vec::<usize>::new()`) should be present only when required by the compiler.

* Re-exports (`pub use foo = bar::foo`) should be limited to definitions that would be private otherwise.

## Formatting and linting

Make sure to fix the lint errors reported by
```
cargo clippy --all-targets
```
and run `cargo fmt` like this:
```
cargo fmt -- --config unstable_features=true --config imports_granularity=Crate
```
or (optimistically)
```
cargo +nightly fmt
```
(see also [rust-lang/rustfmt#4991](https://github.com/rust-lang/rustfmt/issues/4991))

## Managing cargo features and dependencies between crates

* Crates `xxx` which define the `test` feature must include `linera-xxx = { path = ".",
  features = ["test"] }` in the section `[dev-dependencies]` instead of repeating the
  dependencies already declared by the feature `test`.

* A few crates define the features `wasm` and `wasmer`. For conveniency, these crates also
  define a `default` feature. As a consequence, these crates must always be included with
  the flag `no-default-features = true`. (This also applies to the self-dependencies of the
  previous rule.)


Besides the verification bove with clippy, the following steps will verify that most
combinations of features compile for each crate:
```
cargo install --git https://github.com/ma2bd/cargo-all-features --branch workspace_metadata

cargo check-all-features --all-targets
```

## Dealing with test failures `test_format` after code changes

Getting an error with the test in [`linera-rpc/tests/format.rs`](linera-rpc/tests/format.rs) ?
Probably the file [`linera-rpc/tests/staged/formats.yaml`](linera-rpc/tests/staged/formats.yaml) (recording message formats) is
outdated. In the most case (but not always sadly), this can be fixed by running
[`linera-rpc/generate-format.sh`](linera-rpc/generate-format.sh).

See https://github.com/zefchain/serde-reflection for more context.

## WASM support

The support of WASM is controlled by the features `wasmer` and the `wasmtime`, and
currently defaults to `wasmer` only.

Testing the WASM application examples requires configuring a custom test runner included with
`linera-sdk`. First it has to be built:

```
cargo build --release --bin test-runner
```

After that, the WASM tests can be executed with:

```
cd examples
cargo test --target wasm32-unknown-unknown
```

Note that this works because we declared the test-runner in `.cargo/config.toml` with
```
[target.wasm32-unknown-unknown]
runner = "/path/to/repository/target/release/test-runner"
```
(See the [documentation of Cargo](https://doc.rust-lang.org/cargo/reference/config.html#hierarchical-structure) for more context.)

Some tests require the application examples from `examples` to be compiled. If needed, this
can be done manually with
```
cd examples
cargo build --release --target wasm32-unknown-unknown
```
The Rust flags are suggested to reduce the size of the WASM bytecodes.

## Adding dependencies to third-party crates

Given the nature of the project, every dependency will be eventually tracked and audited.
Please be mindful about adding new dependencies to crates that are large and/or unlikely to be
already vetted by the Rust community.

## License and copyright

By contributing to `linera-protocol`, you agree that your contributions will be licensed
under the terms of the [`LICENSE`](LICENSE) file in the root directory of this source
tree.

New files should be copyrighted by "Zefchain Labs, Inc" (the legal entity behind the Linera project) using this header:
```
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
```
