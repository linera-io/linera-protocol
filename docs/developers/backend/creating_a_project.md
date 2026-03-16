# Creating a Linera Project

To create your Linera project, use the `linera project new` command. The command
should be executed outside the `linera-protocol` folder. It sets up the
scaffolding and requisite files:

```bash
linera project new my-counter
```

`linera project new` bootstraps your project by creating the following key
files:

- `Cargo.toml`: your project's manifest filled with the necessary dependencies
  to create an app;
- `rust-toolchain.toml`: a file with configuration for Rust compiler.

> **NOTE:** currently the latest Rust version compatible with our network is
> `1.86.0`. Make sure it's the one used by your project.

- `src/lib.rs`: the application's ABI definition;
- `src/state.rs`: the application's state;
- `src/contract.rs`: the application's contract, and the binary target for the
  contract bytecode;
- `src/service.rs`: the application's service, and the binary target for the
  service bytecode.

> When writing Linera applications it is a convention to use your app's name as
> a prefix for names of `trait`, `struct`, etc. Hence, in the following manual,
> we will use `CounterContract`, `CounterService`, etc.
