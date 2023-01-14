// Copyright (c) Zefchain Labs, Inc.
// Copyright (c) Alex Kladov
// SPDX-License-Identifier: Apache-2.0

//! A binary for running unit tests for Linera applications implemented in WebAssembly.
//!
//! The tests must be compiled for the `wasm32-unknown-unknown` target, and each unit test should
//! use the [webassembly_test attribute][webassembly_test].
//!
//! # Using With Cargo
//!
//! It's possible to make Cargo use this test-runner for the `wasm32-unknown-unknown` target
//! automatically by adding the following line to one of [Cargo's configuration
//! files](https://doc.rust-lang.org/cargo/reference/config.html#hierarchical-structure) or setting
//! the `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER` environment variable.
//!
//! ```ignore
//! [target.wasm32-unknown-unknown]
//! runner = "/path/to/test-runner"
//! ```
//!
//! [webassembly_test]: https://docs.rs/webassembly-test/latest/webassembly_test/

use anyhow::{bail, Result};
use wasmtime::*;

/// Load a test WASM module and run all test functions annotated by [webassembly_test].
///
/// Prints out a summary of executed tests and their results.
fn main() -> Result<()> {
    let engine = Engine::default();
    let test_module = load_test_module(&engine)?;
    let tests: Vec<_> = test_module.exports().filter_map(Test::new).collect();

    eprintln!("\nrunning {} tests", tests.len());
    let mut report = TestReport::default();
    let mut store = Store::new(&engine, ());
    let mut instance = Instance::new(&mut store, &test_module, &[])?;
    for test in tests {
        eprint!("test {} ...", test.name);
        if test.ignore {
            report.ignore();
        } else {
            let f = instance.get_typed_func::<(), (), _>(&mut store, test.function)?;

            let pass = f.call(&mut store, ()).is_ok();
            if pass {
                report.pass();
            } else {
                // Reset instance on test failure. WASM uses `panic=abort`, so
                // `Drop`s are not called after test failures, and a failed test
                // might leave an instance in an inconsistent state.
                store = Store::new(&engine, ());
                instance = Instance::new(&mut store, &test_module, &[])?;

                report.fail();
            }
        }
    }
    report.print();
    Ok(())
}

/// Load the input test WASM module specified as a command line argument.
fn load_test_module(engine: &Engine) -> Result<Module> {
    let module_path = parse_args()?;
    let module = Module::from_file(engine, module_path)?;
    Ok(module)
}

/// Parse command line arguments to extract the path of the input test module.
fn parse_args() -> Result<String> {
    match std::env::args().nth(1) {
        Some(file_path) => Ok(file_path),
        None => {
            bail!("usage: test-runner tests.wasm");
        }
    }
}

/// Information of a test in the input WASM module.
struct Test<'a> {
    name: &'a str,
    function: &'a str,
    ignore: bool,
}

impl<'a> Test<'a> {
    /// Collect test information from a function exported from the WASM module.
    pub fn new(export: ExportType<'a>) -> Option<Self> {
        let function = export.name();
        let test_name = function.strip_prefix("$webassembly-test$")?;
        let ignored_test_name = test_name.strip_prefix("ignore$");

        Some(Test {
            function,
            name: ignored_test_name.unwrap_or(test_name),
            ignore: ignored_test_name.is_some(),
        })
    }
}

/// Summarizer of test results.
#[derive(Clone, Copy, Debug, Default)]
struct TestReport {
    passed: usize,
    failed: usize,
    ignored: usize,
}

impl TestReport {
    /// Report that a test passed.
    pub fn pass(&mut self) {
        self.passed += 1;
        eprintln!(" ok")
    }

    /// Report that a test failed.
    pub fn fail(&mut self) {
        self.failed += 1;
        eprintln!(" FAILED")
    }

    /// Report that a test was ignored.
    pub fn ignore(&mut self) {
        self.ignored += 1;
        eprintln!(" ignored")
    }

    /// Print the report of executed tests.
    pub fn print(self) {
        let TestReport {
            passed,
            failed,
            ignored,
        } = self;

        let status = if failed > 0 { "FAILED" } else { "ok" };

        eprintln!("\ntest result: {status}. {passed} passed; {failed} failed; {ignored} ignored;");
    }
}
