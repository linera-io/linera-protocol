// Copyright (c) Zefchain Labs, Inc.
// Copyright (c) Alex Kladov
// SPDX-License-Identifier: Apache-2.0

//! A binary for running unit tests for Linera applications implemented in WebAssembly.
//!
//! The tests must be compiled for the `wasm32-unknown-unknown` target, and each unit test should
//! use the [webassembly_test attribute][webassembly_test].
//!
//! System APIs are not available to tests, so attempts to use any of them will lead to test
//! failures.
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
use std::process::ExitCode;
use wasmtime::*;

/// Load a test WASM module and run all test functions annotated by [webassembly_test].
///
/// Prints out a summary of executed tests and their results.
fn main() -> Result<ExitCode> {
    let mut report = TestReport::default();
    let mut engine_config = Config::default();
    engine_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
    let engine = Engine::new(&engine_config)?;
    let mut linker = Linker::new(&engine);
    let test_module = load_test_module(&engine)?;
    let tests: Vec<_> = test_module.exports().filter_map(Test::new).collect();

    linker.define_unknown_imports_as_traps(&test_module)?;

    eprintln!("\nrunning {} tests", tests.len());

    for test in tests {
        test.run(&mut report, &linker, &test_module)?;
    }

    Ok(report.print())
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

    /// Run an test function exported from the WASM module and report its result.
    ///
    /// The test is executed in a clean WASM environment.
    pub fn run(
        self,
        report: &mut TestReport,
        linker: &Linker<()>,
        test_module: &Module,
    ) -> Result<()> {
        eprint!("test {} ...", self.name);

        if self.ignore {
            report.ignore();
        } else {
            let mut store = Store::new(linker.engine(), ());
            let instance = linker.instantiate(&mut store, test_module)?;

            let function = instance.get_typed_func::<(), (), _>(&mut store, self.function)?;

            report.result(function.call(&mut store, ()));
        }

        Ok(())
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
    /// Report a test result.
    ///
    /// Reports that a test passed if `result` is `Ok` or that it failed otherwise.
    pub fn result<T>(&mut self, result: Result<T, Trap>) {
        match result {
            Ok(_) => self.pass(),
            Err(trap) => self.fail(trap),
        }
    }

    /// Report that a test passed.
    pub fn pass(&mut self) {
        self.passed += 1;
        eprintln!(" ok")
    }

    /// Report that a test failed.
    pub fn fail(&mut self, trap: Trap) {
        self.failed += 1;
        eprintln!(" FAILED");
        eprintln!("{}", trap);
    }

    /// Report that a test was ignored.
    pub fn ignore(&mut self) {
        self.ignored += 1;
        eprintln!(" ignored")
    }

    /// Print the report of executed tests.
    pub fn print(self) -> ExitCode {
        let TestReport {
            passed,
            failed,
            ignored,
        } = self;

        let status = if failed > 0 { "FAILED" } else { "ok" };

        eprintln!("\ntest result: {status}. {passed} passed; {failed} failed; {ignored} ignored;");

        if failed == 0 {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        }
    }
}
