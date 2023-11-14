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
//! It's possible to make Cargo use this test runner for the `wasm32-unknown-unknown` target
//! automatically by adding the following line to one of [Cargo's configuration
//! files](https://doc.rust-lang.org/cargo/reference/config.html#hierarchical-structure) or setting
//! the `CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER` environment variable.
//!
//! ```ignore
//! [target.wasm32-unknown-unknown]
//! runner = "/path/to/linera-wasm-test-runner"
//! ```
//!
//! [webassembly_test]: https://docs.rs/webassembly-test/latest/webassembly_test/

#[cfg(target_arch = "wasm32")]
compile_error!("The test runner is meant to be compiled for the host target");

mod mock_system_api;

use anyhow::Result;
use mock_system_api::Resources;
use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};
use structopt::StructOpt;
use wasmtime::*;

#[derive(StructOpt)]
#[structopt(
    name = "Linera Wasm test runner",
    about = "A binary for running unit tests for Linera applications implemented in WebAssembly."
)]
struct Options {
    module_path: PathBuf,
}

/// Loads a test Wasm module and runs all test functions annotated by [`webassembly-test`].
///
/// Prints out a summary of executed tests and their results.
#[tokio::main]
async fn main() -> Result<ExitCode> {
    let options = Options::from_args_safe()?;

    let mut report = TestReport::default();
    let mut engine_config = Config::default();
    engine_config.wasm_backtrace_details(WasmBacktraceDetails::Enable);
    engine_config.async_support(true);
    let engine = Engine::new(&engine_config)?;
    let mut linker = Linker::new(&engine);
    let test_module = load_test_module(&options.module_path, &engine)?;
    let tests = test_module
        .exports()
        .filter_map(Test::new)
        .collect::<Vec<_>>();

    mock_system_api::add_to_linker(&mut linker)?;

    linker.define_unknown_imports_as_traps(&test_module)?;

    eprintln!("\nrunning {} tests", tests.len());

    for test in tests {
        test.run(&mut report, &linker, &test_module).await?;
    }

    Ok(report.print())
}

/// Loads the input test Wasm module specified as a command line argument.
fn load_test_module(module_path: &Path, engine: &Engine) -> Result<Module> {
    let module = Module::from_file(engine, module_path)?;
    Ok(module)
}

/// Information of a test in the input Wasm module.
struct Test<'a> {
    name: &'a str,
    function: &'a str,
    ignore: bool,
}

impl<'a> Test<'a> {
    /// Collects test information from a function exported from the Wasm module.
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

    /// Runs a test function exported from the Wasm module and reports its result.
    ///
    /// The test is executed in a clean Wasm environment.
    pub async fn run(
        self,
        report: &mut TestReport,
        linker: &Linker<Resources>,
        test_module: &Module,
    ) -> Result<()> {
        eprint!("test {} ...", self.name);

        if self.ignore {
            report.ignore();
        } else {
            let mut store = Store::new(linker.engine(), Resources::default());
            let instance = linker.instantiate_async(&mut store, test_module).await?;

            let function = instance.get_typed_func::<(), (), _>(&mut store, self.function)?;

            report.result(function.call_async(&mut store, ()).await);
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
    /// Reports a test result.
    ///
    /// Reports that a test passed if `result` is `Ok` or that it failed otherwise.
    pub fn result<T>(&mut self, result: Result<T, Trap>) {
        match result {
            Ok(_) => self.pass(),
            Err(trap) => self.fail(trap),
        }
    }

    /// Reports that a test passed.
    pub fn pass(&mut self) {
        self.passed += 1;
        eprintln!(" ok")
    }

    /// Reports that a test failed.
    pub fn fail(&mut self, trap: Trap) {
        self.failed += 1;
        eprintln!(" FAILED");
        eprintln!("{}", trap);
    }

    /// Reports that a test was ignored.
    pub fn ignore(&mut self) {
        self.ignored += 1;
        eprintln!(" ignored")
    }

    /// Prints the report of executed tests.
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
