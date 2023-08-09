// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper code for testing using different runtimes.

use frunk::{hlist, HList};
use linera_witty::{InstanceWithMemory, MockExportedFunction, MockInstance, RuntimeError};
use std::any::Any;

/// Trait representing a type that can create instances for tests.
pub trait TestInstanceFactory {
    type Instance: InstanceWithMemory;

    fn load_test_module(&mut self, module_name: &str) -> Self::Instance;
}

/// A factory of [`MockInstance`]s.
#[derive(Default)]
pub struct MockInstanceFactory {
    deferred_assertions: Vec<Box<dyn Any>>,
}

impl TestInstanceFactory for MockInstanceFactory {
    type Instance = MockInstance;

    fn load_test_module(&mut self, module: &str) -> Self::Instance {
        let mut instance = MockInstance::default();

        match module {
            "simple-function" => self.simple_function(&mut instance),
            _ => panic!("Attempt to load module {module:?} which has no mocked exported methods"),
        }

        instance
    }
}

impl MockInstanceFactory {
    /// Mock the exported functions for the "simple-function" module.
    fn simple_function(&mut self, instance: &mut MockInstance) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/simple-function#simple",
            |_: HList![]| Ok(hlist![]),
            1,
        );
    }

    /// Mocks an exported function with the provided `name`.
    ///
    /// The `handler` is used when the exported function is called, which expected to happen
    /// `expected_calls` times.
    ///
    /// The created [`MockExportedFunction`] is automatically registered in the `instance` and
    /// added to the current list of deferred assertions, to be checked when the test finishes.
    fn mock_exported_function<Parameters, Results>(
        &mut self,
        instance: &mut MockInstance,
        name: &str,
        handler: fn(Parameters) -> Result<Results, RuntimeError>,
        expected_calls: usize,
    ) where
        Parameters: 'static,
        Results: 'static,
    {
        let mock_exported_function = MockExportedFunction::new(name, handler, expected_calls);

        mock_exported_function.register(instance);

        self.deferred_assertions
            .push(Box::new(mock_exported_function));
    }
}
