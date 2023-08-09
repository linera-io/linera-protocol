// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `wit_import` attribute macro.

#![allow(clippy::bool_assert_comparison)]

#[path = "common/test_instance.rs"]
mod test_instance;

use self::test_instance::{MockInstanceFactory, TestInstanceFactory};
use linera_witty::{Instance, Runtime, RuntimeMemory};
use linera_witty_macros::wit_import;
use test_case::test_case;

/// An interface to import a single function without parameters or return values.
#[wit_import(package = "witty-macros:test-modules")]
trait SimpleFunction {
    fn simple();
}

/// Test importing a simple function without parameters or return values.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
fn simple_function<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForSimpleFunction,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module("simple-function");

    SimpleFunction::new(instance)
        .simple()
        .expect("Failed to call guest's `simple` function");
}
