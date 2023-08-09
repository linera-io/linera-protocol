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

/// An interface to import functions with return values.
#[wit_import(package = "witty-macros:test-modules")]
trait Getters {
    fn get_true() -> bool;
    fn get_false() -> bool;
    fn get_s8() -> i8;
    fn get_u8() -> u8;
    fn get_s16() -> i16;
    fn get_u16() -> u16;
    fn get_s32() -> i32;
    fn get_u32() -> u32;
    fn get_s64() -> i64;
    fn get_u64() -> u64;
    fn get_float32() -> f32;
    fn get_float64() -> f64;
}

/// Test importing functions with return values.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
fn getters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForGetters,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module("getters");

    let mut getters = Getters::new(instance);

    assert_eq!(
        getters
            .get_true()
            .expect("Failed to run guest's `get-true` function"),
        true
    );
    assert_eq!(
        getters
            .get_false()
            .expect("Failed to run guest's `get-false` function"),
        false
    );
    assert_eq!(
        getters
            .get_s8()
            .expect("Failed to run guest's `get-s8` function"),
        -125
    );
    assert_eq!(
        getters
            .get_u8()
            .expect("Failed to run guest's `get-u8` function"),
        200
    );
    assert_eq!(
        getters
            .get_s16()
            .expect("Failed to run guest's `get-s16` function"),
        -410
    );
    assert_eq!(
        getters
            .get_u16()
            .expect("Failed to run guest's `get-u16` function"),
        60_000
    );
    assert_eq!(
        getters
            .get_s32()
            .expect("Failed to run guest's `get-s32` function"),
        -100_000
    );
    assert_eq!(
        getters
            .get_u32()
            .expect("Failed to run guest's `get-u32` function"),
        3_000_111
    );
    assert_eq!(
        getters
            .get_s64()
            .expect("Failed to run guest's `get-s64` function"),
        -5_000_000
    );
    assert_eq!(
        getters
            .get_u64()
            .expect("Failed to run guest's `get-u64` function"),
        10_000_000_000
    );
    assert_eq!(
        getters
            .get_float32()
            .expect("Failed to run guest's `get-f32` function"),
        -0.125
    );
    assert_eq!(
        getters
            .get_float64()
            .expect("Failed to run guest's `get-f64` function"),
        128.25
    );
}
