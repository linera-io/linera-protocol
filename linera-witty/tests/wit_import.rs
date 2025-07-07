// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `wit_import` attribute macro.

#![allow(clippy::bool_assert_comparison)]

#[path = "common/test_instance.rs"]
mod test_instance;
#[path = "common/wit_interface_test.rs"]
mod wit_interface_test;

use std::marker::PhantomData;

use insta::assert_snapshot;
use linera_witty::{
    wit_generation::{FileContentGenerator as _, WitInterface, WitInterfaceWriter, WitWorldWriter},
    wit_import, Instance, MockInstance, Runtime, RuntimeMemory,
};
use test_case::test_case;

#[cfg(with_wasmer)]
use self::test_instance::WasmerInstanceFactory;
#[cfg(with_wasmtime)]
use self::test_instance::WasmtimeInstanceFactory;
use self::{
    test_instance::{MockInstanceFactory, TestInstanceFactory, WithoutExports},
    wit_interface_test::{GETTERS, OPERATIONS, SETTERS, SIMPLE_FUNCTION},
};

/// An interface to import a single function without parameters or return values.
#[wit_import(package = "witty-macros:test-modules")]
trait SimpleFunction {
    fn simple();
}

/// Test importing a simple function without parameters or return values.
#[test_case(MockInstanceFactory::<()>::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_simple_function<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForSimpleFunction,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module::<WithoutExports>("export", "simple-function");

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
#[test_case(MockInstanceFactory::<()>::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_getters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForGetters,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module::<WithoutExports>("export", "getters");

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

/// An interface to import functions with parameters.
#[wit_import(package = "witty-macros:test-modules")]
trait Setters {
    fn set_bool(value: bool);
    fn set_s8(value: i8);
    fn set_u8(value: u8);
    fn set_s16(value: i16);
    fn set_u16(value: u16);
    fn set_s32(value: i32);
    fn set_u32(value: u32);
    fn set_s64(value: i64);
    fn set_u64(value: u64);
    fn set_float32(value: f32);
    fn set_float64(value: f64);
}

/// Test importing functions with parameters.
#[test_case(MockInstanceFactory::<()>::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_setters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForSetters,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module::<WithoutExports>("export", "setters");

    let mut setters = Setters::new(instance);

    setters
        .set_bool(false)
        .expect("Failed to run guest's `set-bool` function");
    setters
        .set_s8(-100)
        .expect("Failed to run guest's `set-s8` function");
    setters
        .set_u8(201)
        .expect("Failed to run guest's `set-u8` function");
    setters
        .set_s16(-20_000)
        .expect("Failed to run guest's `set-s16` function");
    setters
        .set_u16(50_000)
        .expect("Failed to run guest's `set-u16` function");
    setters
        .set_s32(-2_000_000)
        .expect("Failed to run guest's `set-s32` function");
    setters
        .set_u32(4_000_000)
        .expect("Failed to run guest's `set-u32` function");
    setters
        .set_s64(-25_000_000_000)
        .expect("Failed to run guest's `set-s64` function");
    setters
        .set_u64(7_000_000_000)
        .expect("Failed to run guest's `set-u64` function");
    setters
        .set_float32(10.4)
        .expect("Failed to run guest's `set-f32` function");
    setters
        .set_float64(-0.000_08)
        .expect("Failed to run guest's `set-f64` function");
}

/// An interface to import functions with multiple parameters and return values.
#[wit_import(package = "witty-macros:test-modules")]
trait Operations {
    fn and_bool(first: bool, second: bool) -> bool;
    fn add_s8(first: i8, second: i8) -> i8;
    fn add_u8(first: u8, second: u8) -> u8;
    fn add_s16(first: i16, second: i16) -> i16;
    fn add_u16(first: u16, second: u16) -> u16;
    fn add_s32(first: i32, second: i32) -> i32;
    fn add_u32(first: u32, second: u32) -> u32;
    fn add_s64(first: i64, second: i64) -> i64;
    fn add_u64(first: u64, second: u64) -> u64;
    fn add_float32(first: f32, second: f32) -> f32;
    fn add_float64(first: f64, second: f64) -> f64;
}

/// Test importing functions with multiple parameters and return values.
#[test_case(MockInstanceFactory::<()>::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_operations<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForOperations,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
{
    let instance = factory.load_test_module::<WithoutExports>("export", "operations");

    let mut operations = Operations::new(instance);

    assert_eq!(
        operations
            .and_bool(false, true)
            .expect("Failed to run guest's `and-bool` function"),
        false
    );
    assert_eq!(
        operations
            .and_bool(true, true)
            .expect("Failed to run guest's `and-bool` function"),
        true
    );
    assert_eq!(
        operations
            .add_s8(-126, 1)
            .expect("Failed to run guest's `add-s8` function"),
        -125
    );
    assert_eq!(
        operations
            .add_u8(189, 11)
            .expect("Failed to run guest's `add-u8` function"),
        200
    );
    assert_eq!(
        operations
            .add_s16(-400, -10)
            .expect("Failed to run guest's `add-s16` function"),
        -410
    );
    assert_eq!(
        operations
            .add_u16(32_000, 28_000)
            .expect("Failed to run guest's `add-u16` function"),
        60_000
    );
    assert_eq!(
        operations
            .add_s32(-2_000_000, 1_900_000)
            .expect("Failed to run guest's `add-s32` function"),
        -100_000
    );
    assert_eq!(
        operations
            .add_u32(3_000_000, 111)
            .expect("Failed to run guest's `add-u32` function"),
        3_000_111
    );
    assert_eq!(
        operations
            .add_s64(-2_000_000_001, 5_000_000_000)
            .expect("Failed to run guest's `add-s64` function"),
        2_999_999_999
    );
    assert_eq!(
        operations
            .add_u64(1_000_000_000, 1_000_000_000_000)
            .expect("Failed to run guest's `add-u64` function"),
        1_001_000_000_000
    );
    assert_eq!(
        operations
            .add_float32(0.0, -0.125)
            .expect("Failed to run guest's `add-f32` function"),
        -0.125
    );
    assert_eq!(
        operations
            .add_float64(128.0, 0.25)
            .expect("Failed to run guest's `add-f64` function"),
        128.25
    );
}

/// Tests the generated [`WitInterface`] implementations for the types used in this test.
#[test_case(PhantomData::<SimpleFunction<MockInstance<()>>>, SIMPLE_FUNCTION; "of_simple_function")]
#[test_case(PhantomData::<Getters<MockInstance<()>>>, GETTERS; "of_getters")]
#[test_case(PhantomData::<Setters<MockInstance<()>>>, SETTERS; "of_setters")]
#[test_case(PhantomData::<Operations<MockInstance<()>>>, OPERATIONS; "of_operations")]
fn test_wit_interface<Interface>(
    _: PhantomData<Interface>,
    expected_snippets: (&str, &[&str], &[(&str, &str)]),
) where
    Interface: WitInterface,
{
    wit_interface_test::test_wit_interface::<Interface>(expected_snippets);
}

/// Tests the generated file contents for the [`WitInterface`] implementations for the types used
/// in this test.
#[test_case(PhantomData::<SimpleFunction<MockInstance<()>>>, "simple-function"; "of_simple_function")]
#[test_case(PhantomData::<Getters<MockInstance<()>>>, "getters"; "of_getters")]
#[test_case(PhantomData::<Setters<MockInstance<()>>>, "setters"; "of_setters")]
#[test_case(PhantomData::<Operations<MockInstance<()>>>, "operations"; "of_operations")]
fn test_wit_interface_file<Interface>(_: PhantomData<Interface>, name: &str)
where
    Interface: WitInterface,
{
    let mut buffer = Vec::new();
    WitInterfaceWriter::new::<Interface>()
        .generate_file_contents(&mut buffer)
        .unwrap();
    assert_snapshot!(name, String::from_utf8(buffer).unwrap());
}

/// Tests the generated file contents for a WIT world containing all the interfaces used in this
/// test.
#[test]
fn test_wit_world_file() {
    let mut buffer = Vec::new();
    WitWorldWriter::new("witty-macros:test-modules", "test-world")
        .export::<SimpleFunction<MockInstance<()>>>()
        .export::<Getters<MockInstance<()>>>()
        .export::<Setters<MockInstance<()>>>()
        .export::<Operations<MockInstance<()>>>()
        .generate_file_contents(&mut buffer)
        .unwrap();
    assert_snapshot!(String::from_utf8(buffer).unwrap());
}
