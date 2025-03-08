// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `wit_export` attribute macro.

#[path = "common/test_instance.rs"]
mod test_instance;
#[path = "common/wit_interface_test.rs"]
mod wit_interface_test;

use std::marker::PhantomData;

use insta::assert_snapshot;
use linera_witty::{
    wit_export,
    wit_generation::{WitInterface, WitInterfaceWriter, WitWorldWriter},
    wit_import, ExportTo, Instance, MockInstance, Runtime, RuntimeMemory,
};
use test_case::test_case;

#[cfg(with_wasmer)]
use self::test_instance::WasmerInstanceFactory;
#[cfg(with_wasmtime)]
use self::test_instance::WasmtimeInstanceFactory;
use self::{
    test_instance::{MockInstanceFactory, TestInstanceFactory},
    wit_interface_test::{ENTRYPOINT, GETTERS, OPERATIONS, SETTERS, SIMPLE_FUNCTION},
};

/// An interface to call into the test modules.
#[wit_import(package = "witty-macros:test-modules")]
pub trait Entrypoint {
    fn entrypoint();
}

/// Type to export a simple function without parameters or a return value.
pub struct SimpleFunction;

#[wit_export(package = "witty-macros:test-modules")]
impl SimpleFunction {
    fn simple() {
        println!("In simple");
    }
}

/// Test exporting a simple function without parameters or return values.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_simple_function<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    SimpleFunction: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<SimpleFunction>("import", "simple-function");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `simple` function");
}

/// Type to export functions with return values.
pub struct Getters;

#[wit_export(package = "witty-macros:test-modules")]
impl Getters {
    fn get_true() -> bool {
        true
    }

    fn get_false() -> bool {
        false
    }

    fn get_s8() -> i8 {
        -125
    }

    fn get_u8() -> u8 {
        200
    }

    fn get_s16() -> i16 {
        -410
    }

    fn get_u16() -> u16 {
        60_000
    }

    fn get_s32() -> i32 {
        -100_000
    }

    fn get_u32() -> u32 {
        3_000_111
    }

    fn get_s64() -> i64 {
        -5_000_000
    }

    fn get_u64() -> u64 {
        10_000_000_000
    }

    fn get_float32() -> f32 {
        -0.125
    }

    fn get_float64() -> f64 {
        128.25
    }
}

/// Test exporting functions with return values.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_getters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    Getters: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<Getters>("import", "getters");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to execute test of imported getters");
}

/// Type to export functions with parameters.
pub struct Setters;

#[wit_export(package = "witty-macros:test-modules")]
impl Setters {
    #[expect(clippy::bool_assert_comparison)]
    fn set_bool(value: bool) {
        assert_eq!(value, false);
    }

    fn set_s8(value: i8) {
        assert_eq!(value, -100);
    }

    fn set_u8(value: u8) {
        assert_eq!(value, 201);
    }

    fn set_s16(value: i16) {
        assert_eq!(value, -20_000);
    }

    fn set_u16(value: u16) {
        assert_eq!(value, 50_000);
    }

    fn set_s32(value: i32) {
        assert_eq!(value, -2_000_000);
    }

    fn set_u32(value: u32) {
        assert_eq!(value, 4_000_000);
    }

    fn set_s64(value: i64) {
        assert_eq!(value, -25_000_000_000);
    }

    fn set_u64(value: u64) {
        assert_eq!(value, 7_000_000_000);
    }

    fn set_float32(value: f32) {
        assert_eq!(value, 10.4);
    }

    fn set_float64(value: f64) {
        assert_eq!(value, -0.000_08);
    }
}

/// Test exporting functions with parameters.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_setters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    Setters: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<Setters>("import", "setters");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to execute test of imported setters");
}

/// Type to export functions with multiple parameters and return values.
pub struct Operations;

#[wit_export(package = "witty-macros:test-modules")]
impl Operations {
    fn and_bool(first: bool, second: bool) -> bool {
        first && second
    }

    fn add_s8(first: i8, second: i8) -> i8 {
        first + second
    }

    fn add_u8(first: u8, second: u8) -> u8 {
        first + second
    }

    fn add_s16(first: i16, second: i16) -> i16 {
        first + second
    }

    fn add_u16(first: u16, second: u16) -> u16 {
        first + second
    }

    fn add_s32(first: i32, second: i32) -> i32 {
        first + second
    }

    fn add_u32(first: u32, second: u32) -> u32 {
        first + second
    }

    fn add_s64(first: i64, second: i64) -> i64 {
        first + second
    }

    fn add_u64(first: u64, second: u64) -> u64 {
        first + second
    }

    fn add_float32(first: f32, second: f32) -> f32 {
        first + second
    }

    fn add_float64(first: f64, second: f64) -> f64 {
        first + second
    }
}

/// Test exporting functions with multiple parameters and return values.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_operations<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    Operations: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<Operations>("import", "operations");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to execute test of imported operations");
}

/// Test the generated [`WitInterface`] implementations for the types used in this test.
#[test_case(PhantomData::<Entrypoint<MockInstance<()>>>, ENTRYPOINT; "of_entrypoint")]
#[test_case(PhantomData::<SimpleFunction>, SIMPLE_FUNCTION; "of_simple_function")]
#[test_case(PhantomData::<Getters>, GETTERS; "of_getters")]
#[test_case(PhantomData::<Setters>, SETTERS; "of_setters")]
#[test_case(PhantomData::<Operations>, OPERATIONS; "of_operations")]
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
#[test_case(PhantomData::<Entrypoint<MockInstance<()>>>, "entrypoint"; "of_entrypoint")]
#[test_case(PhantomData::<SimpleFunction>, "simple-function"; "of_simple_function")]
#[test_case(PhantomData::<Getters>, "getters"; "of_getters")]
#[test_case(PhantomData::<Setters>, "setters"; "of_setters")]
#[test_case(PhantomData::<Operations>, "operations"; "of_operations")]
fn test_wit_interface_file<Interface>(_: PhantomData<Interface>, name: &str)
where
    Interface: WitInterface,
{
    assert_snapshot!(
        name,
        WitInterfaceWriter::new::<Interface>()
            .generate_file_contents()
            .collect::<String>()
    );
}

/// Tests the generated file contents for a WIT world containing all the interfaces used in this
/// test.
#[test]
fn test_wit_world_file() {
    assert_snapshot!(
        WitWorldWriter::new("witty-macros:test-modules", "test-world")
            .export::<Entrypoint<MockInstance<()>>>()
            .import::<SimpleFunction>()
            .import::<Getters>()
            .import::<Setters>()
            .import::<Operations>()
            .generate_file_contents()
            .collect::<String>()
    );
}
