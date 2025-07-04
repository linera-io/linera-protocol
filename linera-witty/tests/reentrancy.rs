// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `wit_import` and `wit_export` attribute macro using reentrant host functions.

#[path = "common/test_instance.rs"]
mod test_instance;
#[path = "common/wit_interface_test.rs"]
mod wit_interface_test;

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use insta::assert_snapshot;
use linera_witty::{
    wit_export,
    wit_generation::{FileContentGenerator as _, WitInterface, WitInterfaceWriter, WitWorldWriter},
    wit_import, ExportTo, Instance, MockInstance, Runtime, RuntimeError, RuntimeMemory,
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

/// An interface to import a single function without parameters or return values.
#[wit_import(package = "witty-macros:test-modules", interface = "simple-function")]
trait ImportedSimpleFunction {
    fn simple();
}

/// Type to export a simple reentrant function without parameters or return values.
pub struct ExportedSimpleFunction;

#[wit_export(package = "witty-macros:test-modules", interface = "simple-function")]
impl ExportedSimpleFunction {
    fn simple<Caller>(caller: &mut Caller) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSimpleFunction,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        tracing::debug!("Before reentrant call");
        ImportedSimpleFunction::new(caller).simple()?;
        tracing::debug!("After reentrant call");
        Ok(())
    }
}

/// Test a simple reentrant function without parameters or return values.
///
/// The host function is called from the guest, and calls the guest back through a function with
/// the same name.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_simple_function<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedSimpleFunction: ExportTo<InstanceFactory::Builder>,
{
    let instance =
        factory.load_test_module::<ExportedSimpleFunction>("reentrancy", "simple-function");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");
}

/// An interface to import functions with return values.
#[wit_import(package = "witty-macros:test-modules", interface = "getters")]
trait ImportedGetters {
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

/// Type to export reentrant functions with return values.
pub struct ExportedGetters<Caller>(PhantomData<Caller>);

#[wit_export(package = "witty-macros:test-modules", interface = "getters")]
impl<Caller> ExportedGetters<Caller>
where
    Caller: InstanceForImportedGetters,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn get_true(caller: &mut Caller) -> Result<bool, RuntimeError> {
        ImportedGetters::new(caller).get_true()
    }

    fn get_false(caller: &mut Caller) -> Result<bool, RuntimeError> {
        ImportedGetters::new(caller).get_false()
    }

    fn get_s8(caller: &mut Caller) -> Result<i8, RuntimeError> {
        ImportedGetters::new(caller).get_s8()
    }

    fn get_u8(caller: &mut Caller) -> Result<u8, RuntimeError> {
        ImportedGetters::new(caller).get_u8()
    }

    fn get_s16(caller: &mut Caller) -> Result<i16, RuntimeError> {
        ImportedGetters::new(caller).get_s16()
    }

    fn get_u16(caller: &mut Caller) -> Result<u16, RuntimeError> {
        ImportedGetters::new(caller).get_u16()
    }

    fn get_s32(caller: &mut Caller) -> Result<i32, RuntimeError> {
        ImportedGetters::new(caller).get_s32()
    }

    fn get_u32(caller: &mut Caller) -> Result<u32, RuntimeError> {
        ImportedGetters::new(caller).get_u32()
    }

    fn get_s64(caller: &mut Caller) -> Result<i64, RuntimeError> {
        ImportedGetters::new(caller).get_s64()
    }

    fn get_u64(caller: &mut Caller) -> Result<u64, RuntimeError> {
        ImportedGetters::new(caller).get_u64()
    }

    fn get_float32(caller: &mut Caller) -> Result<f32, RuntimeError> {
        ImportedGetters::new(caller).get_float32()
    }

    fn get_float64(caller: &mut Caller) -> Result<f64, RuntimeError> {
        ImportedGetters::new(caller).get_float64()
    }
}

/// Test reentrant functions with return values.
///
/// The host functions are called from the guest, and they return values obtained by calling back
/// the guest through functions with the same names.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_getters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedGetters<InstanceFactory::Caller<'static>>: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<ExportedGetters<_>>("reentrancy", "getters");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");
}

/// An interface to import functions with parameters.
#[wit_import(package = "witty-macros:test-modules", interface = "setters")]
trait ImportedSetters {
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

/// Type to export reentrant functions with parameters.
pub struct ExportedSetters<Caller>(PhantomData<Caller>);

#[wit_export(package = "witty-macros:test-modules", interface = "setters")]
impl<Caller> ExportedSetters<Caller>
where
    Caller: InstanceForImportedSetters,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn set_bool(caller: &mut Caller, value: bool) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_bool(value)
    }

    fn set_s8(caller: &mut Caller, value: i8) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_s8(value)
    }

    fn set_u8(caller: &mut Caller, value: u8) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_u8(value)
    }

    fn set_s16(caller: &mut Caller, value: i16) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_s16(value)
    }

    fn set_u16(caller: &mut Caller, value: u16) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_u16(value)
    }

    fn set_s32(caller: &mut Caller, value: i32) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_s32(value)
    }

    fn set_u32(caller: &mut Caller, value: u32) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_u32(value)
    }

    fn set_s64(caller: &mut Caller, value: i64) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_s64(value)
    }

    fn set_u64(caller: &mut Caller, value: u64) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_u64(value)
    }

    fn set_float32(caller: &mut Caller, value: f32) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_float32(value)
    }

    fn set_float64(caller: &mut Caller, value: f64) -> Result<(), RuntimeError> {
        ImportedSetters::new(caller).set_float64(value)
    }
}

/// Test reentrant functions with parameters.
///
/// The host functions are called from the guest, and they forward the arguments back to the guest
/// by calling guest functions with the same names.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_setters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedSetters<InstanceFactory::Caller<'static>>: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<ExportedSetters<_>>("reentrancy", "setters");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");
}

/// An interface to import functions with multiple parameters and return values.
#[wit_import(package = "witty-macros:test-modules", interface = "operations")]
trait ImportedOperations {
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

/// Type to export reentrant functions with multiple parameters and return values.
pub struct ExportedOperations<Caller>(PhantomData<Caller>);

#[wit_export(package = "witty-macros:test-modules", interface = "operations")]
impl<Caller> ExportedOperations<Caller>
where
    Caller: InstanceForImportedOperations,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn and_bool(caller: &mut Caller, first: bool, second: bool) -> Result<bool, RuntimeError> {
        ImportedOperations::new(caller).and_bool(first, second)
    }

    fn add_s8(caller: &mut Caller, first: i8, second: i8) -> Result<i8, RuntimeError> {
        ImportedOperations::new(caller).add_s8(first, second)
    }

    fn add_u8(caller: &mut Caller, first: u8, second: u8) -> Result<u8, RuntimeError> {
        ImportedOperations::new(caller).add_u8(first, second)
    }

    fn add_s16(caller: &mut Caller, first: i16, second: i16) -> Result<i16, RuntimeError> {
        ImportedOperations::new(caller).add_s16(first, second)
    }

    fn add_u16(caller: &mut Caller, first: u16, second: u16) -> Result<u16, RuntimeError> {
        ImportedOperations::new(caller).add_u16(first, second)
    }

    fn add_s32(caller: &mut Caller, first: i32, second: i32) -> Result<i32, RuntimeError> {
        ImportedOperations::new(caller).add_s32(first, second)
    }

    fn add_u32(caller: &mut Caller, first: u32, second: u32) -> Result<u32, RuntimeError> {
        ImportedOperations::new(caller).add_u32(first, second)
    }

    fn add_s64(caller: &mut Caller, first: i64, second: i64) -> Result<i64, RuntimeError> {
        ImportedOperations::new(caller).add_s64(first, second)
    }

    fn add_u64(caller: &mut Caller, first: u64, second: u64) -> Result<u64, RuntimeError> {
        ImportedOperations::new(caller).add_u64(first, second)
    }

    fn add_float32(caller: &mut Caller, first: f32, second: f32) -> Result<f32, RuntimeError> {
        ImportedOperations::new(caller).add_float32(first, second)
    }

    fn add_float64(caller: &mut Caller, first: f64, second: f64) -> Result<f64, RuntimeError> {
        ImportedOperations::new(caller).add_float64(first, second)
    }
}

/// Test reentrant functions with multiple parameters and with return values.
///
/// The host functions are called from the guest, and they call the guest back through functions
/// with the same names, forwarding the arguments and retrieving the final results.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_operations<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedOperations<InstanceFactory::Caller<'static>>: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<ExportedOperations<_>>("reentrancy", "operations");

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");
}

/// An interface to import functions to use with a reentrancy global state test.
#[wit_import(package = "witty-macros:test-modules", interface = "global-state")]
trait ImportedGlobalState {
    fn entrypoint(value: u32) -> u32;
    fn get_global_state() -> u32;
}

/// Type to export reentrant functions to use with a global state test.
pub struct ExportedGlobalState;

#[wit_export(package = "witty-macros:test-modules", interface = "get-host-value")]
impl ExportedGlobalState {
    fn get_host_value<Caller>(caller: &mut Caller) -> Result<u32, RuntimeError>
    where
        Caller: InstanceForImportedGlobalState,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGlobalState::new(caller).get_global_state()
    }
}

/// Test global state inside a Wasm guest accessed through reentrant functions.
///
/// The host calls the entrypoint passing an integer argument which the guest stores in its global
/// state. Before returning, the guest calls the host's `get-host-value` function in order to
/// obtain the value to return. The host function calls the guest back to obtain the return value
/// from the guest's global state.
///
/// The final value returned from the guest must match the initial value the host sent in.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::<()>::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::<()>::default(); "with Wasmtime"))]
fn test_global_state<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForImportedGlobalState,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedGlobalState: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module::<ExportedGlobalState>("reentrancy", "global-state");
    let value = 100;

    let result = ImportedGlobalState::new(instance)
        .entrypoint(value)
        .expect("Failed to call guest's `entrypoint` function");

    assert_eq!(result, value);
}

/// Type to export a simple reentrant function while using custom user data
pub struct ExportedSimpleFunctionWithUserData<Caller>(PhantomData<Caller>);

#[wit_export(package = "witty-macros:test-modules", interface = "simple-function")]
impl<Caller> ExportedSimpleFunctionWithUserData<Caller>
where
    Caller: Instance<UserData = Arc<AtomicBool>> + InstanceForImportedSimpleFunction,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn simple(caller: &mut Caller) -> Result<(), RuntimeError> {
        tracing::debug!("Before reentrant call");
        ImportedSimpleFunction::new(&mut *caller).simple()?;
        tracing::debug!("After reentrant call");
        caller.user_data().store(true, Ordering::Relaxed);
        Ok(())
    }
}

/// Test global state inside a Wasm guest accessed through reentrant functions.
///
/// The host calls the entrypoint passing an integer argument which the guest stores in its global
/// state. Before returning, the guest calls the host's `get-host-value` function in order to
/// obtain the value to return. The host function calls the guest back to obtain the return value
/// from the guest's global state.
///
/// The final value returned from the guest must match the initial value the host sent in.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(with_wasmer, test_case(WasmerInstanceFactory::default(); "with Wasmer"))]
#[cfg_attr(with_wasmtime, test_case(WasmtimeInstanceFactory::default(); "with Wasmtime"))]
#[allow(clippy::bool_assert_comparison)]
fn test_user_data<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: Instance<UserData = Arc<AtomicBool>> + InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedSimpleFunctionWithUserData<InstanceFactory::Caller<'static>>:
        ExportTo<InstanceFactory::Builder>,
{
    let instance = factory
        .load_test_module::<ExportedSimpleFunctionWithUserData<_>>("reentrancy", "simple-function");

    let user_data = instance.user_data().clone();

    assert_eq!(user_data.load(Ordering::Relaxed), false);

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");

    assert_eq!(user_data.load(Ordering::Relaxed), true);
}

/// Test the generated [`WitInterface`] implementations for the types used in this test.
#[test_case(PhantomData::<Entrypoint<MockInstance<()>>>, ENTRYPOINT; "of_entrypoint")]
#[test_case(
    PhantomData::<ImportedSimpleFunction<MockInstance<()>>>, SIMPLE_FUNCTION;
    "of_imported_simple_function"
)]
#[test_case(PhantomData::<ImportedGetters<MockInstance<()>>>, GETTERS; "of_imported_getters")]
#[test_case(PhantomData::<ImportedSetters<MockInstance<()>>>, SETTERS; "of_imported_setters")]
#[test_case(
    PhantomData::<ImportedOperations<MockInstance<()>>>, OPERATIONS;
    "of_imported_operations"
)]
#[test_case(PhantomData::<ExportedSimpleFunction>, SIMPLE_FUNCTION; "of_exported_simple_function")]
#[test_case(PhantomData::<ExportedGetters<MockInstance<()>>>, GETTERS; "of_exported_getters")]
#[test_case(PhantomData::<ExportedSetters<MockInstance<()>>>, SETTERS; "of_exported_setters")]
#[test_case(
    PhantomData::<ExportedOperations<MockInstance<()>>>, OPERATIONS;
    "of_exported_operations"
)]
fn test_wit_interface<Interface>(
    _: PhantomData<Interface>,
    expected_snippets: (&str, &[&str], &[(&str, &str)]),
) where
    Interface: WitInterface,
{
    wit_interface_test::test_wit_interface::<Interface>(expected_snippets);
}

#[test_case(PhantomData::<Entrypoint<MockInstance<()>>>, "entrypoint"; "of_entrypoint")]
#[test_case(
    PhantomData::<ImportedSimpleFunction<MockInstance<()>>>, "simple_function";
    "of_imported_simple_function"
)]
#[test_case(PhantomData::<ImportedGetters<MockInstance<()>>>, "getters"; "of_imported_getters")]
#[test_case(PhantomData::<ImportedSetters<MockInstance<()>>>, "setters"; "of_imported_setters")]
#[test_case(
    PhantomData::<ImportedOperations<MockInstance<()>>>, "operations";
    "of_imported_operations"
)]
#[test_case(PhantomData::<ExportedSimpleFunction>, "simple_function"; "of_exported_simple_function")]
#[test_case(PhantomData::<ExportedGetters<MockInstance<()>>>, "getters"; "of_exported_getters")]
#[test_case(PhantomData::<ExportedSetters<MockInstance<()>>>, "setters"; "of_exported_setters")]
#[test_case(
    PhantomData::<ExportedOperations<MockInstance<()>>>, "operations";
    "of_exported_operations"
)]
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
        .export::<Entrypoint<MockInstance<()>>>()
        .export::<ImportedSimpleFunction<MockInstance<()>>>()
        .export::<ImportedGetters<MockInstance<()>>>()
        .export::<ImportedSetters<MockInstance<()>>>()
        .export::<ImportedOperations<MockInstance<()>>>()
        .import::<ExportedSimpleFunction>()
        .import::<ExportedGetters<MockInstance<()>>>()
        .import::<ExportedSetters<MockInstance<()>>>()
        .import::<ExportedOperations<MockInstance<()>>>()
        .generate_file_contents(&mut buffer)
        .unwrap();

    assert_snapshot!(String::from_utf8(buffer).unwrap());
}
