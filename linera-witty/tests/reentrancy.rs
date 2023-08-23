// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `wit_import` and `wit_export` attribute macro using reentrant host functions.

#[path = "common/test_instance.rs"]
mod test_instance;

#[cfg(feature = "wasmer")]
use self::test_instance::WasmerInstanceFactory;
#[cfg(feature = "wasmtime")]
use self::test_instance::WasmtimeInstanceFactory;
use self::test_instance::{MockInstanceFactory, TestInstanceFactory};
use linera_witty::{
    wit_export, wit_import, ExportTo, Instance, Runtime, RuntimeError, RuntimeMemory,
};
use test_case::test_case;

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
    fn simple<Caller>(caller: Caller) -> Result<(), RuntimeError>
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
#[cfg_attr(feature = "wasmer", test_case(WasmerInstanceFactory; "with Wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmtimeInstanceFactory; "with Wasmtime"))]
fn simple_function<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedSimpleFunction: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module("reentrancy", "simple-function", |linker| {
        ExportedSimpleFunction::export_to(linker)
            .expect("Failed to export reentrant simple function WIT interface")
    });

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
pub struct ExportedGetters;

#[wit_export(package = "witty-macros:test-modules", interface = "getters")]
impl ExportedGetters {
    fn get_true<Caller>(caller: Caller) -> Result<bool, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_true()
    }

    fn get_false<Caller>(caller: Caller) -> Result<bool, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_false()
    }

    fn get_s8<Caller>(caller: Caller) -> Result<i8, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_s8()
    }

    fn get_u8<Caller>(caller: Caller) -> Result<u8, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_u8()
    }

    fn get_s16<Caller>(caller: Caller) -> Result<i16, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_s16()
    }

    fn get_u16<Caller>(caller: Caller) -> Result<u16, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_u16()
    }

    fn get_s32<Caller>(caller: Caller) -> Result<i32, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_s32()
    }

    fn get_u32<Caller>(caller: Caller) -> Result<u32, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_u32()
    }

    fn get_s64<Caller>(caller: Caller) -> Result<i64, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_s64()
    }

    fn get_u64<Caller>(caller: Caller) -> Result<u64, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_u64()
    }

    fn get_float32<Caller>(caller: Caller) -> Result<f32, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_float32()
    }

    fn get_float64<Caller>(caller: Caller) -> Result<f64, RuntimeError>
    where
        Caller: InstanceForImportedGetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedGetters::new(caller).get_float64()
    }
}

/// Test reentrant functions with return values.
///
/// The host functions are called from the guest, and they return values obtained by calling back
/// the guest through functions with the same names.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(feature = "wasmer", test_case(WasmerInstanceFactory; "with Wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmtimeInstanceFactory; "with Wasmtime"))]
fn getters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedGetters: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module("reentrancy", "getters", |instance| {
        ExportedGetters::export_to(instance)
            .expect("Failed to export reentrant getters WIT interface")
    });

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
pub struct ExportedSetters;

#[wit_export(package = "witty-macros:test-modules", interface = "setters")]
impl ExportedSetters {
    fn set_bool<Caller>(caller: Caller, value: bool) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_bool(value)
    }

    fn set_s8<Caller>(caller: Caller, value: i8) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_s8(value)
    }

    fn set_u8<Caller>(caller: Caller, value: u8) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_u8(value)
    }

    fn set_s16<Caller>(caller: Caller, value: i16) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_s16(value)
    }

    fn set_u16<Caller>(caller: Caller, value: u16) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_u16(value)
    }

    fn set_s32<Caller>(caller: Caller, value: i32) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_s32(value)
    }

    fn set_u32<Caller>(caller: Caller, value: u32) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_u32(value)
    }

    fn set_s64<Caller>(caller: Caller, value: i64) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_s64(value)
    }

    fn set_u64<Caller>(caller: Caller, value: u64) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_u64(value)
    }

    fn set_float32<Caller>(caller: Caller, value: f32) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_float32(value)
    }

    fn set_float64<Caller>(caller: Caller, value: f64) -> Result<(), RuntimeError>
    where
        Caller: InstanceForImportedSetters,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedSetters::new(caller).set_float64(value)
    }
}

/// Test reentrant functions with parameters.
///
/// The host functions are called from the guest, and they forward the arguments back to the guest
/// by calling guest functions with the same names.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(feature = "wasmer", test_case(WasmerInstanceFactory; "with Wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmtimeInstanceFactory; "with Wasmtime"))]
fn setters<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedSetters: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module("reentrancy", "setters", |instance| {
        ExportedSetters::export_to(instance)
            .expect("Failed to export reentrant setters WIT interface")
    });

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
pub struct ExportedOperations;

#[wit_export(package = "witty-macros:test-modules", interface = "operations")]
impl ExportedOperations {
    fn and_bool<Caller>(caller: Caller, first: bool, second: bool) -> Result<bool, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).and_bool(first, second)
    }

    fn add_s8<Caller>(caller: Caller, first: i8, second: i8) -> Result<i8, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_s8(first, second)
    }

    fn add_u8<Caller>(caller: Caller, first: u8, second: u8) -> Result<u8, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_u8(first, second)
    }

    fn add_s16<Caller>(caller: Caller, first: i16, second: i16) -> Result<i16, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_s16(first, second)
    }

    fn add_u16<Caller>(caller: Caller, first: u16, second: u16) -> Result<u16, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_u16(first, second)
    }

    fn add_s32<Caller>(caller: Caller, first: i32, second: i32) -> Result<i32, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_s32(first, second)
    }

    fn add_u32<Caller>(caller: Caller, first: u32, second: u32) -> Result<u32, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_u32(first, second)
    }

    fn add_s64<Caller>(caller: Caller, first: i64, second: i64) -> Result<i64, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_s64(first, second)
    }

    fn add_u64<Caller>(caller: Caller, first: u64, second: u64) -> Result<u64, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_u64(first, second)
    }

    fn add_float32<Caller>(caller: Caller, first: f32, second: f32) -> Result<f32, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_float32(first, second)
    }

    fn add_float64<Caller>(caller: Caller, first: f64, second: f64) -> Result<f64, RuntimeError>
    where
        Caller: InstanceForImportedOperations,
        <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
    {
        ImportedOperations::new(caller).add_float64(first, second)
    }
}

/// Test reentrant functions with multiple parameters and with return values.
///
/// The host functions are called from the guest, and they call the guest back through functions
/// with the same names, forwarding the arguments and retrieving the final results.
#[test_case(MockInstanceFactory::default(); "with a mock instance")]
#[cfg_attr(feature = "wasmer", test_case(WasmerInstanceFactory; "with Wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmtimeInstanceFactory; "with Wasmtime"))]
fn operations<InstanceFactory>(mut factory: InstanceFactory)
where
    InstanceFactory: TestInstanceFactory,
    InstanceFactory::Instance: InstanceForEntrypoint,
    <<InstanceFactory::Instance as Instance>::Runtime as Runtime>::Memory:
        RuntimeMemory<InstanceFactory::Instance>,
    ExportedOperations: ExportTo<InstanceFactory::Builder>,
{
    let instance = factory.load_test_module("reentrancy", "operations", |instance| {
        ExportedOperations::export_to(instance)
            .expect("Failed to export reentrant operations WIT interface")
    });

    Entrypoint::new(instance)
        .entrypoint()
        .expect("Failed to call guest's `entrypoint` function");
}
