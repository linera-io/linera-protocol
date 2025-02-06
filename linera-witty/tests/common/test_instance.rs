// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper code for testing using different runtimes.

use std::{
    any::Any,
    fmt::Debug,
    marker::PhantomData,
    ops::Add,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use frunk::{hlist, hlist_pat, HList};
#[cfg(with_wasmer)]
use linera_witty::wasmer;
#[cfg(with_wasmtime)]
use linera_witty::wasmtime;
use linera_witty::{
    ExportTo, InstanceWithMemory, Layout, MockExportedFunction, MockInstance, RuntimeError,
    WitLoad, WitStore,
};

/// Trait representing a type that can create instances for tests.
pub trait TestInstanceFactory {
    /// The type used to build a guest Wasm instance, which supports registration of exported host
    /// functions.
    type Builder;

    /// The type representing a guest Wasm instance.
    type Instance: InstanceWithMemory;

    /// The type received by host functions to use for reentrant calls.
    type Caller<'caller>;

    /// Loads a test module with the provided `module_name` from the named `group`
    fn load_test_module<ExportedFunctions>(
        &mut self,
        group: &str,
        module_name: &str,
    ) -> Self::Instance
    where
        ExportedFunctions: ExportTo<Self::Builder>;
}

/// A factory of [`wasmtime::Entrypoint`] instances.
#[cfg(with_wasmtime)]
#[derive(Default)]
pub struct WasmtimeInstanceFactory<UserData>(PhantomData<UserData>);

#[cfg(with_wasmtime)]
impl<UserData> TestInstanceFactory for WasmtimeInstanceFactory<UserData>
where
    UserData: Default + 'static,
{
    type Builder = ::wasmtime::Linker<UserData>;
    type Instance = wasmtime::EntrypointInstance<UserData>;
    type Caller<'caller> = ::wasmtime::Caller<'caller, UserData>;

    fn load_test_module<ExportedFunctions>(&mut self, group: &str, module: &str) -> Self::Instance
    where
        ExportedFunctions: ExportTo<Self::Builder>,
    {
        let engine = ::wasmtime::Engine::default();
        let module = ::wasmtime::Module::from_file(
            &engine,
            format!("../target/wasm32-unknown-unknown/debug/{group}-{module}.wasm"),
        )
        .expect("Failed to load module");

        let mut linker = wasmtime::Linker::new(&engine);

        ExportedFunctions::export_to(&mut linker)
            .expect("Failed to export functions to Wasmtime linker");

        let mut store = ::wasmtime::Store::new(&engine, UserData::default());
        let instance = linker
            .instantiate(&mut store, &module)
            .expect("Failed to instantiate module");

        wasmtime::EntrypointInstance::new(instance, store)
    }
}

/// A factory of [`wasmer::EntrypointInstance`]s.
#[cfg(with_wasmer)]
#[derive(Default)]
pub struct WasmerInstanceFactory<UserData>(PhantomData<UserData>);

#[cfg(with_wasmer)]
impl<UserData> TestInstanceFactory for WasmerInstanceFactory<UserData>
where
    UserData: Default + Send + 'static,
{
    type Builder = wasmer::InstanceBuilder<UserData>;
    type Instance = wasmer::EntrypointInstance<UserData>;
    type Caller<'caller> = ::wasmer::FunctionEnvMut<'caller, wasmer::Environment<UserData>>;

    fn load_test_module<ExportedFunctions>(&mut self, group: &str, module: &str) -> Self::Instance
    where
        ExportedFunctions: ExportTo<Self::Builder>,
    {
        let engine = ::wasmer::sys::EngineBuilder::new(::wasmer::Singlepass::default())
            .engine()
            .into();
        let module = ::wasmer::Module::from_file(
            &engine,
            format!("../target/wasm32-unknown-unknown/debug/{group}-{module}.wasm"),
        )
        .expect("Failed to load module");

        let mut builder = wasmer::InstanceBuilder::new(engine, UserData::default());

        ExportedFunctions::export_to(&mut builder)
            .expect("Failed to export functions to Wasmer instance builder");

        builder
            .instantiate(&module)
            .expect("Failed to instantiate module")
    }
}

/// A factory of [`MockInstance`]s.
#[derive(Default)]
pub struct MockInstanceFactory<UserData = ()> {
    deferred_assertions: Vec<Box<dyn Any>>,
    user_data: PhantomData<UserData>,
}

impl<UserData> TestInstanceFactory for MockInstanceFactory<UserData>
where
    UserData: Default + 'static,
{
    type Builder = MockInstance<UserData>;
    type Instance = MockInstance<UserData>;
    type Caller<'caller> = MockInstance<UserData>;

    fn load_test_module<ExportedFunctions>(&mut self, group: &str, module: &str) -> Self::Instance
    where
        ExportedFunctions: ExportTo<Self::Builder>,
    {
        let mut instance = MockInstance::default();

        match (group, module) {
            ("export", "simple-function") => self.export_simple_function(&mut instance),
            ("export", "getters") => self.export_getters(&mut instance),
            ("export", "setters") => self.export_setters(&mut instance),
            ("export", "operations") => self.export_operations(&mut instance),
            ("import", "simple-function") => self.import_simple_function(&mut instance),
            ("import", "getters") => self.import_getters(&mut instance),
            ("import", "setters") => self.import_setters(&mut instance),
            ("import", "operations") => self.import_operations(&mut instance),
            ("reentrancy", "simple-function") => self.reentrancy_simple_function(&mut instance),
            ("reentrancy", "getters") => self.reentrancy_getters(&mut instance),
            ("reentrancy", "setters") => self.reentrancy_setters(&mut instance),
            ("reentrancy", "operations") => self.reentrancy_operations(&mut instance),
            ("reentrancy", "global-state") => self.reentrancy_global_state(&mut instance),
            _ => panic!(
                "Attempt to load module \"{group}-{module}\" which has no mock configuration"
            ),
        }

        ExportedFunctions::export_to(&mut instance)
            .expect("Failed to export functions to mock instance");

        instance
    }
}

impl<UserData> MockInstanceFactory<UserData>
where
    UserData: 'static,
{
    /// Mock the exported functions from the "export-simple-function" module.
    fn export_simple_function(&mut self, instance: &mut MockInstance<UserData>) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/simple-function#simple",
            |_, _: HList![]| Ok(hlist![]),
            1,
        );
    }

    /// Mock the exported functions from the "export-getters" module.
    fn export_getters(&mut self, instance: &mut MockInstance<UserData>) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-true",
            |_, _: HList![]| Ok(hlist![1_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-false",
            |_, _: HList![]| Ok(hlist![0_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s8",
            |_, _: HList![]| Ok(hlist![-125_i8 as u8 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u8",
            |_, _: HList![]| Ok(hlist![200_u8 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s16",
            |_, _: HList![]| Ok(hlist![-410_i16 as u16 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u16",
            |_, _: HList![]| Ok(hlist![60_000_u16 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s32",
            |_, _: HList![]| Ok(hlist![-100_000_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u32",
            |_, _: HList![]| Ok(hlist![3_000_111_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s64",
            |_, _: HList![]| Ok(hlist![-5_000_000_i64]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u64",
            |_, _: HList![]| Ok(hlist![10_000_000_000_i64]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-float32",
            |_, _: HList![]| Ok(hlist![-0.125_f32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-float64",
            |_, _: HList![]| Ok(hlist![128.25_f64]),
            1,
        );
    }

    /// Mock the exported functions from the "export-setters" module.
    fn export_setters(&mut self, instance: &mut MockInstance<UserData>) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-bool",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 0);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s8",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -100_i8 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u8",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 201);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s16",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -20_000_i16 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u16",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 50_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s32",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -2_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u32",
            |_, hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 4_000_000_u32 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s64",
            |_, hlist_pat![parameter]: HList![i64]| {
                assert_eq!(parameter, -25_000_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u64",
            |_, hlist_pat![parameter]: HList![i64]| {
                assert_eq!(parameter, 7_000_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-float32",
            |_, hlist_pat![parameter]: HList![f32]| {
                assert_eq!(parameter, 10.4);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-float64",
            |_, hlist_pat![parameter]: HList![f64]| {
                assert_eq!(parameter, -0.000_08);
                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock the exported functions from the "operations" module.
    fn export_operations(&mut self, instance: &mut MockInstance<UserData>) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#and-bool",
            |_, hlist_pat![first, second]: HList![i32, i32]| {
                Ok(hlist![if first != 0 && second != 0 { 1 } else { 0 }])
            },
            2,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s8",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u8",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s16",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u16",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s32",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u32",
            |_, hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s64",
            |_, hlist_pat![first, second]: HList![i64, i64]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u64",
            |_, hlist_pat![first, second]: HList![i64, i64]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-float32",
            |_, hlist_pat![first, second]: HList![f32, f32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-float64",
            |_, hlist_pat![first, second]: HList![f64, f64]| Ok(hlist![first + second]),
            1,
        );
    }

    /// Mock calling the imported function in the "import-simple-function" module.
    fn import_simple_function(&mut self, instance: &mut MockInstance<UserData>) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/entrypoint#entrypoint",
            |caller, _: HList![]| {
                let hlist_pat![] = caller.call_imported_function(
                    "witty-macros:test-modules/simple-function#simple",
                    hlist![],
                )?;
                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock calling the imported functions in the "import-getters" module.
    fn import_getters(&mut self, instance: &mut MockInstance<UserData>) {
        fn check_getter<Value, UserData>(
            caller: &MockInstance<UserData>,
            name: &str,
            expected_value: Value,
        ) where
            Value: Debug + PartialEq + WitLoad + 'static,
        {
            let value: Value = caller
                .call_imported_function(
                    &format!("witty-macros:test-modules/getters#{name}"),
                    hlist![],
                )
                .unwrap_or_else(|error| panic!("Failed to call getter function {name:?}: {error}"));

            assert_eq!(value, expected_value);
        }

        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/entrypoint#entrypoint",
            |caller, _: HList![]| {
                check_getter(&caller, "get-true", true);
                check_getter(&caller, "get-false", false);
                check_getter(&caller, "get-s8", -125_i8);
                check_getter(&caller, "get-u8", 200_u8);
                check_getter(&caller, "get-s16", -410_i16);
                check_getter(&caller, "get-u16", 60_000_u16);
                check_getter(&caller, "get-s32", -100_000_i32);
                check_getter(&caller, "get-u32", 3_000_111_u32);
                check_getter(&caller, "get-s64", -5_000_000_i64);
                check_getter(&caller, "get-u64", 10_000_000_000_u64);
                check_getter(&caller, "get-float32", -0.125_f32);
                check_getter(&caller, "get-float64", 128.25_f64);

                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock calling the imported functions in the "import-setters" module.
    fn import_setters(&mut self, instance: &mut MockInstance<UserData>) {
        fn send_to_setter<Value, UserData>(
            caller: &MockInstance<UserData>,
            name: &str,
            value: Value,
        ) where
            Value: WitStore + 'static,
            Value::Layout: Add<HList![]>,
            <Value::Layout as Add<HList![]>>::Output:
                Layout<Flat = <<Value::Layout as Layout>::Flat as Add<HList![]>>::Output>,
            <Value::Layout as Layout>::Flat: Add<HList![]>,
        {
            let () = caller
                .call_imported_function(
                    &format!("witty-macros:test-modules/setters#{name}"),
                    hlist![value],
                )
                .unwrap_or_else(|error| panic!("Failed to call setter function {name:?}: {error}"));
        }

        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/entrypoint#entrypoint",
            |caller, _: HList![]| {
                send_to_setter(&caller, "set-bool", false);
                send_to_setter(&caller, "set-s8", -100_i8);
                send_to_setter(&caller, "set-u8", 201_u8);
                send_to_setter(&caller, "set-s16", -20_000_i16);
                send_to_setter(&caller, "set-u16", 50_000_u16);
                send_to_setter(&caller, "set-s32", -2_000_000_i32);
                send_to_setter(&caller, "set-u32", 4_000_000_u32);
                send_to_setter(&caller, "set-s64", -25_000_000_000_i64);
                send_to_setter(&caller, "set-u64", 7_000_000_000_u64);
                send_to_setter(&caller, "set-float32", 10.4_f32);
                send_to_setter(&caller, "set-float64", -0.000_08_f64);

                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock calling the imported functions in the "import-operations".
    fn import_operations(&mut self, instance: &mut MockInstance<UserData>) {
        fn check_operation<Value, UserData>(
            caller: &MockInstance<UserData>,
            name: &str,
            operands: impl WitStore + 'static,
            expected_result: Value,
        ) where
            Value: Debug + PartialEq + WitLoad + 'static,
        {
            let result: Value = caller
                .call_imported_function(
                    &format!("witty-macros:test-modules/operations#{name}"),
                    operands,
                )
                .unwrap_or_else(|error| panic!("Failed to call setter function {name:?}: {error}"));

            assert_eq!(result, expected_result);
        }

        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/entrypoint#entrypoint",
            |caller, _: HList![]| {
                check_operation(&caller, "and-bool", (true, true), true);
                check_operation(&caller, "and-bool", (true, false), false);
                check_operation(&caller, "add-s8", (-100_i8, 40_i8), -60_i8);
                check_operation(&caller, "add-u8", (201_u8, 32_u8), 233_u8);
                check_operation(&caller, "add-s16", (-20_000_i16, 30_000_i16), 10_000_i16);
                check_operation(&caller, "add-u16", (50_000_u16, 256_u16), 50_256_u16);
                check_operation(&caller, "add-s32", (-2_000_000_i32, -1_i32), -2_000_001_i32);
                check_operation(&caller, "add-u32", (4_000_000_u32, 1_u32), 4_000_001_u32);
                check_operation(
                    &caller,
                    "add-s64",
                    (-16_000_000_i64, 32_000_000_i64),
                    16_000_000_i64,
                );
                check_operation(
                    &caller,
                    "add-u64",
                    (3_000_000_000_u64, 9_345_678_999_u64),
                    12_345_678_999_u64,
                );
                check_operation(&caller, "add-float32", (10.5_f32, 120.25_f32), 130.75_f32);
                check_operation(
                    &caller,
                    "add-float64",
                    (-0.000_08_f64, 1.0_f64),
                    0.999_92_f64,
                );

                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock the behavior of the "reentrancy-simple-function" module.
    fn reentrancy_simple_function(&mut self, instance: &mut MockInstance<UserData>) {
        self.import_simple_function(instance);
        self.export_simple_function(instance);
    }

    /// Mock the behavior of the "reentrancy-getters" module.
    fn reentrancy_getters(&mut self, instance: &mut MockInstance<UserData>) {
        self.import_getters(instance);
        self.export_getters(instance);
    }

    /// Mock the behavior of the "reentrancy-setters" module.
    fn reentrancy_setters(&mut self, instance: &mut MockInstance<UserData>) {
        self.import_setters(instance);
        self.export_setters(instance);
    }

    /// Mock the behavior of the "reentrancy-operations" module.
    fn reentrancy_operations(&mut self, instance: &mut MockInstance<UserData>) {
        self.import_operations(instance);
        self.export_operations(instance);
    }

    /// Mock the behavior of the "reentrancy-global-state" module.
    fn reentrancy_global_state(&mut self, instance: &mut MockInstance<UserData>) {
        let global_state_for_entrypoint = Arc::new(AtomicU32::new(0));
        let global_state_for_getter = global_state_for_entrypoint.clone();

        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/global-state#entrypoint",
            move |caller, hlist_pat![value]: HList![i32]| {
                global_state_for_entrypoint.store(value as u32, Ordering::Release);

                caller
                    .call_imported_function(
                        "witty-macros:test-modules/get-host-value#get-host-value",
                        (),
                    )
                    .map(|value: u32| hlist![value as i32])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/global-state#get-global-state",
            move |_, _: HList![]| {
                Ok(hlist![
                    global_state_for_getter.load(Ordering::Acquire) as i32
                ])
            },
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
        instance: &mut MockInstance<UserData>,
        name: &str,
        handler: impl Fn(MockInstance<UserData>, Parameters) -> Result<Results, RuntimeError> + 'static,
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

/// Marker type to indicate no extra functions should be exported to the Wasm instance.
#[allow(dead_code)]
pub struct WithoutExports;

impl<T> ExportTo<T> for WithoutExports {
    fn export_to(_target: &mut T) -> Result<(), RuntimeError> {
        Ok(())
    }
}
