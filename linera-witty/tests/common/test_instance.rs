// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper code for testing using different runtimes.

use frunk::{hlist, hlist_pat, HList};
#[cfg(feature = "wasmer")]
use linera_witty::wasmer;
#[cfg(feature = "wasmtime")]
use linera_witty::wasmtime;
use linera_witty::{InstanceWithMemory, MockExportedFunction, MockInstance, RuntimeError};
use std::any::Any;

/// Trait representing a type that can create instances for tests.
pub trait TestInstanceFactory {
    /// The type used to build a guest Wasm instance, which supports registration of exported host
    /// functions.
    type Builder;

    /// The type representing a guest Wasm instance.
    type Instance: InstanceWithMemory;

    /// Loads a test module with the provided `module_name` from the named `group`_
    fn load_test_module(
        &mut self,
        group: &str,
        module_name: &str,
        add_exports: impl Fn(&mut Self::Builder),
    ) -> Self::Instance;
}

/// A factory of [`wasmtime::Entrypoint`] instances.
#[cfg(feature = "wasmtime")]
pub struct WasmtimeInstanceFactory;

#[cfg(feature = "wasmtime")]
impl TestInstanceFactory for WasmtimeInstanceFactory {
    type Builder = ::wasmtime::Linker<()>;
    type Instance = wasmtime::EntrypointInstance;

    fn load_test_module(
        &mut self,
        group: &str,
        module: &str,
        add_exports: impl Fn(&mut Self::Builder),
    ) -> Self::Instance {
        let engine = ::wasmtime::Engine::default();
        let module = ::wasmtime::Module::from_file(
            &engine,
            format!("../target/wasm32-unknown-unknown/debug/{group}-{module}.wasm"),
        )
        .expect("Failed to load module");

        let mut linker = wasmtime::Linker::new(&engine);
        add_exports(&mut linker);

        let mut store = ::wasmtime::Store::new(&engine, ());
        let instance = linker
            .instantiate(&mut store, &module)
            .expect("Failed to instantiate module");

        wasmtime::EntrypointInstance::new(instance, store)
    }
}

/// A factory of [`wasmer::EntrypointInstance`]s.
#[cfg(feature = "wasmer")]
pub struct WasmerInstanceFactory;

#[cfg(feature = "wasmer")]
impl TestInstanceFactory for WasmerInstanceFactory {
    type Builder = wasmer::InstanceBuilder;
    type Instance = wasmer::EntrypointInstance;

    fn load_test_module(
        &mut self,
        group: &str,
        module: &str,
        add_exports: impl Fn(&mut Self::Builder),
    ) -> Self::Instance {
        let engine = ::wasmer::EngineBuilder::new(::wasmer::Singlepass::default()).engine();
        let module = ::wasmer::Module::from_file(
            &engine,
            format!("../target/wasm32-unknown-unknown/debug/{group}-{module}.wasm"),
        )
        .expect("Failed to load module");

        let mut builder = wasmer::InstanceBuilder::new(engine);

        add_exports(&mut builder);

        builder
            .instantiate(&module)
            .expect("Failed to instantiate module")
    }
}

/// A factory of [`MockInstance`]s.
#[derive(Default)]
pub struct MockInstanceFactory {
    deferred_assertions: Vec<Box<dyn Any>>,
}

impl TestInstanceFactory for MockInstanceFactory {
    type Builder = MockInstance;
    type Instance = MockInstance;

    fn load_test_module(
        &mut self,
        group: &str,
        module: &str,
        add_exports: impl Fn(&mut Self::Builder),
    ) -> Self::Instance {
        let mut instance = MockInstance::default();

        match (group, module) {
            ("export", "simple-function") => self.export_simple_function(&mut instance),
            ("export", "getters") => self.export_getters(&mut instance),
            ("export", "setters") => self.export_setters(&mut instance),
            ("export", "operations") => self.export_operations(&mut instance),
            _ => panic!(
                "Attempt to load module \"{group}-{module}\" which has no mock configuration"
            ),
        }

        add_exports(&mut instance);

        instance
    }
}

impl MockInstanceFactory {
    /// Mock the exported functions for the "simple-function" module.
    fn export_simple_function(&mut self, instance: &mut MockInstance) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/simple-function#simple",
            |_: HList![]| Ok(hlist![]),
            1,
        );
    }

    /// Mock the exported functions for the "getters" module.
    fn export_getters(&mut self, instance: &mut MockInstance) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-true",
            |_: HList![]| Ok(hlist![1_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-false",
            |_: HList![]| Ok(hlist![0_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s8",
            |_: HList![]| Ok(hlist![-125_i8 as u8 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u8",
            |_: HList![]| Ok(hlist![200_u8 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s16",
            |_: HList![]| Ok(hlist![-410_i16 as u16 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u16",
            |_: HList![]| Ok(hlist![60_000_u16 as i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s32",
            |_: HList![]| Ok(hlist![-100_000_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u32",
            |_: HList![]| Ok(hlist![3_000_111_i32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-s64",
            |_: HList![]| Ok(hlist![-5_000_000_i64]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-u64",
            |_: HList![]| Ok(hlist![10_000_000_000_i64]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-float32",
            |_: HList![]| Ok(hlist![-0.125_f32]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/getters#get-float64",
            |_: HList![]| Ok(hlist![128.25_f64]),
            1,
        );
    }

    /// Mock the exported functions for the "setters" module.
    fn export_setters(&mut self, instance: &mut MockInstance) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-bool",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 0);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s8",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -100_i8 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u8",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 201);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s16",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -20_000_i16 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u16",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 50_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s32",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, -2_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u32",
            |hlist_pat![parameter]: HList![i32]| {
                assert_eq!(parameter, 4_000_000_u32 as i32);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-s64",
            |hlist_pat![parameter]: HList![i64]| {
                assert_eq!(parameter, -25_000_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-u64",
            |hlist_pat![parameter]: HList![i64]| {
                assert_eq!(parameter, 7_000_000_000);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-float32",
            |hlist_pat![parameter]: HList![f32]| {
                assert_eq!(parameter, 10.5);
                Ok(hlist![])
            },
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/setters#set-float64",
            |hlist_pat![parameter]: HList![f64]| {
                assert_eq!(parameter, -0.000_08);
                Ok(hlist![])
            },
            1,
        );
    }

    /// Mock the exported functions for the "operations" module.
    fn export_operations(&mut self, instance: &mut MockInstance) {
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#and-bool",
            |hlist_pat![first, second]: HList![i32, i32]| {
                Ok(hlist![if first != 0 && second != 0 { 1 } else { 0 }])
            },
            2,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s8",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u8",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s16",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u16",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s32",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u32",
            |hlist_pat![first, second]: HList![i32, i32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-s64",
            |hlist_pat![first, second]: HList![i64, i64]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-u64",
            |hlist_pat![first, second]: HList![i64, i64]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-float32",
            |hlist_pat![first, second]: HList![f32, f32]| Ok(hlist![first + second]),
            1,
        );
        self.mock_exported_function(
            instance,
            "witty-macros:test-modules/operations#add-float64",
            |hlist_pat![first, second]: HList![f64, f64]| Ok(hlist![first + second]),
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
