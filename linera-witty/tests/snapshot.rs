// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Round-trip serialization of Wasm-instance snapshots.
//!
//! These tests exercise the snapshot/restore cycle for both Wasmer and Wasmtime,
//! using a hand-written Wasm module that exposes all four numeric mutable global
//! types (i32, i64, f32, f64) plus linear memory. Each backend has two tests:
//!
//! 1. **all_numeric_types_roundtrip** — mutates every global type and a memory
//!    region, snapshots, encodes to BCS bytes, decodes on a fresh instance,
//!    restores, and verifies every observable value matches.
//!
//! 2. **two_snapshots_are_independent** — captures two snapshots at different
//!    points in the same instance's life, then restores each onto its own fresh
//!    instance and verifies that each restored instance reflects the state at
//!    the moment its snapshot was taken (i.e. snapshots are independent value
//!    captures, not aliases of live state).

#![cfg(any(with_wasmer, with_wasmtime))]

const WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (table $t (export "indirect") 2 funcref)
  (global $g_i32 (export "g_i32") (mut i32) (i32.const 0))
  (global $g_i64 (export "g_i64") (mut i64) (i64.const 0))
  (global $g_f32 (export "g_f32") (mut f32) (f32.const 0))
  (global $g_f64 (export "g_f64") (mut f64) (f64.const 0))

  (func (export "set_i32") (param i32) local.get 0 global.set $g_i32)
  (func (export "set_i64") (param i64) local.get 0 global.set $g_i64)
  (func (export "set_f32") (param f32) local.get 0 global.set $g_f32)
  (func (export "set_f64") (param f64) local.get 0 global.set $g_f64)

  (func (export "get_i32") (result i32) global.get $g_i32)
  (func (export "get_i64") (result i64) global.get $g_i64)
  (func (export "get_f32") (result f32) global.get $g_f32)
  (func (export "get_f64") (result f64) global.get $g_f64)

  (func (export "write_byte") (param i32 i32)
    local.get 0
    local.get 1
    i32.store8)
  (func (export "read_byte") (param i32) (result i32)
    local.get 0
    i32.load8_u)
  (func (export "grow_pages") (param i32) (result i32)
    local.get 0
    memory.grow)
  (func (export "grow_table") (param i32) (result i32)
    ref.null func
    local.get 0
    table.grow $t))
"#;

/// A byte offset that lives in the second page of linear memory: the snapshot
/// captures it, but a freshly built instance has only one initial page, so
/// restoring must grow the live memory before copying.
const SECOND_PAGE_OFFSET: i32 = 70_000;

/// A reference state used by both backends. Matching values land on instances of
/// every numeric global plus a small varied byte pattern in linear memory.
#[derive(Clone, Copy)]
struct State {
    i32_val: i32,
    i64_val: i64,
    f32_val: f32,
    f64_val: f64,
    /// A small varied pattern written at offsets `0..bytes.len()` of memory.
    bytes: [u8; 8],
}

const STATE_A: State = State {
    i32_val: -123_456,
    i64_val: 9_876_543_210_987,
    f32_val: -0.125_f32,
    f64_val: 12_345.678_9_f64,
    bytes: [0xde, 0xad, 0xbe, 0xef, 0x00, 0xff, 0x42, 0x77],
};

const STATE_B: State = State {
    i32_val: 0x4242_4242,
    i64_val: -1,
    f32_val: f32::INFINITY,
    f64_val: -0.0_f64,
    bytes: [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
};

#[cfg(with_wasmer)]
mod wasmer_tests {
    use linera_witty::wasmer::{InstanceBuilder, WasmInstanceSnapshot};
    use wasmer::{
        sys::{EngineBuilder, Singlepass},
        Engine, Module,
    };

    use super::{State, STATE_A, STATE_B, WAT};

    fn engine() -> Engine {
        EngineBuilder::new(Singlepass::default()).engine().into()
    }

    fn build_instance(
        engine: Engine,
        module: &Module,
    ) -> linera_witty::wasmer::EntrypointInstance<()> {
        InstanceBuilder::<()>::new(engine, ())
            .instantiate(module)
            .expect("instantiate")
    }

    fn write_state(instance: &mut linera_witty::wasmer::EntrypointInstance<()>, state: &State) {
        let (mut store, wasmer_instance) = instance.as_store_and_instance_mut();
        wasmer_instance
            .exports
            .get_function("set_i32")
            .unwrap()
            .call(&mut store, &[wasmer::Value::I32(state.i32_val)])
            .unwrap();
        wasmer_instance
            .exports
            .get_function("set_i64")
            .unwrap()
            .call(&mut store, &[wasmer::Value::I64(state.i64_val)])
            .unwrap();
        wasmer_instance
            .exports
            .get_function("set_f32")
            .unwrap()
            .call(&mut store, &[wasmer::Value::F32(state.f32_val)])
            .unwrap();
        wasmer_instance
            .exports
            .get_function("set_f64")
            .unwrap()
            .call(&mut store, &[wasmer::Value::F64(state.f64_val)])
            .unwrap();
        for (offset, byte) in state.bytes.iter().enumerate() {
            wasmer_instance
                .exports
                .get_function("write_byte")
                .unwrap()
                .call(
                    &mut store,
                    &[
                        wasmer::Value::I32(offset as i32),
                        wasmer::Value::I32(i32::from(*byte)),
                    ],
                )
                .unwrap();
        }
    }

    fn assert_state(instance: &mut linera_witty::wasmer::EntrypointInstance<()>, expected: &State) {
        let (mut store, wasmer_instance) = instance.as_store_and_instance_mut();
        let i32_val = wasmer_instance
            .exports
            .get_function("get_i32")
            .unwrap()
            .call(&mut store, &[])
            .unwrap();
        assert_eq!(i32_val[0].i32(), Some(expected.i32_val));

        let i64_val = wasmer_instance
            .exports
            .get_function("get_i64")
            .unwrap()
            .call(&mut store, &[])
            .unwrap();
        assert_eq!(i64_val[0].i64(), Some(expected.i64_val));

        let f32_val = wasmer_instance
            .exports
            .get_function("get_f32")
            .unwrap()
            .call(&mut store, &[])
            .unwrap();
        assert_eq!(
            f32_val[0].f32().map(f32::to_bits),
            Some(expected.f32_val.to_bits())
        );

        let f64_val = wasmer_instance
            .exports
            .get_function("get_f64")
            .unwrap()
            .call(&mut store, &[])
            .unwrap();
        assert_eq!(
            f64_val[0].f64().map(f64::to_bits),
            Some(expected.f64_val.to_bits())
        );

        for (offset, byte) in expected.bytes.iter().enumerate() {
            let read = wasmer_instance
                .exports
                .get_function("read_byte")
                .unwrap()
                .call(&mut store, &[wasmer::Value::I32(offset as i32)])
                .unwrap();
            assert_eq!(read[0].i32(), Some(i32::from(*byte)));
        }
    }

    #[test]
    fn all_numeric_types_roundtrip() {
        let engine = engine();
        let module = Module::new(&engine, wat::parse_str(WAT).unwrap()).unwrap();

        // Mutate every numeric global type and a slice of memory, snapshot, encode.
        let bytes = {
            let mut instance = build_instance(engine.clone(), &module);
            write_state(&mut instance, &STATE_A);
            let snapshot = instance.create_snapshot().expect("create_snapshot");
            bcs::to_bytes(&snapshot).expect("serialize")
        };

        // Decode and restore on a fresh instance; observe every value.
        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(engine, &module);
        instance.restore_snapshot(&snapshot).expect("restore_snapshot");
        assert_state(&mut instance, &STATE_A);
    }

    #[test]
    fn two_snapshots_are_independent() {
        let engine = engine();
        let module = Module::new(&engine, wat::parse_str(WAT).unwrap()).unwrap();

        // Drive one instance through two distinct states, capturing a snapshot at each.
        let (bytes_a, bytes_b) = {
            let mut instance = build_instance(engine.clone(), &module);
            write_state(&mut instance, &STATE_A);
            let snap_a = bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize a");
            write_state(&mut instance, &STATE_B);
            let snap_b = bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize b");
            (snap_a, snap_b)
        };

        // Each snapshot, restored onto its own fresh instance, must recover its own state.
        let snap_a: WasmInstanceSnapshot = bcs::from_bytes(&bytes_a).expect("deserialize a");
        let mut instance_a = build_instance(engine.clone(), &module);
        instance_a.restore_snapshot(&snap_a).expect("restore_snapshot");
        assert_state(&mut instance_a, &STATE_A);

        let snap_b: WasmInstanceSnapshot = bcs::from_bytes(&bytes_b).expect("deserialize b");
        let mut instance_b = build_instance(engine, &module);
        instance_b.restore_snapshot(&snap_b).expect("restore_snapshot");
        assert_state(&mut instance_b, &STATE_B);
    }

    #[test]
    fn restore_grows_memory_when_snapshot_is_larger() {
        let engine = engine();
        let module = Module::new(&engine, wat::parse_str(super::WAT).unwrap()).unwrap();

        let bytes = {
            let mut instance = build_instance(engine.clone(), &module);
            let (mut store, wasmer_instance) = instance.as_store_and_instance_mut();
            wasmer_instance
                .exports
                .get_function("grow_pages")
                .unwrap()
                .call(&mut store, &[wasmer::Value::I32(1)])
                .unwrap();
            wasmer_instance
                .exports
                .get_function("write_byte")
                .unwrap()
                .call(
                    &mut store,
                    &[
                        wasmer::Value::I32(super::SECOND_PAGE_OFFSET),
                        wasmer::Value::I32(0xa5),
                    ],
                )
                .unwrap();
            bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize")
        };

        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(engine, &module);
        instance.restore_snapshot(&snapshot).expect("restore_snapshot");

        let (mut store, wasmer_instance) = instance.as_store_and_instance_mut();
        let read = wasmer_instance
            .exports
            .get_function("read_byte")
            .unwrap()
            .call(
                &mut store,
                &[wasmer::Value::I32(super::SECOND_PAGE_OFFSET)],
            )
            .unwrap();
        assert_eq!(read[0].i32(), Some(0xa5));
    }

    #[test]
    fn restore_errors_when_table_size_changed() {
        use linera_witty::SnapshotError;

        let engine = engine();
        let module = Module::new(&engine, wat::parse_str(super::WAT).unwrap()).unwrap();

        // Source instance grows the table, then snapshots.
        let bytes = {
            let mut instance = build_instance(engine.clone(), &module);
            let (mut store, wasmer_instance) = instance.as_store_and_instance_mut();
            wasmer_instance
                .exports
                .get_function("grow_table")
                .unwrap()
                .call(&mut store, &[wasmer::Value::I32(3)])
                .unwrap();
            bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize")
        };

        // Restore on a fresh instance: table size differs, must return an error.
        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(engine, &module);
        let err = instance.restore_snapshot(&snapshot).expect_err("expected error");
        assert!(
            matches!(err, SnapshotError::TableSizeMismatch { .. }),
            "unexpected error: {err}",
        );
    }
}

#[cfg(with_wasmtime)]
mod wasmtime_tests {
    use linera_witty::wasmtime::{EntrypointInstance, WasmInstanceSnapshot};
    use wasmtime::{Engine, Linker, Module, Store};

    use super::{State, STATE_A, STATE_B, WAT};

    fn build_instance(engine: &Engine, module: &Module) -> EntrypointInstance<()> {
        let linker: Linker<()> = Linker::new(engine);
        let mut store = Store::new(engine, ());
        let instance = linker.instantiate(&mut store, module).expect("instantiate");
        EntrypointInstance::new(instance, store)
    }

    fn write_state(entry: &mut EntrypointInstance<()>, state: &State) {
        let (mut store, instance) = entry.as_store_and_instance_mut();
        instance
            .get_func(&mut store, "set_i32")
            .unwrap()
            .call(&mut store, &[wasmtime::Val::I32(state.i32_val)], &mut [])
            .unwrap();
        instance
            .get_func(&mut store, "set_i64")
            .unwrap()
            .call(&mut store, &[wasmtime::Val::I64(state.i64_val)], &mut [])
            .unwrap();
        instance
            .get_func(&mut store, "set_f32")
            .unwrap()
            .call(
                &mut store,
                &[wasmtime::Val::F32(state.f32_val.to_bits())],
                &mut [],
            )
            .unwrap();
        instance
            .get_func(&mut store, "set_f64")
            .unwrap()
            .call(
                &mut store,
                &[wasmtime::Val::F64(state.f64_val.to_bits())],
                &mut [],
            )
            .unwrap();
        let write_byte = instance.get_func(&mut store, "write_byte").unwrap();
        for (offset, byte) in state.bytes.iter().enumerate() {
            write_byte
                .call(
                    &mut store,
                    &[
                        wasmtime::Val::I32(offset as i32),
                        wasmtime::Val::I32(i32::from(*byte)),
                    ],
                    &mut [],
                )
                .unwrap();
        }
    }

    fn assert_state(entry: &mut EntrypointInstance<()>, expected: &State) {
        let (mut store, instance) = entry.as_store_and_instance_mut();

        let mut out = [wasmtime::Val::I32(0)];
        instance
            .get_func(&mut store, "get_i32")
            .unwrap()
            .call(&mut store, &[], &mut out)
            .unwrap();
        assert_eq!(out[0].i32(), Some(expected.i32_val));

        let mut out = [wasmtime::Val::I64(0)];
        instance
            .get_func(&mut store, "get_i64")
            .unwrap()
            .call(&mut store, &[], &mut out)
            .unwrap();
        assert_eq!(out[0].i64(), Some(expected.i64_val));

        let mut out = [wasmtime::Val::F32(0)];
        instance
            .get_func(&mut store, "get_f32")
            .unwrap()
            .call(&mut store, &[], &mut out)
            .unwrap();
        assert_eq!(
            out[0].f32().map(f32::to_bits),
            Some(expected.f32_val.to_bits())
        );

        let mut out = [wasmtime::Val::F64(0)];
        instance
            .get_func(&mut store, "get_f64")
            .unwrap()
            .call(&mut store, &[], &mut out)
            .unwrap();
        assert_eq!(
            out[0].f64().map(f64::to_bits),
            Some(expected.f64_val.to_bits())
        );

        let read_byte = instance.get_func(&mut store, "read_byte").unwrap();
        let mut out = [wasmtime::Val::I32(0)];
        for (offset, byte) in expected.bytes.iter().enumerate() {
            read_byte
                .call(&mut store, &[wasmtime::Val::I32(offset as i32)], &mut out)
                .unwrap();
            assert_eq!(out[0].i32(), Some(i32::from(*byte)));
        }
    }

    #[test]
    fn all_numeric_types_roundtrip() {
        let engine = Engine::default();
        let module = Module::new(&engine, wat::parse_str(WAT).unwrap()).unwrap();

        let bytes = {
            let mut instance = build_instance(&engine, &module);
            write_state(&mut instance, &STATE_A);
            let snapshot = instance.create_snapshot().expect("create_snapshot");
            bcs::to_bytes(&snapshot).expect("serialize")
        };

        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(&engine, &module);
        instance.restore_snapshot(&snapshot).expect("restore_snapshot");
        assert_state(&mut instance, &STATE_A);
    }

    #[test]
    fn two_snapshots_are_independent() {
        let engine = Engine::default();
        let module = Module::new(&engine, wat::parse_str(WAT).unwrap()).unwrap();

        let (bytes_a, bytes_b) = {
            let mut instance = build_instance(&engine, &module);
            write_state(&mut instance, &STATE_A);
            let snap_a = bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize a");
            write_state(&mut instance, &STATE_B);
            let snap_b = bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize b");
            (snap_a, snap_b)
        };

        let snap_a: WasmInstanceSnapshot = bcs::from_bytes(&bytes_a).expect("deserialize a");
        let mut instance_a = build_instance(&engine, &module);
        instance_a.restore_snapshot(&snap_a).expect("restore_snapshot");
        assert_state(&mut instance_a, &STATE_A);

        let snap_b: WasmInstanceSnapshot = bcs::from_bytes(&bytes_b).expect("deserialize b");
        let mut instance_b = build_instance(&engine, &module);
        instance_b.restore_snapshot(&snap_b).expect("restore_snapshot");
        assert_state(&mut instance_b, &STATE_B);
    }

    #[test]
    fn restore_grows_memory_when_snapshot_is_larger() {
        let engine = Engine::default();
        let module = Module::new(&engine, wat::parse_str(super::WAT).unwrap()).unwrap();

        let bytes = {
            let mut instance = build_instance(&engine, &module);
            let (mut store, wasmtime_instance) = instance.as_store_and_instance_mut();
            let mut grow_out = [wasmtime::Val::I32(0)];
            wasmtime_instance
                .get_func(&mut store, "grow_pages")
                .unwrap()
                .call(&mut store, &[wasmtime::Val::I32(1)], &mut grow_out)
                .unwrap();
            wasmtime_instance
                .get_func(&mut store, "write_byte")
                .unwrap()
                .call(
                    &mut store,
                    &[
                        wasmtime::Val::I32(super::SECOND_PAGE_OFFSET),
                        wasmtime::Val::I32(0xa5),
                    ],
                    &mut [],
                )
                .unwrap();
            bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize")
        };

        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(&engine, &module);
        instance.restore_snapshot(&snapshot).expect("restore_snapshot");

        let (mut store, wasmtime_instance) = instance.as_store_and_instance_mut();
        let mut out = [wasmtime::Val::I32(0)];
        wasmtime_instance
            .get_func(&mut store, "read_byte")
            .unwrap()
            .call(
                &mut store,
                &[wasmtime::Val::I32(super::SECOND_PAGE_OFFSET)],
                &mut out,
            )
            .unwrap();
        assert_eq!(out[0].i32(), Some(0xa5));
    }

    #[test]
    fn restore_errors_when_table_size_changed() {
        use linera_witty::SnapshotError;

        let engine = Engine::default();
        let module = Module::new(&engine, wat::parse_str(super::WAT).unwrap()).unwrap();

        // Source instance grows the table, then snapshots.
        let bytes = {
            let mut instance = build_instance(&engine, &module);
            let (mut store, wasmtime_instance) = instance.as_store_and_instance_mut();
            let mut grow_out = [wasmtime::Val::I32(0)];
            wasmtime_instance
                .get_func(&mut store, "grow_table")
                .unwrap()
                .call(
                    &mut store,
                    &[wasmtime::Val::I32(3)],
                    &mut grow_out,
                )
                .unwrap();
            bcs::to_bytes(&instance.create_snapshot().expect("create_snapshot")).expect("serialize")
        };

        // Restore on a fresh instance: table size differs, must return an error.
        let snapshot: WasmInstanceSnapshot = bcs::from_bytes(&bytes).expect("deserialize");
        let mut instance = build_instance(&engine, &module);
        let err = instance.restore_snapshot(&snapshot).expect_err("expected error");
        assert!(
            matches!(err, SnapshotError::TableSizeMismatch { .. }),
            "unexpected error: {err}",
        );
    }
}
