// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared code to test generated [`WitInterface`] implementations for the interfaces used
//! in the tests.

use linera_witty::{
    test::{assert_interface_dependencies, assert_interface_functions},
    wit_generation::WitInterface,
};

/// Expected snippets for the `entrypoint` interface.
// The `wit_import` integration test does not use an `Entrypoint` interface
#[allow(dead_code)]
pub const ENTRYPOINT: (&str, &[&str], &[(&str, &str)]) =
    ("entrypoint", &["    entrypoint: func();"], &[]);

/// Expected snippets for the `simple-function` interface.
pub const SIMPLE_FUNCTION: (&str, &[&str], &[(&str, &str)]) =
    ("simple-function", &["    simple: func();"], &[]);

/// Expected snippets for the `getters` interface.
pub const GETTERS: (&str, &[&str], &[(&str, &str)]) = (
    "getters",
    &[
        "    get-true: func() -> bool;",
        "    get-false: func() -> bool;",
        "    get-s8: func() -> s8;",
        "    get-u8: func() -> u8;",
        "    get-s16: func() -> s16;",
        "    get-u16: func() -> u16;",
        "    get-s32: func() -> s32;",
        "    get-u32: func() -> u32;",
        "    get-s64: func() -> s64;",
        "    get-u64: func() -> u64;",
        "    get-float32: func() -> float32;",
        "    get-float64: func() -> float64;",
    ],
    &[
        ("bool", ""),
        ("float32", ""),
        ("float64", ""),
        ("s16", ""),
        ("s32", ""),
        ("s64", ""),
        ("s8", ""),
        ("u16", ""),
        ("u32", ""),
        ("u64", ""),
        ("u8", ""),
    ],
);

/// Expected snippets for the `setters` interface.
pub const SETTERS: (&str, &[&str], &[(&str, &str)]) = (
    "setters",
    &[
        "    set-bool: func(value: bool);",
        "    set-s8: func(value: s8);",
        "    set-u8: func(value: u8);",
        "    set-s16: func(value: s16);",
        "    set-u16: func(value: u16);",
        "    set-s32: func(value: s32);",
        "    set-u32: func(value: u32);",
        "    set-s64: func(value: s64);",
        "    set-u64: func(value: u64);",
        "    set-float32: func(value: float32);",
        "    set-float64: func(value: float64);",
    ],
    &[
        ("bool", ""),
        ("float32", ""),
        ("float64", ""),
        ("s16", ""),
        ("s32", ""),
        ("s64", ""),
        ("s8", ""),
        ("u16", ""),
        ("u32", ""),
        ("u64", ""),
        ("u8", ""),
    ],
);

/// Expected snippets for the `operations` interface.
pub const OPERATIONS: (&str, &[&str], &[(&str, &str)]) = (
    "operations",
    &[
        "    and-bool: func(first: bool, second: bool) -> bool;",
        "    add-s8: func(first: s8, second: s8) -> s8;",
        "    add-u8: func(first: u8, second: u8) -> u8;",
        "    add-s16: func(first: s16, second: s16) -> s16;",
        "    add-u16: func(first: u16, second: u16) -> u16;",
        "    add-s32: func(first: s32, second: s32) -> s32;",
        "    add-u32: func(first: u32, second: u32) -> u32;",
        "    add-s64: func(first: s64, second: s64) -> s64;",
        "    add-u64: func(first: u64, second: u64) -> u64;",
        "    add-float32: func(first: float32, second: float32) -> float32;",
        "    add-float64: func(first: float64, second: float64) -> float64;",
    ],
    &[
        ("bool", ""),
        ("float32", ""),
        ("float64", ""),
        ("s16", ""),
        ("s32", ""),
        ("s64", ""),
        ("s8", ""),
        ("u16", ""),
        ("u32", ""),
        ("u64", ""),
        ("u8", ""),
    ],
);

/// Tests the [`WitInterface`] of a specified type, checking that its WIT snippets match
/// the `expected_snippets`.
pub fn test_wit_interface<Interface>(expected_snippets: (&str, &[&str], &[(&str, &str)]))
where
    Interface: WitInterface,
{
    let (expected_name, expected_functions, expected_dependencies) = expected_snippets;

    assert_eq!(Interface::wit_package(), "witty-macros:test-modules");
    assert_eq!(Interface::wit_name(), expected_name);
    assert_interface_functions::<Interface>(expected_functions);
    assert_interface_dependencies::<Interface>(expected_dependencies.iter().copied());
}
