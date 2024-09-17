// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of WIT files.

mod stub_instance;

use std::collections::BTreeMap;

use genawaiter::{rc::gen, yield_};

pub use self::stub_instance::StubInstance;
pub use crate::type_traits::RegisterWitTypes;

/// Generates WIT snippets for an interface.
pub trait WitInterface {
    /// The [`WitType`][`crate::WitType`]s that this interface uses.
    type Dependencies: RegisterWitTypes;

    /// The name of the package the interface belongs to.
    fn wit_package() -> &'static str;

    /// The name of the interface.
    fn wit_name() -> &'static str;

    /// The WIT definitions of each function in this interface.
    fn wit_functions() -> Vec<String>;
}

/// Helper type to write a [`WitInterface`] to a file.
#[derive(Clone, Debug)]
pub struct WitInterfaceWriter {
    package: &'static str,
    name: &'static str,
    types: BTreeMap<String, String>,
    functions: Vec<String>,
}

impl WitInterfaceWriter {
    /// Prepares a new [`WitInterfaceWriter`] to write the provided `Interface`.
    pub fn new<Interface>() -> Self
    where
        Interface: WitInterface,
    {
        let mut types = BTreeMap::new();

        Interface::Dependencies::register_wit_types(&mut types);

        WitInterfaceWriter {
            package: Interface::wit_package(),
            name: Interface::wit_name(),
            types,
            functions: Interface::wit_functions(),
        }
    }

    /// Returns an [`Iterator`] with the file contents of the WIT interface file.
    pub fn generate_file_contents(&self) -> impl Iterator<Item = &str> {
        gen!({
            yield_!("package ");
            yield_!(self.package);
            yield_!(";\n\n");

            yield_!("interface ");
            yield_!(self.name);
            yield_!(" {\n");

            for function in &self.functions {
                yield_!(&function);
                yield_!("\n");
            }

            for type_declaration in self.types.values() {
                if !type_declaration.is_empty() {
                    yield_!("\n");
                    yield_!(&type_declaration);
                }
            }

            yield_!("}\n");
        })
        .into_iter()
    }
}

/// Helper type to write a WIT file declaring a
/// [world](https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md#wit-worlds).
#[derive(Clone, Debug)]
pub struct WitWorldWriter {
    package: Option<&'static str>,
    name: String,
    imports: Vec<&'static str>,
    exports: Vec<&'static str>,
}

impl WitWorldWriter {
    /// Creates a new [`WitWorldWriter`] to write a world with the provided `name`.
    pub fn new(package: impl Into<Option<&'static str>>, name: impl Into<String>) -> Self {
        WitWorldWriter {
            package: package.into(),
            name: name.into(),
            imports: Vec::new(),
            exports: Vec::new(),
        }
    }

    /// Registers a [`WitInterface`] to be imported into this world.
    pub fn import<Interface>(mut self) -> Self
    where
        Interface: WitInterface,
    {
        self.imports.push(Interface::wit_name());
        self
    }

    /// Registers a [`WitInterface`] to be exported from this world.
    pub fn export<Interface>(mut self) -> Self
    where
        Interface: WitInterface,
    {
        self.exports.push(Interface::wit_name());
        self
    }

    /// Returns an [`Iterator`] with the file contents of the WIT world file, optionally including
    /// a package header.
    pub fn generate_file_contents(&self) -> impl Iterator<Item = &str> {
        gen!({
            if let Some(package) = &self.package {
                yield_!("package ");
                yield_!(package);
                yield_!(";\n\n");
            }

            yield_!("world ");
            yield_!(&self.name);
            yield_!(" {\n");

            for import in &self.imports {
                yield_!("    import ");
                yield_!(import);
                yield_!(";\n");
            }

            if !self.imports.is_empty() {
                yield_!("\n");
            }

            for export in &self.exports {
                yield_!("    export ");
                yield_!(export);
                yield_!(";\n");
            }

            yield_!("}\n");
        })
        .into_iter()
    }
}
