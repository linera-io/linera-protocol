// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of WIT files.

use std::collections::BTreeMap;

use genawaiter::{rc::gen, yield_};

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
    pub fn generate_file_contents(&self) -> impl Iterator<Item = &[u8]> {
        gen!({
            yield_!(b"package ".as_slice());
            yield_!(self.package.as_bytes());
            yield_!(b";\n\n");

            yield_!(b"interface ");
            yield_!(self.name.as_bytes());
            yield_!(b" {\n");

            for function in &self.functions {
                yield_!(function.as_bytes());
                yield_!(b"\n");
            }

            for type_declaration in self.types.values() {
                if !type_declaration.is_empty() {
                    yield_!(b"\n");
                    yield_!(type_declaration.as_bytes());
                }
            }

            yield_!(b"}\n");
        })
        .into_iter()
    }
}
