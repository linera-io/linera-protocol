// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to import functions from a Wasm guest module.

use std::collections::HashSet;

use heck::ToKebabCase;
use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::{spanned::Spanned, FnArg, Ident, ItemTrait, LitStr, ReturnType, TraitItem, TraitItemFn};

use super::wit_interface;
use crate::util::{AttributeParameters, TokensSetItem};

/// Returns the code generated for calling imported Wasm functions.
///
/// The generated code contains a new generic type with the `trait_definition`'s name that allows
/// calling into the functions imported from a guest Wasm instance represented by a generic
/// parameter.
pub fn generate(trait_definition: ItemTrait, parameters: AttributeParameters) -> TokenStream {
    WitImportGenerator::new(&trait_definition, parameters).generate()
}

/// A helper type for generation of the importing of Wasm functions.
///
/// Code generating is done in two phases. First the necessary pieces are collected and stored in
/// this type. Then, they are used to generate the final code.
pub struct WitImportGenerator<'input> {
    parameters: AttributeParameters,
    trait_name: &'input Ident,
    namespace: LitStr,
    functions: Vec<FunctionInformation<'input>>,
}

/// Pieces of information extracted from a function's definition.
pub(crate) struct FunctionInformation<'input> {
    pub(crate) function: &'input TraitItemFn,
    parameter_definitions: TokenStream,
    parameter_bindings: TokenStream,
    return_type: TokenStream,
    interface: TokenStream,
    instance_constraint: TokenStream,
}

impl<'input> WitImportGenerator<'input> {
    /// Collects the pieces necessary for code generation from the inputs.
    fn new(trait_definition: &'input ItemTrait, parameters: AttributeParameters) -> Self {
        let trait_name = &trait_definition.ident;
        let namespace = parameters.namespace(trait_name);
        let functions = trait_definition
            .items
            .iter()
            .map(FunctionInformation::from)
            .collect::<Vec<_>>();

        WitImportGenerator {
            trait_name,
            parameters,
            namespace,
            functions,
        }
    }

    /// Consumes the collected pieces to generate the final code.
    fn generate(self) -> TokenStream {
        let function_slots = self.function_slots();
        let slot_initializations = self.slot_initializations();
        let imported_functions = self.imported_functions();
        let (instance_trait_alias_name, instance_trait_alias) = self.instance_trait_alias();

        let trait_name = self.trait_name;

        let wit_interface_implementation = wit_interface::generate(
            self.parameters.package_name(),
            self.parameters.interface_name(trait_name),
            &self.functions,
        );

        quote! {
            #[allow(clippy::type_complexity)]
            pub struct #trait_name<Instance>
            where
                Instance: #instance_trait_alias_name,
                <Instance::Runtime as linera_witty::Runtime>::Memory:
                    linera_witty::RuntimeMemory<Instance>,
            {
                instance: Instance,
                #( #function_slots ),*
            }

            impl<Instance> #trait_name<Instance>
            where
                Instance: #instance_trait_alias_name,
                <Instance::Runtime as linera_witty::Runtime>::Memory:
                    linera_witty::RuntimeMemory<Instance>,
            {
                pub fn new(instance: Instance) -> Self {
                    #trait_name {
                        instance,
                        #( #slot_initializations ),*
                    }
                }

                #( #imported_functions )*
            }

            impl<Instance> linera_witty::wit_generation::WitInterface for #trait_name<Instance>
            where
                Instance: #instance_trait_alias_name,
                <Instance::Runtime as linera_witty::Runtime>::Memory:
                    linera_witty::RuntimeMemory<Instance>,
            {
                #wit_interface_implementation
            }

            #instance_trait_alias
        }
    }

    /// Returns the function slots definitions.
    ///
    /// The function slots are `Option` types used to lazily store handles to the functions
    /// obtained from a Wasm guest instance.
    fn function_slots(&self) -> impl Iterator<Item = TokenStream> + '_ {
        self.functions.iter().map(|function| {
            let function_name = function.name();
            let instance_constraint = &function.instance_constraint;

            quote_spanned! { function.span() =>
                #function_name: Option<<Instance as #instance_constraint>::Function>
            }
        })
    }

    /// Returns the expressions to initialize the function slots.
    fn slot_initializations(&self) -> impl Iterator<Item = TokenStream> + '_ {
        self.functions.iter().map(|function| {
            let function_name = function.name();

            quote_spanned! { function.span() =>
                #function_name: None
            }
        })
    }

    /// Returns the code to import and call each function.
    fn imported_functions(&self) -> impl Iterator<Item = TokenStream> + '_ {
        self.functions.iter().map(|function| {
            let namespace = &self.namespace;

            let function_name = function.name();
            let function_wit_name = function_name.to_string().to_kebab_case();

            let instance = &function.instance_constraint;
            let parameters = &function.parameter_definitions;
            let parameter_bindings = &function.parameter_bindings;
            let return_type = &function.return_type;
            let interface = &function.interface;

            quote_spanned! { function.span() =>
                pub fn #function_name(
                    &mut self,
                    #parameters
                ) -> Result<#return_type, linera_witty::RuntimeError>  {
                    let function = match &self.#function_name {
                        Some(function) => function,
                        None => {
                            self.#function_name = Some(<Instance as #instance>::load_function(
                                &mut self.instance,
                                &format!("{}#{}", #namespace, #function_wit_name),
                            )?);

                            self.#function_name
                                .as_ref()
                                .expect("Function loaded into slot, but the slot remains empty")
                        }
                    };

                    let flat_parameters = #interface::lower_parameters(
                        linera_witty::hlist![#parameter_bindings],
                        &mut self.instance.memory()?,
                    )?;

                    let flat_results = self.instance.call(function, flat_parameters)?;

                    #[allow(clippy::let_unit_value)]
                    let result = #interface::lift_results(flat_results, &self.instance.memory()?)?;

                    Ok(result)
                }
            }
        })
    }

    /// Returns a trait alias for all the instance constraints necessary for the generated type.
    fn instance_trait_alias(&self) -> (Ident, TokenStream) {
        let name = format_ident!("InstanceFor{}", self.trait_name);
        let constraints = self.instance_constraints();

        let definition = quote! {
            pub trait #name : #constraints
            where
                <<Self as linera_witty::Instance>::Runtime as linera_witty::Runtime>::Memory:
                    linera_witty::RuntimeMemory<Self>,
            {}

            impl<AnyInstance> #name for AnyInstance
            where
                AnyInstance: #constraints,
                <AnyInstance::Runtime as linera_witty::Runtime>::Memory:
                    linera_witty::RuntimeMemory<AnyInstance>,
            {}
        };

        (name, definition)
    }

    /// Returns the instance constraints necessary for the generated type.
    fn instance_constraints(&self) -> TokenStream {
        let constraint_set: HashSet<_> = self
            .functions
            .iter()
            .map(|function| TokensSetItem::from(&function.instance_constraint))
            .collect();

        constraint_set.into_iter().fold(
            quote! { linera_witty::InstanceWithMemory },
            |list, item| quote! { #list + #item },
        )
    }
}

impl<'input> FunctionInformation<'input> {
    /// Extracts the necessary information from the `function` and stores it in a new
    /// [`FunctionInformation`] instance.
    pub fn new(function: &'input TraitItemFn) -> Self {
        let (parameter_definitions, parameter_bindings, parameter_types) =
            Self::parse_parameters(function.sig.inputs.iter());

        let return_type = match &function.sig.output {
            ReturnType::Default => quote_spanned! { function.sig.output.span() => () },
            ReturnType::Type(_, return_type) => return_type.to_token_stream(),
        };

        let interface = quote_spanned! { function.sig.span() =>
            <(linera_witty::HList![#parameter_types], #return_type)
                as linera_witty::ImportedFunctionInterface>
        };

        let instance_constraint = quote_spanned! { function.sig.span() =>
            linera_witty::InstanceWithFunction<
                #interface::GuestParameters,
                #interface::GuestResults,
            >
        };

        FunctionInformation {
            function,
            parameter_definitions,
            parameter_bindings,
            return_type,
            interface,
            instance_constraint,
        }
    }

    /// Parses a function's parameters and returns the pieces constructed from the parameters.
    ///
    /// Returns the parameter definitions (the name and type pairs), the parameter bindings (the
    /// names) and the parameter types.
    fn parse_parameters(
        function_inputs: impl Iterator<Item = &'input FnArg>,
    ) -> (TokenStream, TokenStream, TokenStream) {
        let parameters = function_inputs.map(|input| match input {
            FnArg::Typed(parameter) => parameter,
            FnArg::Receiver(receiver) => abort!(
                receiver.self_token,
                "Imported interfaces can not have `self` parameters"
            ),
        });

        let mut parameter_definitions = quote! {};
        let mut parameter_bindings = quote! {};
        let mut parameter_types = quote! {};

        for parameter in parameters {
            let parameter_binding = &parameter.pat;
            let parameter_type = &parameter.ty;

            parameter_definitions.extend(quote! { #parameter, });
            parameter_bindings.extend(quote! { #parameter_binding, });
            parameter_types.extend(quote! { #parameter_type, });
        }

        (parameter_definitions, parameter_bindings, parameter_types)
    }

    /// Returns the name of the function.
    pub fn name(&self) -> &Ident {
        &self.function.sig.ident
    }

    /// Returns the code span of the function.
    pub fn span(&self) -> Span {
        self.function.span()
    }
}

impl<'input> From<&'input TraitItem> for FunctionInformation<'input> {
    fn from(item: &'input TraitItem) -> Self {
        match item {
            TraitItem::Fn(function) => FunctionInformation::new(function),
            TraitItem::Const(const_item) => abort!(
                const_item.ident,
                "Const items are not supported in imported traits"
            ),
            TraitItem::Type(type_item) => abort!(
                type_item.ident,
                "Type items are not supported in imported traits"
            ),
            TraitItem::Macro(macro_item) => abort!(
                macro_item.mac.path,
                "Macro items are not supported in imported traits"
            ),
            _ => abort!(item, "Only function items are supported in imported traits"),
        }
    }
}
