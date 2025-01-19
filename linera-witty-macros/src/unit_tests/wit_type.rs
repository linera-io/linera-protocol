// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `WitType` derive macro.

#![cfg(test)]

use proc_macro2::Span;
use quote::quote;
use syn::{parse_quote, Fields, ItemEnum, ItemStruct, LitStr};

use super::{derive_for_enum, derive_for_struct, discover_wit_name};

/// Checks the generated code for the body of the implementation of `WitType` for a unit struct.
#[test]
fn zero_sized_type() {
    let input = Fields::Unit;
    let output = derive_for_struct(LitStr::new("zero-sized-type", Span::call_site()), &input);

    let expected = quote! {
        const SIZE: u32 = <linera_witty::HList![] as linera_witty::WitType>::SIZE;

        type Layout = <linera_witty::HList![] as linera_witty::WitType>::Layout;
        type Dependencies = linera_witty::HList![];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "zero-sized-type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration =
                String::from(concat!("    record " , "zero-sized-type" , " {\n"));
            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for a named struct.
#[test]
fn named_struct() {
    let input: ItemStruct = parse_quote! {
        struct Type {
            first: u8,
            second: CustomType,
        }
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_struct(wit_name, &input.fields);

    let expected = quote! {
        const SIZE: u32 = <linera_witty::HList![u8, CustomType] as linera_witty::WitType>::SIZE;

        type Layout = <linera_witty::HList![u8, CustomType] as linera_witty::WitType>::Layout;
        type Dependencies = linera_witty::HList![u8, CustomType];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(concat!("    record " , "type" , " {\n"));

            wit_declaration.push_str("        ");
            wit_declaration.push_str("first");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<u8 as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("second");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<CustomType as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for a tuple struct.
#[test]
fn tuple_struct() {
    let input: ItemStruct = parse_quote! {
        struct Type(String, Vec<CustomType>, i64);
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_struct(wit_name, &input.fields);

    let expected = quote! {
        const SIZE: u32 =
            <linera_witty::HList![String, Vec<CustomType>, i64] as linera_witty::WitType>::SIZE;

        type Layout =
            <linera_witty::HList![String, Vec<CustomType>, i64] as linera_witty::WitType>::Layout;

        type Dependencies = linera_witty::HList![String, Vec<CustomType>, i64];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(concat!("    record " , "type" , " {\n"));

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner0");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<String as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner1");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<Vec<CustomType> as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner2");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<i64 as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for an enum.
#[test]
fn enum_type() {
    let input: ItemEnum = parse_quote! {
        enum Enum {
            Empty,
            Tuple(i8, CustomType),
            Struct {
                first: (),
                second: String,
            },
        }
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_enum(&input.ident, wit_name, input.variants.iter());

    let expected = quote! {
        const SIZE: u32 = {
            let discriminant_size = std::mem::size_of::<u8>() as u32;
            let mut size = discriminant_size;
            let mut variants_alignment = <
                < <linera_witty::HList![] as linera_witty::WitType>::Layout as linera_witty::Merge<
                    < <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::Layout
                    as linera_witty::Merge<
                        <linera_witty::HList![(), String] as linera_witty::WitType>::Layout
                    >>::Output
                >>::Output
            as linera_witty::Layout>::ALIGNMENT;
            let padding = (-(size as i32) & (variants_alignment as i32 - 1)) as u32;

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![(), String] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let end_padding = (-(size as i32) & (variants_alignment as i32 - 1)) as u32;
            size + end_padding
        };

        type Layout = linera_witty::HCons<u8,
            < <linera_witty::HList![] as linera_witty::WitType>::Layout as linera_witty::Merge<
                < <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::Layout
                as linera_witty::Merge<
                    <linera_witty::HList![(), String] as linera_witty::WitType>::Layout
                >>::Output
            >>::Output>;

        type Dependencies = linera_witty::HList![i8, CustomType, (), String];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "enum".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(
                concat!("    ", "variant", " ", "enum" , " {\n"),
            );

            wit_declaration.push_str("        ");
            wit_declaration.push_str("empty");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("tuple");
            wit_declaration.push_str("(tuple<");
            wit_declaration.push_str(
                &<i8 as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(", ");
            wit_declaration.push_str(
                &<CustomType as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(">)");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("struct");
            wit_declaration.push_str("(tuple<");
            wit_declaration.push_str(
                &<() as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(", ");
            wit_declaration.push_str(
                &<String as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(">)");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for a named struct
/// with some ignored fields.
#[test]
fn named_struct_with_skipped_fields() {
    let input: ItemStruct = parse_quote! {
        struct Type {
            #[witty(skip)]
            ignored1: u8,
            first: u8,
            #[witty(skip)]
            ignored2: i128,
            #[witty(skip)]
            ignored3: String,
            second: CustomType,
            #[witty(skip)]
            ignored4: Vec<()>,
        }
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_struct(wit_name, &input.fields);

    let expected = quote! {
        const SIZE: u32 = <linera_witty::HList![u8, CustomType] as linera_witty::WitType>::SIZE;

        type Layout = <linera_witty::HList![u8, CustomType] as linera_witty::WitType>::Layout;
        type Dependencies = linera_witty::HList![u8, CustomType];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(concat!("    record " , "type" , " {\n"));

            wit_declaration.push_str("        ");
            wit_declaration.push_str("first");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<u8 as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("second");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<CustomType as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for a tuple struct
/// with some ignored fields.
#[test]
fn tuple_struct_with_skipped_fields() {
    let input: ItemStruct = parse_quote! {
        struct Type(
            #[witty(skip)]
            PhantomData<T>,
            String,
            Vec<CustomType>,
            #[witty(skip)]
            bool,
            i64,
        );
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_struct(wit_name, &input.fields);

    let expected = quote! {
        const SIZE: u32 =
            <linera_witty::HList![String, Vec<CustomType>, i64] as linera_witty::WitType>::SIZE;

        type Layout =
            <linera_witty::HList![String, Vec<CustomType>, i64] as linera_witty::WitType>::Layout;

        type Dependencies = linera_witty::HList![String, Vec<CustomType>, i64];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(concat!("    record " , "type" , " {\n"));

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner1");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<String as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner2");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<Vec<CustomType> as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner4");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<i64 as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for an enum
/// with some ignored fields.
#[test]
fn enum_type_with_skipped_fields() {
    let input: ItemEnum = parse_quote! {
        enum Enum {
            Empty,
            Tuple(i8, CustomType, #[witty(skip)] u128),
            Struct {
                first: (),
                #[witty(skip)]
                ignored1: (u8, u16),
                #[witty(skip)]
                ignored2: String,
                second: String,
                #[witty(skip)]
                ignored3: Option<String>,
            },
        }
    };
    let wit_name = discover_wit_name(&[], &input.ident);
    let output = derive_for_enum(&input.ident, wit_name, input.variants.iter());

    let expected = quote! {
        const SIZE: u32 = {
            let discriminant_size = std::mem::size_of::<u8>() as u32;
            let mut size = discriminant_size;
            let mut variants_alignment = <
                < <linera_witty::HList![] as linera_witty::WitType>::Layout as linera_witty::Merge<
                    < <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::Layout
                    as linera_witty::Merge<
                        <linera_witty::HList![(), String] as linera_witty::WitType>::Layout
                    >>::Output
                >>::Output
            as linera_witty::Layout>::ALIGNMENT;
            let padding = (-(size as i32) & (variants_alignment as i32 - 1)) as u32;

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let variant_size = discriminant_size
                + padding
                + <linera_witty::HList![(), String] as linera_witty::WitType>::SIZE;

            if variant_size > size {
                size = variant_size;
            }

            let end_padding = (-(size as i32) & (variants_alignment as i32 - 1)) as u32;
            size + end_padding
        };

        type Layout = linera_witty::HCons<u8,
            < <linera_witty::HList![] as linera_witty::WitType>::Layout as linera_witty::Merge<
                < <linera_witty::HList![i8, CustomType] as linera_witty::WitType>::Layout
                as linera_witty::Merge<
                    <linera_witty::HList![(), String] as linera_witty::WitType>::Layout
                >>::Output
            >>::Output>;

        type Dependencies = linera_witty::HList![i8, CustomType, (), String];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "enum".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration = String::from(
                concat!("    ", "variant", " ", "enum" , " {\n"),
            );

            wit_declaration.push_str("        ");
            wit_declaration.push_str("empty");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("tuple");
            wit_declaration.push_str("(tuple<");
            wit_declaration.push_str(
                &<i8 as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(", ");
            wit_declaration.push_str(
                &<CustomType as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(">)");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("        ");
            wit_declaration.push_str("struct");
            wit_declaration.push_str("(tuple<");
            wit_declaration.push_str(
                &<() as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(", ");
            wit_declaration.push_str(
                &<String as linera_witty::WitType>::wit_type_name(),
            );
            wit_declaration.push_str(">)");
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}

/// Checks the generated code for the body of the implementation of `WitType` for a struct
/// with a custom WIT type name.
#[test]
fn struct_with_a_custom_wit_name() {
    let input: ItemStruct = parse_quote! {
        #[witty(name = "renamed-type")]
        struct Type(i16);
    };
    let wit_name = discover_wit_name(&input.attrs, &input.ident);
    let output = derive_for_struct(wit_name, &input.fields);

    let expected = quote! {
        const SIZE: u32 = <linera_witty::HList![i16] as linera_witty::WitType>::SIZE;

        type Layout = <linera_witty::HList![i16] as linera_witty::WitType>::Layout;
        type Dependencies = linera_witty::HList![i16];

        fn wit_type_name() -> std::borrow::Cow<'static, str> {
            "renamed-type".into()
        }

        fn wit_type_declaration() -> std::borrow::Cow<'static, str> {
            let mut wit_declaration =
                String::from(concat!("    record " , "renamed-type" , " {\n"));

            wit_declaration.push_str("        ");
            wit_declaration.push_str("inner0");
            wit_declaration.push_str(": ");
            wit_declaration.push_str(&*<i16 as linera_witty::WitType>::wit_type_name());
            wit_declaration.push_str(",\n");

            wit_declaration.push_str("    }\n");
            wit_declaration.into ()
        }
    };

    assert_eq!(output.to_string(), expected.to_string());
}
