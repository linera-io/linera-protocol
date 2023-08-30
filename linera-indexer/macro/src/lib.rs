// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the macro to build indexer plugins.

extern crate proc_macro;
use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse_macro_input, parse_quote,
    visit_mut::{self, VisitMut},
    Expr, Ident, ImplItem, ImplItemFn, ItemImpl, ItemStruct, Stmt, Type, Visibility,
};

struct SelfReplace;

impl VisitMut for SelfReplace {
    fn visit_ident_mut(&mut self, id: &mut Ident) {
        if &id.to_string() == "self" {
            *id = format_ident!("_plugin");
        } else {
            visit_mut::visit_ident_mut(self, id);
        }
    }
}

fn get_impl_name(implem: &ItemImpl) -> Ident {
    let Type::Path(path) = *implem.self_ty.clone() else {
        panic!("unexpected impl type")
    };
    path.path.segments[0].ident.clone()
}

fn plugin_struct(name: &Ident, plugin_name: &Ident) -> ItemStruct {
    parse_quote! {
        #[derive(Clone)]
        pub struct #plugin_name<C>(std::sync::Arc<tokio::sync::Mutex<#name<C>>>);
    }
}

fn handler_impl(plugin_name: &Ident) -> ItemImpl {
    parse_quote! {
        impl<C> #plugin_name<C>
        where
            C: linera_views::common::Context + Send + Sync + 'static + Clone,
            linera_views::views::ViewError: From<C::Error>,
        {
            async fn handler(
                schema: axum::extract::Extension<
                    async_graphql::Schema<
                    Self,
                async_graphql::EmptyMutation,
                async_graphql::EmptySubscription,
                >,
                >,
                req: async_graphql_axum::GraphQLRequest,
            ) -> async_graphql_axum::GraphQLResponse {
                schema.execute(req.into_inner()).await.into()
            }
        }
    }
}

fn helper_impl(name: &Ident, functions: &[ImplItem]) -> proc_macro2::TokenStream {
    if functions.is_empty() {
        quote! {}
    } else {
        let mut implem: ItemImpl = parse_quote! {
            impl<C> #name<C>
            where
                C: linera_views::common::Context + Send + Sync + 'static + Clone,
                linera_views::views::ViewError: From<C::Error>,
            {}
        };
        implem.items = functions.to_vec();
        implem.to_token_stream()
    }
}

fn wrap_fn(reg: &mut ImplItemFn) {
    let mut block = reg.block.clone();
    SelfReplace.visit_block_mut(&mut block);
    let stmts: &mut Vec<Stmt> = &mut block.stmts;
    let stmt: Stmt = parse_quote! {
        let _plugin = self.0.lock().await;
    };
    let mut stmts_new = vec![stmt];
    stmts_new.append(stmts);
    reg.block.stmts = stmts_new
}

fn wrap_register(reg: &mut ImplItemFn) {
    let mut block = reg.block.clone();
    SelfReplace.visit_block_mut(&mut block);
    let stmts: &mut Vec<Stmt> = &mut block.stmts;
    let stmt: Stmt = parse_quote! {
        let mut _plugin = self.0.lock().await;
    };
    let mut stmts_new = vec![stmt];
    stmts_new.append(stmts);

    if let Some(stmt) = stmts_new.pop() {
        match stmt.clone() {
            Stmt::Expr(Expr::Call(call), None) => match *call.func.clone() {
                Expr::Path(path) => {
                    if &path.path.segments[0].ident.to_string() == "Ok" {
                        let expr = call.args[0].clone();
                        let stmt: Stmt = parse_quote! {
                            #expr;
                        };
                        stmts_new.push(stmt)
                    } else {
                        stmts_new.push(stmt)
                    }
                }
                _ => stmts_new.push(stmt),
            },
            _ => stmts_new.push(stmt),
        }
    }
    let stmt: Stmt = parse_quote! {
        return Ok(_plugin.save().await?);
    };
    stmts_new.push(stmt);
    reg.block.stmts = stmts_new
}

fn find_register(items: &[ImplItem]) -> ImplItemFn {
    let mut register_function = items
        .iter()
        .find_map(|item| {
            let ImplItem::Fn(fun_item) = item else {
                return None;
            };
            if &fun_item.sig.ident.to_string() == "register" {
                Some(fun_item.clone())
            } else {
                None
            }
        })
        .expect("register function not found");
    wrap_register(&mut register_function);
    register_function
}

fn public_functions(items: &[ImplItem]) -> Vec<ImplItem> {
    items
        .iter()
        .filter_map(|item| {
            let ImplItem::Fn(mut fun_item) = item.clone() else {
                return None;
            };
            match (
                fun_item.sig.ident.to_string().as_str(),
                fun_item.vis.clone(),
            ) {
                ("register", _) => None,
                (_, Visibility::Public(_)) => {
                    wrap_fn(&mut fun_item);
                    Some(ImplItem::Fn(fun_item))
                }
                _ => None,
            }
        })
        .collect()
}

fn private_functions(items: &[ImplItem]) -> Vec<ImplItem> {
    items
        .iter()
        .filter_map(|item| {
            let ImplItem::Fn(fun_item) = item.clone() else {
                return None;
            };
            match (
                fun_item.sig.ident.to_string().as_str(),
                fun_item.vis.clone(),
            ) {
                ("register", _) => None,
                (_, Visibility::Inherited) => Some(ImplItem::Fn(fun_item)),
                _ => None,
            }
        })
        .collect()
}

fn plugin_impl(
    name: &Ident,
    name_string: &str,
    plugin_name: &Ident,
    register_fn: &ImplItemFn,
) -> ItemImpl {
    parse_quote! {
        #[async_trait::async_trait]
        impl<DB> linera_indexer::plugin::Plugin<DB> for #plugin_name<linera_views::common::ContextFromDb<(), DB>>
        where
            DB: linera_views::common::KeyValueStoreClient + Clone + Send + Sync + 'static,
            DB::Error: From<bcs::Error>
            + From<linera_views::value_splitting::DatabaseConsistencyError>
            + Send
            + Sync
            + std::error::Error
            + 'static,

            linera_views::views::ViewError: From<DB::Error>,
        {
            #register_fn

            async fn from_context(
                context: linera_views::common::ContextFromDb<(), DB>,
            ) -> Result<Self, linera_indexer::common::IndexerError> {
                let plugin = #name::load(context).await?;
                Ok(#plugin_name(std::sync::Arc::new(tokio::sync::Mutex::new(plugin))))
            }

            fn sdl(&self) -> String {
                async_graphql::Schema::new(self.clone(), async_graphql::EmptyMutation, async_graphql::EmptySubscription).sdl()
            }

            fn static_name() -> String {
                #name_string.to_string()
            }

            fn route(&self, app: axum::Router) -> axum::Router {
                app.route(
                    &format!("/{}", Self::static_name()),
                    axum::routing::get(linera_indexer::common::graphiql).post(Self::handler),
                )
                    .layer(axum::extract::Extension(async_graphql::Schema::new(self.clone(), async_graphql::EmptyMutation, async_graphql::EmptySubscription)))
            }
        }
    }
}

fn query_impl(plugin_name: &Ident, queries: &[ImplItem]) -> proc_macro2::TokenStream {
    if queries.is_empty() {
        quote! {}
    } else {
        let mut implem: ItemImpl = parse_quote! {
            #[async_graphql::Object]
            impl<C> #plugin_name<C>
            where
                C: linera_views::common::Context + Send + Sync + 'static + Clone,
                linera_views::views::ViewError: From<C::Error>,
            {}
        };
        implem.items = queries.to_vec();
        implem.to_token_stream()
    }
}

#[proc_macro_attribute]
pub fn plugin(_args: TokenStream, input: TokenStream) -> TokenStream {
    let impl_ori = parse_macro_input!(input as ItemImpl);

    let name = get_impl_name(&impl_ori);
    let register_fn = find_register(&impl_ori.items);
    let queries = public_functions(&impl_ori.items);
    let functions = private_functions(&impl_ori.items);
    let plugin_name = format_ident!("{}Plugin", name);
    let name_string = name.to_string().to_case(Case::Snake);

    let plugin_struct = plugin_struct(&name, &plugin_name);
    let handler_impl = handler_impl(&plugin_name);
    let helper_impl = helper_impl(&name, &functions);
    let plugin_impl = plugin_impl(&name, &name_string, &plugin_name, &register_fn);
    let query_impl = query_impl(&plugin_name, &queries);
    quote! {
        #plugin_struct
        #helper_impl
        #handler_impl
        #plugin_impl
        #query_impl
    }
    .into()
}
