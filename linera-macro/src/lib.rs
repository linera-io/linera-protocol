extern crate proc_macro;
extern crate syn;
use std::str::FromStr;
use proc_macro2::Span;
use syn::{parse_macro_input, ItemStruct};
use proc_macro::{TokenStream,TokenTree};
use quote::quote;

fn get_title_strings_kernel(item_group: TokenStream) -> Vec<(String, String)> {
    let mut pos = 0;
    let mut list_double_dot = Vec::new();
    let item_vect: Vec<TokenTree> = item_group
        .into_iter()
        .map(|x| {
            if let TokenTree::Punct(punct) = x.clone() {
                if punct.as_char() == ':' {
                    list_double_dot.push(pos);
                }
            }
            pos += 1;
            x
        })
        .collect();
    let mut title_type_vect = Vec::new();
    for pos_double_dot in list_double_dot {
        let title_str = format!("{}", item_vect[pos_double_dot - 1]);
        let e_type = format!("{}", item_vect[pos_double_dot + 1]);
        title_type_vect.push((title_str.clone(), e_type.clone()));
    }
    title_type_vect
}

fn is_doc(input: TokenStream) -> bool {
    if let Some(x) = input.into_iter().next() {
        let x_str = format!("{}", x);
        if x_str == "doc" {
            return true;
        }
        return false;
    }
    false
}

fn get_title_strings(input: TokenStream) -> Option<Vec<(String, String)>> {
    for x in input {
        match x {
            TokenTree::Group(group) => {
                let stream = group.stream();
                if !is_doc(stream.clone()) {
                    return Some(get_title_strings_kernel(stream));
                }
            }
            _v => {}
        };
    }
    None
}

fn read_preamble(input: TokenStream) -> Option<(String, Vec<String>)> {
    let mut level = 0;
    let mut name = None;
    let mut template_vect = Vec::new();
    let mut has_group = false;
    let mut pos_struct = None;
    for (idx, x) in input.into_iter().enumerate() {
        match x {
            TokenTree::Group(_group) => {
                has_group = true;
            }
            x => {
                let x_str = format!("{}", x);
                if x_str == "struct" {
                    pos_struct = Some(idx);
                }
                if let Some(pos_struct) = pos_struct {
                    if idx == pos_struct + 1 {
                        name = Some(x_str.clone());
                    }
                }
                if x_str == *"<" {
                    level += 1;
                }
                if level == 1 && x_str != *"," && x_str != *"<" && x_str != *">" {
                    template_vect.push(x_str.clone());
                }
                if x_str == *">" {
                    level -= 1;
                }
            }
        }
    }
    if !has_group || level != 0 {
        return None;
    }
    name.map(|name| (name, template_vect))
}

fn get_header(trait_name: String, input: TokenStream) -> String {
    let pair_preamble = read_preamble(input).expect("read_preamble parsing error");
    let struct_name = pair_preamble.0;
    let template_vect = pair_preamble.1;
    let first_template = template_vect.get(0).expect("The list of template is empty");
    let mut seq_template = first_template.clone();
    for i in 1..template_vect.len() {
        seq_template += &(", ".to_owned() + template_vect.get(i).unwrap());
    }
    let mut header = "#[async_trait]\n".to_string();
    header += &format!(
        "impl<{}> {}<{}> for {}<{}>\n",
        seq_template, trait_name, first_template, struct_name, seq_template
    );
    header += "where\n";
    header += &format!(
        "    {}: Context + Send + Sync + Clone + 'static,\n",
        first_template
    );
    header += &format!("    ViewError: From<{}::Error>,\n", first_template);
    header += "{\n";
    header
}

fn generate_view_code(
    title_type_vect: Vec<(String, String)>,
    input: TokenStream,
) -> TokenStream {
    let title_first = title_type_vect[0].clone().0;
    let pair_preamble = read_preamble(input.clone()).expect("read_preamble parsing error");
    let template_vect = pair_preamble.1;
    let first_template = template_vect.get(0).expect("The list of template is empty");

    let mut context_code_str = format!("  fn context(&self) -> &{}", first_template) + " {\n";
    context_code_str += &format!("    self.{}.context()\n", title_first);
    context_code_str += "  }\n";
    //
    let mut load_code_str = format!(
        "  async fn load(context: {}) -> Result<Self, ViewError>",
        first_template
    ) + " {\n";
    let mut return_code_str = "    Ok(Self {".to_string();
    for (i_title, title_type) in title_type_vect.iter().enumerate() {
        let otype = title_type.1.clone();
        let str0 = format!("    let index{} : u64 = {};", i_title, i_title);
        let str1 = format!(
            "    let base_key{} : Vec<u8> = context.derive_key(&index{})?;",
            i_title, i_title
        );
        let str2 = format!(
            "    let {} = {}::load(context.clone_with_base_key(base_key{})).await?;",
            title_type.0, otype, i_title
        );
        load_code_str += &(str0 + "\n");
        load_code_str += &(str1 + "\n");
        load_code_str += &(str2 + "\n");
        if i_title > 0 {
            return_code_str += ",";
        }
        return_code_str += &format!(" {}", title_type.0);
    }
    return_code_str += " })";
    load_code_str += &(return_code_str + "\n");
    load_code_str += "  }\n";
    //
    let mut rollback_code_str = "  fn rollback(&mut self) {\n".to_string();
    for title_type in title_type_vect.clone() {
        let str0 = format!("    self.{}.rollback();", title_type.0);
        rollback_code_str += &(str0 + "\n");
    }
    rollback_code_str += "  }\n";
    //
    let mut flush_code_str =
        "  fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {\n".to_string();
    for title_type in title_type_vect.clone() {
        let str0 = format!("    self.{}.flush(batch)?;", title_type.0);
        flush_code_str += &(str0 + "\n");
    }
    flush_code_str += "    Ok(())\n  }\n";
    //
    let mut delete_code_str = "  fn delete(self, batch: &mut Batch) {\n".to_string();
    for title_type in title_type_vect.clone() {
        let str0 = format!("    self.{}.delete(batch);", title_type.0);
        delete_code_str += &(str0 + "\n");
    }
    delete_code_str += "  }\n";
    //
    let mut clear_code_str = "  fn clear(&mut self) {\n".to_string();
    for title_type in title_type_vect {
        let str0 = format!("    self.{}.clear();", title_type.0);
        clear_code_str += &(str0 + "\n");
    }
    clear_code_str += "  }\n";
    //
    let header = get_header("View".to_string(), input);
    let gen_code = header
        + &context_code_str
        + &load_code_str
        + &rollback_code_str
        + &flush_code_str
        + &delete_code_str
        + &clear_code_str
        + "}\n";
    println!("gen_code={}", gen_code);

    TokenStream::from_str(&gen_code).expect("Failure in constructing the View code")
}

fn generate_container_view_code(
    title_type_vect: Vec<(String, String)>,
    input: TokenStream,
) -> TokenStream {
    let mut save_code_str = "  async fn save(&mut self) -> Result<(), ViewError> {\n".to_string();
    save_code_str += "    let mut batch = Batch::default();\n";
    for title_type in title_type_vect.clone() {
        let str0 = format!("    self.{}.flush(&mut batch)?;", title_type.0);
        save_code_str += &(str0 + "\n");
    }
    save_code_str += "    self.context().write_batch(batch).await?;\n";
    save_code_str += "    Ok(())\n  }\n";

    let mut delete_code_str =
        "  async fn write_delete(self) -> Result<(), ViewError> {\n".to_string();
    //    delete_code_str += "use $crate::views::View;\n";
    //    delete_code_str += "use $crate::common::Batch;\n";
    delete_code_str += "    let context = self.context().clone();\n";
    delete_code_str += "    let batch = Batch::build(move |batch| {\n";
    delete_code_str += "      Box::pin(async move {\n";
    for title_type in title_type_vect {
        delete_code_str += &format!("        self.{}.delete(batch);\n", title_type.0);
    }
    delete_code_str += "        Ok(())\n";
    delete_code_str += "      })\n";
    delete_code_str += "    }).await?;\n";
    delete_code_str += "    context.write_batch(batch).await?;\n";
    delete_code_str += "    Ok(())\n";
    delete_code_str += "  }\n";

    let header = get_header("ContainerView".to_string(), input);
    let gen_code = header + &save_code_str + &delete_code_str + "}\n";
    println!("gen_code={}", gen_code);

    TokenStream::from_str(&gen_code)
        .expect("Failure in constructing the ContainerView code")
}

fn generate_hash_view_code(
    title_type_vect: Vec<(String, String)>,
    input: TokenStream,
) -> TokenStream {
    let hasher_code_str = "  type Hasher = sha2::Sha512;\n";

    let mut hash_code_str =
        "  async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {\n"
            .to_string();
    hash_code_str += "    use views::{Hasher, HashView};\n";
    hash_code_str += "    use std::io::Write;\n";
    hash_code_str += "    let mut hasher = Self::Hasher::default();\n";
    for title_type in title_type_vect {
        let str0 = format!(
            "    hasher.write_all(self.{}.hash().await?.as_ref())?;",
            title_type.0
        );
        hash_code_str += &(str0 + "\n");
    }
    hash_code_str += "    Ok(hasher.finalize())\n  }\n";

    let header = get_header("HashView".to_string(), input);
    let gen_code = header + hasher_code_str + &hash_code_str + "}\n";
    println!("gen_code={}", gen_code);

    TokenStream::from_str(&gen_code)
        .expect("Failure in constructing the ContainerView code")
}

fn get_seq_parameter(generics: syn::Generics) -> Vec<syn::Ident> {
    let mut generic_vect = Vec::new();
    for param in generics.params {
        if let syn::GenericParam::Type(param) = param {
            generic_vect.push(param.ident);
        }
    }
    generic_vect
}

fn generate_hash_func_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = input.ident;
    let generics = input.generics;
    println!("struct_name={}", struct_name);
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect.get(0).expect("failed to find the first generic parameter");
    println!("first_generic={}", first_generic);

    let hash_type = syn::Ident::new(&format!("{}Hash", struct_name), Span::call_site());
    let quote_o = quote!{
        #[async_trait]
        impl #generics HashFunc<#first_generic> for #struct_name #generics
        where
            #first_generic: Context + Send + Sync + Clone + 'static,
            ViewError: From<#first_generic::Error>,
        {
            async fn hash_value(&mut self) -> Result<HashValue, ViewError> {
                use crypto::{BcsSignable, HashValue};
                use generic_array::GenericArray;
                use views::HashView;
                use serde::{Serialize, Deserialize};
                use sha2::{Sha512, Digest};
                #[derive(Serialize, Deserialize)]
                struct #hash_type(GenericArray<u8, <Sha512 as Digest>::OutputSize>);
                impl BcsSignable for #hash_type {}
                let hash = self.hash().await?;
                Ok(HashValue::new(&#hash_type(hash)))
            }
        }
    };
    println!("quote_o={}", quote_o);
    TokenStream::from(quote_o)
}

#[proc_macro_derive(View)]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let title_type_vect = get_title_strings(input.clone()).expect("derive_view failed");
    generate_view_code(title_type_vect, input)
}

#[proc_macro_derive(ContainerView)]
pub fn derive_container_view(input: TokenStream) -> TokenStream {
    let title_type_vect = get_title_strings(input.clone()).expect("derive_view failed");
    generate_container_view_code(title_type_vect, input)
}

#[proc_macro_derive(HashView)]
pub fn derive_hash_view(input: TokenStream) -> TokenStream {
    let title_type_vect = get_title_strings(input.clone()).expect("derive_view failed");
    generate_hash_view_code(title_type_vect, input)
}

#[proc_macro_derive(HashFunc)]
pub fn derive_hash_func(input: TokenStream) -> TokenStream {
    generate_hash_func_code(input)
}
