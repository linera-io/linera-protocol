extern crate proc_macro;
use std::str::FromStr;


fn get_type(item_vect: Vec<proc_macro::TokenTree>) -> String {
    let mut e_type = "".to_string();
//    let mut level = 0;
    for item in item_vect {
        let item_str = format!("{}", item);
        return item_str;
    }
    e_type
}


fn get_title_strings_kernel(item_group: proc_macro::TokenStream) -> Vec<(String,String)> {
//    println!("get_title_strings_kernel iterm_group={:?}", item_group);
    let mut pos = 0;
    let mut list_double_dot = Vec::new();
    let l_item : Vec<proc_macro::TokenTree> = item_group.clone().into_iter().map(
        |x| {
            match x.clone() {
                proc_macro::TokenTree::Punct(punct) => {
                    if punct.as_char() == ':' {
                        list_double_dot.push(pos);
                    }
                },
                _ => (),
            }
            pos += 1;
            x
        }).collect();
    let mut l_title_type = Vec::new();
    let n_sep = list_double_dot.len();
    for i_sep in 0..n_sep {
        let pos_double_dot = list_double_dot[i_sep];
        let pos_first = pos_double_dot + 1;
        let pos_last = if i_sep == n_sep - 1 {
            l_item.len()
        } else {
            list_double_dot[i_sep+1] - 1
        } - 1;
        let title_str = format!("{}", l_item[pos_double_dot-1]);
        let e_type = get_type(l_item[pos_first..pos_last].to_vec());
        l_title_type.push((title_str.clone(), e_type.clone()));
    }
//    println!("returning l_title");
    l_title_type
}


fn is_doc(input: proc_macro::TokenStream) -> bool {
    for x in input.into_iter() {
        let x_str = format!("{}", x);
        if x_str == "doc" {
            return true;
        }
        return false;
    }
    return false;
}


fn get_title_strings(input: proc_macro::TokenStream) -> Option<Vec<(String,String)>> {
    for x in input {
        match x {
            proc_macro::TokenTree::Group(group) => {
                let stream = group.stream();
                if !is_doc(stream.clone()) {
                    return Some(get_title_strings_kernel(stream));
                }
            },
            _v => {},
        };
    }
    None
}

fn read_preamble(input: proc_macro::TokenStream) -> Option<(String,Vec<String>)> {
    let mut level = 0;
    let mut idx = 0;
    let mut name = None;
    let mut l_template = Vec::new();
    let mut has_group = false;
    let mut pos_struct = None;
    for x in input.into_iter() {
        match x {
            proc_macro::TokenTree::Group(_group) => {has_group = true;},
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
                if x_str == "<".to_string() {
                    level += 1;
                }
                if level == 1 && x_str != ",".to_string() {
                    if x_str != "<".to_string() && x_str != ">".to_string() {
                        l_template.push(x_str.clone());
                    }
                }
                if x_str == ">".to_string() {
                    level -= 1;
                }
            }
        }
        idx += 1;
    }
    if !has_group || level != 0 {
        return None;
    }
    match name {
        None => {
            return None;
        },
        Some(name) => {
            return Some((name,l_template));
        },
    }
}







fn get_header(trait_name: String, input: proc_macro::TokenStream) -> String {
    let pair_preamble = read_preamble(input.clone()).expect("read_preamble parsing error");
    let struct_name = pair_preamble.0;
    let l_template = pair_preamble.1;
    let first_template = l_template.get(0).expect("The list of template is empty");
    let mut seq_template = first_template.clone();
    for i in 1..l_template.len() {
        seq_template += &(", ".to_owned() + l_template.get(i).unwrap());
    }
    let mut header = "#[async_trait]\n".to_string();
    header += &format!("impl<{}> {}<{}> for {}<{}>\n", seq_template, trait_name, first_template, struct_name, seq_template);
    header += "where\n";
    header += &format!("    {}: Context + Send + Sync + Clone + 'static,\n", first_template);
    header += &format!("    ViewError: From<{}::Error>,\n", first_template);
    header += "{\n";
    return header
}



fn generate_view_code(l_title_type: Vec<(String,String)>, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    /*
    println!("generate_view_code l_title={:?} input={:?}", l_title, input);
    let mut pos = 0;
    for x in input.clone().into_iter() {
        println!("pos={} x={:?}", pos, x);
        pos += 1;
    }
    */
    let title_first = l_title_type[0].clone().0;
    let title_count = l_title_type.len();
    let pair_preamble = read_preamble(input.clone()).expect("read_preamble parsing error");
    let l_template = pair_preamble.1;
    let first_template = l_template.get(0).expect("The list of template is empty");

    let mut context_code_str = format!("  fn context(&self) -> &{}", first_template) + " {\n";
    context_code_str += &format!("    self.{}.context()\n", title_first);
    context_code_str += "  }\n";
    //
    let mut load_code_str = format!("  async fn load(context: {}) -> Result<Self, ViewError>", first_template) + " {\n";
    let mut return_code_str = "    Ok(Self {".to_string();
    for i_title in 0..title_count {
        let otype = l_title_type[i_title].1.clone();
        let str0 = format!("    let index{} : u64 = {};", i_title, i_title);
        let str1 = format!("    let base_key{} : Vec<u8> = context.derive_key(&index{})?;", i_title, i_title);
        let str2 = format!("    let {} = {}::load(context.clone_with_base_key(base_key{})).await?;", l_title_type[i_title].0, otype, i_title);
        load_code_str += &(str0 + "\n");
        load_code_str += &(str1 + "\n");
        load_code_str += &(str2 + "\n");
        if i_title > 0 {
            return_code_str += ",";
        }
        return_code_str += &format!(" {}", l_title_type[i_title].0);
    }
    return_code_str += " })";
    load_code_str += &(return_code_str + "\n");
    load_code_str += "  }\n";
    //
    let mut rollback_code_str = "  fn rollback(&mut self) {\n".to_string();
    for i_title in 0..title_count {
        let str0 = format!("    self.{}.rollback();", l_title_type[i_title].0);
        rollback_code_str += &(str0 + "\n");
    }
    rollback_code_str += "  }\n";
    //
    let mut flush_code_str = "  fn flush(&mut self, batch: &mut Batch) -> Result<(), ViewError> {\n".to_string();
//    flush_code_str += "use $crate::views::View;\n";
    for i_title in 0..title_count {
        let str0 = format!("    self.{}.flush(batch)?;", l_title_type[i_title].0);
        flush_code_str += &(str0 + "\n");
    }
    flush_code_str += "    Ok(())\n  }\n";
    //
    let mut delete_code_str = "  fn delete(self, batch: &mut Batch) {\n".to_string();
//    delete_code_str += "use $crate::views::View;\n";
    for i_title in 0..title_count {
        let str0 = format!("    self.{}.delete(batch);", l_title_type[i_title].0);
        delete_code_str += &(str0 + "\n");
    }
    delete_code_str += "  }\n";
    //
    let mut clear_code_str = "  fn clear(&mut self) {\n".to_string();
//    clear_code_str += "use $crate::views::View;\n";
    for i_title in 0..title_count {
        let str0 = format!("    self.{}.clear();", l_title_type[i_title].0);
        clear_code_str += &(str0 + "\n");
    }
    clear_code_str += "  }\n";
    //
    let header = get_header("View".to_string(), input);
    let gen_code = header + &context_code_str + &load_code_str + &rollback_code_str + &flush_code_str + &delete_code_str + &clear_code_str + "}\n";
    println!("gen_code={}", gen_code);

    proc_macro::TokenStream::from_str(&gen_code).expect("Failure in constructing the View code")
}


fn generate_container_view_code(l_title_type: Vec<(String,String)>, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
//    println!("generate_container_view_code l_title={:?} input={:?}", l_title, input);

    let title_count = l_title_type.len();

    let mut save_code_str = "  async fn save(&mut self) -> Result<(), ViewError> {\n".to_string();
//    save_code_str += "use $crate::views::View;\n";
    save_code_str += "    let mut batch = Batch::default();\n";
    for i_title in 0..title_count {
        let str0 = format!("    self.{}.flush(&mut batch)?;", l_title_type[i_title].0);
        save_code_str += &(str0 + "\n");
    }
    save_code_str += "    self.context().write_batch(batch).await?;\n";
    save_code_str += "    Ok(())\n  }\n";

    let mut delete_code_str = "  async fn write_delete(self) -> Result<(), ViewError> {\n".to_string();
//    delete_code_str += "use $crate::views::View;\n";
//    delete_code_str += "use $crate::common::Batch;\n";
    delete_code_str += "    let context = self.context().clone();\n";
    delete_code_str += "    let batch = Batch::build(move |batch| {\n";
    delete_code_str += "      Box::pin(async move {\n";
    for i_title in 0..title_count {
        delete_code_str += &format!("        self.{}.delete(batch);\n", l_title_type[i_title].0);
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

    proc_macro::TokenStream::from_str(&gen_code).expect("Failure in constructing the ContainerView code")
}



fn generate_hash_view_code(l_title_type: Vec<(String,String)>, input: proc_macro::TokenStream) -> proc_macro::TokenStream {
//    println!("generate_hash_view_code l_title={:?} input={:?}", l_title, input);

    let title_count = l_title_type.len();

    let hasher_code_str = "  type Hasher = sha2::Sha512;\n";

    let mut hash_code_str = "  async fn hash(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {\n".to_string();
    hash_code_str += "    use views::{Hasher, HashView};\n";
    hash_code_str += "    use std::io::Write;\n";
    hash_code_str += "    let mut hasher = Self::Hasher::default();\n";
    for i_title in 0..title_count {
        let str0 = format!("    hasher.write_all(self.{}.hash().await?.as_ref())?;", l_title_type[i_title].0);
        hash_code_str += &(str0 + "\n");
    }
    hash_code_str += "    Ok(hasher.finalize())\n  }\n";

    let header = get_header("HashView".to_string(), input);
    let gen_code = header + &hasher_code_str + &hash_code_str + "}\n";
    println!("gen_code={}", gen_code);

    proc_macro::TokenStream::from_str(&gen_code).expect("Failure in constructing the ContainerView code")
}


fn generate_hash_func_code(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
//    println!("generate_hash_func_code input={:?}", input);

    let name = read_preamble(input.clone()).expect("read_preamble parsing error").0;
    let name_hash = format!("{}Hash", name);

    let mut hash_value_code_str = "  async fn hash_value(&mut self) -> Result<HashValue, ViewError> {\n".to_string();
    hash_value_code_str += "    use crypto::{BcsSignable, HashValue};\n";
    hash_value_code_str += "    use generic_array::GenericArray;\n";
    hash_value_code_str += "    use views::HashView;\n";
    hash_value_code_str += "    use serde::{Serialize, Deserialize};\n";
    hash_value_code_str += "    use sha2::{Sha512, Digest};\n";
    hash_value_code_str += "    #[derive(Serialize, Deserialize)]\n";
    hash_value_code_str += &format!("    struct {}(GenericArray<u8, <Sha512 as Digest>::OutputSize>);\n", name_hash);
    hash_value_code_str += &(format!("    impl BcsSignable for {}", name_hash) + " {}\n");
    hash_value_code_str += "    let hash = self.hash().await?;\n";
    hash_value_code_str += &format!("    Ok(HashValue::new(&{}(hash)))\n", name_hash);
    hash_value_code_str += "  }\n";

    let header = get_header("HashFunc".to_string(), input);
    let gen_code = header + &hash_value_code_str + "}\n";
    println!("gen_code={}", gen_code);

    proc_macro::TokenStream::from_str(&gen_code).expect("Failure in constructing the HashValue code")
}


#[proc_macro_derive(View)]
pub fn derive_view(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let l_title_type = get_title_strings(input.clone()).expect("derive_view failed");
    generate_view_code(l_title_type, input)
}


#[proc_macro_derive(ContainerView)]
pub fn derive_container_view(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let l_title_type = get_title_strings(input.clone()).expect("derive_view failed");
    generate_container_view_code(l_title_type, input)
}


#[proc_macro_derive(HashView)]
pub fn derive_hash_view(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let l_title_type = get_title_strings(input.clone()).expect("derive_view failed");
    generate_hash_view_code(l_title_type, input)
}


#[proc_macro_derive(HashFunc)]
pub fn derive_hash_func(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    generate_hash_func_code(input)
}
