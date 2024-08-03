// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

include!("src/serde_pretty/type.rs");
include!("src/version_info/type.rs");

fn main() {
    let VersionInfo {
        crate_version:
            Pretty {
                value:
                    CrateVersion {
                        major,
                        minor,
                        patch,
                    },
                ..
            },
        git_commit,
        git_dirty,
        rpc_hash,
        graphql_hash,
        wit_hash,
    } = {
        let mut paths = vec![];
        let version_info = VersionInfo::trace_get(
            std::path::Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()),
            &mut paths,
        )
        .unwrap();

        for path in paths {
            println!("cargo:rerun-if-changed={}", path.display());
        }

        version_info
    };

    let static_code = quote::quote! {
        VersionInfo {
            crate_version: crate::serde_pretty::Pretty::new(
                CrateVersion { major: #major, minor: #minor, patch: #patch },
            ),
            git_commit: ::std::borrow::Cow::Borrowed(#git_commit),
            git_dirty: #git_dirty,
            rpc_hash: ::std::borrow::Cow::Borrowed(#rpc_hash),
            graphql_hash: ::std::borrow::Cow::Borrowed(#graphql_hash),
            wit_hash: ::std::borrow::Cow::Borrowed(#wit_hash),
        }
    };

    let out_path = std::path::Path::new(&std::env::var_os("OUT_DIR").unwrap()).join("static.rs");

    fs_err::write(&out_path, static_code.to_string().as_bytes()).unwrap_or_else(|e| {
        panic!(
            "failed to write output file `{}`: {}",
            out_path.display(),
            e
        )
    });

    println!("cargo:rustc-cfg=linera_version_building");
    println!(
        "cargo:rustc-env=LINERA_VERSION_STATIC_PATH={}",
        out_path.display()
    )
}
