(function() {
    var type_impls = Object.fromEntries([["alloy",[]],["alloy_provider",[]],["async_graphql",[]],["cranelift_codegen",[]],["libssh2_sys",[]],["libz_sys",[]],["linera_wasmer_vm",[]],["openssl_sys",[]],["revm_interpreter",[]],["revm_precompile",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[12,22,21,25,19,16,24,19,24,23]}