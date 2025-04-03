(function() {
    var type_impls = Object.fromEntries([["alloy",[]],["alloy_provider",[]],["cranelift_codegen",[]],["libssh2_sys",[]],["libz_sys",[]],["linera_wasmer_vm",[]],["openssl_sys",[]],["revm",[]],["revm_interpreter",[]],["revm_precompile",[]],["revm_primitives",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[12,22,25,19,16,24,19,12,24,23,23]}