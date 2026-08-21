(function() {
    var type_impls = Object.fromEntries([["alloy_primitives",[]],["revm_database",[]],["revm_state",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[23,21,18]}