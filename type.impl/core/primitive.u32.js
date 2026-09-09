(function() {
    var type_impls = Object.fromEntries([["const_oid",[]],["libc",[]],["linux_raw_sys",[]],["lz4_sys",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[16,12,21,15]}