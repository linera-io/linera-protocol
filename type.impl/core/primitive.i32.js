(function() {
    var type_impls = Object.fromEntries([["libc",[]],["linux_raw_sys",[]],["lz4_sys",[]],["unsafe_libyaml",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[11,21,15,22]}