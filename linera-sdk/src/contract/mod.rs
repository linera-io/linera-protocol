mod conversions_from_wit;
mod conversions_to_wit;
pub mod system_api;

// Import the system interface.
wit_bindgen_guest_rust::import!("writable_system.wit");

// Export the contract interface.
wit_bindgen_guest_rust::export!(
    export_macro = "export_contract"
    types_path = "contract"
    "contract.wit"
);
