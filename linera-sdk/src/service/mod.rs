mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;

// Import the system interface.
wit_bindgen_guest_rust::import!("queryable_system.wit");

// Export the service interface.
wit_bindgen_guest_rust::export!(
    export_macro = "export_service"
    types_path = "service"
    "service.wit"
);
