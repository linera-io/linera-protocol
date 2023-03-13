mod conversions_from_wit;
mod conversions_to_wit;
pub mod exported_futures;
pub mod system_api;

// Import the system interface.
wit_bindgen_guest_rust::import!("writable_system.wit");

// Export the contract interface.
wit_bindgen_guest_rust::export!(
    export_macro = "export_contract"
    types_path = "contract"
    reexported_crate_path = "wit_bindgen_guest_rust"
    "contract.wit"
);

/// Declares an implementation of the [`Contract`][`crate::Contract`] trait, exporting it from the
/// WASM module.
///
/// Generates the necessary boilerplate for implementing the contract WIT interface, exporting the
/// necessary resource types and functions so that the host can call the contract application.
#[macro_export]
macro_rules! contract {
    ($application:ident) => {
        // Export the contract interface.
        $crate::export_contract!($application);

        /// Mark the contract type to be exported.
        impl $crate::contract::Contract for $application {
            type Initialize = Initialize;
            type ExecuteOperation = ExecuteOperation;
            type ExecuteEffect = ExecuteEffect;
            type HandleApplicationCall = HandleApplicationCall;
            type CallSession = CallSession;
        }

        $crate::instance_exported_future! {
            contract::Initialize<$application>(
                context: $crate::contract::OperationContext,
                argument: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteOperation<$application>(
                context: $crate::contract::OperationContext,
                operation: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::ExecuteEffect<$application>(
                context: $crate::contract::EffectContext,
                effect: Vec<u8>,
            ) -> PollExecutionResult
        }

        $crate::instance_exported_future! {
            contract::HandleApplicationCall<$application>(
                context: $crate::contract::CalleeContext,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::SessionId>,
            ) -> PollCallApplication
        }

        $crate::instance_exported_future! {
            contract::CallSession<$application>(
                context: $crate::contract::CalleeContext,
                session: $crate::contract::Session,
                argument: Vec<u8>,
                forwarded_sessions: Vec<$crate::contract::SessionId>,
            ) -> PollCallSession
        }
    };
}
