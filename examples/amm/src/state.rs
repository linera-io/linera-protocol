use linera_sdk::views::{GraphQLView, RegisterView, RootView, ViewStorageContext};

#[derive(RootView, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct Amm {
    // TODO(#968): We should support stateless Applications/empty user views
    pub _dummy: RegisterView<u8>,
}
