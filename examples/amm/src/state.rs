use linera_sdk::views::{RegisterView, ViewStorageContext};
use linera_sdk::views::{GraphQLView, RootView};

#[derive(RootView, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct Amm {
    // TODO(#968): We should support stateless Applications/empty user views
    pub _dummy: RegisterView<u8>,
}
