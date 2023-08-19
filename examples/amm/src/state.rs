use linera_sdk::views::{RegisterView, ViewStorageContext};
use linera_views::views::{GraphQLView, RootView};

#[derive(RootView, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct Amm {
    pub _dummy: RegisterView<u8>,
}
