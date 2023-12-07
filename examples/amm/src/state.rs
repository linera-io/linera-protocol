use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};

#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct Amm {
    // TODO(#968): We should support stateless Applications/empty user views
    pub _dummy: RegisterView<u8>,
}
