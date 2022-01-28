// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use futures::future;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::runtime::Runtime;

#[derive(Clone)]
struct LocalRequester(Arc<AtomicU32>);

impl LocalRequester {
    fn new() -> Self {
        Self(Arc::new(AtomicU32::new(0)))
    }
}

impl Requester for LocalRequester {
    type Key = &'static str;
    type Value = u32;

    fn query(&mut self, _key: Self::Key) -> future::BoxFuture<Self::Value> {
        Box::pin(future::ready(self.0.fetch_add(1, Ordering::Relaxed)))
    }
}

#[test]
fn test_local_downloader() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
        let requester = LocalRequester::new();
        let (task, mut handle) = Downloader::start(requester, vec![("a", 10), ("d", 11)]);
        assert_eq!(handle.query("b").await.unwrap(), 0);
        assert_eq!(handle.query("a").await.unwrap(), 10);
        assert_eq!(handle.query("d").await.unwrap(), 11);
        assert_eq!(handle.query("c").await.unwrap(), 1);
        assert_eq!(handle.query("b").await.unwrap(), 0);
        handle.stop().await.unwrap();
        let values: Vec<_> = task.await.unwrap().collect();
        // Cached values are returned ordered by keys.
        assert_eq!(values, vec![10, 0, 1, 11]);
    });
}
