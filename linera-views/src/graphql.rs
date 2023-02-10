use async_graphql::scalar;
use serde::{Deserialize, Serialize};

scalar!(Range);

/// A range struct used for querying `LogView`s.
/// The behaviour of this struct is similar to `Range<usize>`.
#[derive(Serialize, Deserialize)]
pub struct Range {
    /// The start value.
    pub start: usize,
    /// The end value.
    pub end: usize,
}

impl From<Range> for std::ops::Range<usize> {
    fn from(range: Range) -> Self {
        std::ops::Range {
            start: range.start,
            end: range.end,
        }
    }
}
