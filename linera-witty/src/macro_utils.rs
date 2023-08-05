// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Common macros used by other macros.

/// Repeats a `macro` call with a growing list of `name: Type` or just `name` elements.
///
/// Calls the `macro` repeatedly, starting with list of the elements until the first `|`, then
/// adding the next element for each successive call.
macro_rules! repeat_macro {
    (
        $macro:tt =>
        $( $current_names:ident $( : $current_types:ident )? ),*
        | $next_name:ident $( : $next_type:ident )?
        $(, $queued_names:ident $( : $queued_types:ident )? )* $(,)?
    ) => {
        $macro!($( $current_names $(: $current_types)? ),*);

        repeat_macro!(
            $macro =>
            $( $current_names $(: $current_types)?, )*
            $next_name $(: $next_type)?
            | $($queued_names $(: $queued_types)? ),*
        );
    };

    ( $macro:tt => $( $current_names:ident $( : $current_types:ident )? ),* |) => {
        $macro!($( $current_names $(: $current_types)? ),*);
    };

    ( $macro:tt => $( $current_names:ident $( : $current_types:ident )? ),* $(,)*) => {
        repeat_macro!($macro => | $( $current_names $(: $current_types)? ),*);
    }
}
