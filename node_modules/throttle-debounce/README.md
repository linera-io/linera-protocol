# throttle-debounce

[![Build Status][ci-img]][ci]
[![BrowserStack Status][browserstack-img]][browserstack]
[![Mentioned in Awesome Micro npm Packages][awesome-img]][awesome]

Throttle and debounce functions.

This module is the same as [jquery-throttle-debounce][jquery-throttle-debounce]
([with some differences](#differences-with-original-module)), but it’s
transferred to ES Modules and CommonJS format.

## Install

```sh
npm install throttle-debounce --save
```

## Usage

### `throttle`

```js
import { throttle } from 'throttle-debounce';

const throttleFunc = throttle(
	1000,
	(num) => {
		console.log('num:', num);
	},
	{ noLeading: false, noTrailing: false }
);

// Can also be used like this, because noLeading and noTrailing are false by default
const throttleFunc = throttle(1000, (num) => {
	console.log('num:', num);
});

throttleFunc(1); // Will execute the callback
throttleFunc(2); // Won’t execute callback
throttleFunc(3); // Won’t execute callback

// Will execute the callback, because noTrailing is false,
// but if we set noTrailing to true, this callback won’t be executed
throttleFunc(4);

setTimeout(() => {
	throttleFunc(10); // Will execute the callback
}, 1200);

// Output
// num: 1
// num: 4
// num: 10
```

### `debounce`

```js
import { debounce } from 'throttle-debounce';

const debounceFunc = debounce(
	1000,
	(num) => {
		console.log('num:', num);
	},
	{ atBegin: false }
);

// Can also be used like this, because atBegin is false by default
const debounceFunc = debounce(1000, (num) => {
	console.log('num:', num);
});

// Won’t execute the callback, because atBegin is false,
// but if we set atBegin to true, this callback will be executed.
debounceFunc(1);

debounceFunc(2); // Won’t execute callback
debounceFunc(3); // Won’t execute callback

// Will execute the callback,
// but if we set atBegin to true, this callback won’t be executed.
debounceFunc(4);

setTimeout(() => {
	debounceFunc(10); // Will execute the callback
}, 1200);

// Output
// num: 4
// num: 10
```

### Cancelling

Debounce and throttle can both be cancelled by calling the `cancel` function.

```js
const throttleFunc = throttle(300, () => {
	// Throttled function
});

throttleFunc.cancel();

const debounceFunc = debounce(300, () => {
	// Debounced function
});

debounceFunc.cancel();
```

The logic that is being throttled or debounced will no longer be called.

To cancel only one upcoming debounced call, you can pass `upcomingOnly: true`
option to `cancel` function:

```js
const debounceFunc = debounce(300, () => {
	// Debounced function
});

debounceFunc(); // will not be invoked

debounceFunc.cancel({ upcomingOnly: true });

debounceFunc(); // will be invoked
```

## API

### throttle(delay, callback, { noLeading, noTrailing, debounceMode })

Returns: `Function`

Throttle execution of a function. Especially useful for rate limiting execution
of handlers on events like resize and scroll.

#### delay

Type: `Number`

A zero-or-greater delay in milliseconds. For event callbacks, values around 100
or 250 (or even higher) are most useful.

#### callback

Type: `Function`

A function to be executed after delay milliseconds. The `this` context and all
arguments are passed through, as-is, to `callback` when the throttled-function
is executed.

#### noTrailing

Type: `Boolean`

Optional, defaults to false. If noTrailing is true, callback will only execute
every `delay` milliseconds while the throttled-function is being called. If
noTrailing is false or unspecified, callback will be executed one final time
after the last throttled-function call. (After the throttled-function has not
been called for `delay` milliseconds, the internal counter is reset)

#### noLeading

Type: `Boolean`

Optional, defaults to false. If noLeading is false, the first throttled-function
call will execute callback immediately. If noLeading is true, the first the
callback execution will be skipped. It should be noted that callback will never
executed if both noLeading = true and noTrailing = true.

#### debounceMode

Type: `Boolean`

If `debounceMode` is true (at begin), schedule `clear` to execute after `delay`
ms. If `debounceMode` is false (at end), schedule `callback` to execute after
`delay` ms.

### debounce(delay, callback, { atBegin })

Returns: `Function`

Debounce execution of a function. Debouncing, unlike throttling, guarantees that
a function is only executed a single time, either at the very beginning of a
series of calls, or at the very end.

#### delay

Type: `Number`

A zero-or-greater delay in milliseconds. For event callbacks, values around 100
or 250 (or even higher) are most useful.

#### callback

Type: `Function`

A function to be executed after delay milliseconds. The `this` context and all
arguments are passed through, as-is, to `callback` when the debounced-function
is executed.

#### atBegin

Type: `Boolean`

Optional, defaults to false. If `atBegin` is false or unspecified, callback will
only be executed `delay` milliseconds after the last debounced-function call. If
`atBegin` is true, callback will be executed only at the first
debounced-function call. (After the throttled-function has not been called for
`delay` milliseconds, the internal counter is reset).

## Differences with original module

-   Dependancy on jQuery is removed, so if you rely on GUIDs set by jQuery, plan
    accordingly
-   There is no standalone version available, so don’t rely on `$.throttle` and
    `$.debounce` to be available

## Browser support

Tested in Chrome 72, Edge 15, Firefox 65 and should work in all modern browsers
([support based on Browserslist configuration](https://browserslist.dev/?q=bGFzdCAzIG1ham9yIHZlcnNpb25zLCBzaW5jZSAyMDE5LCBlZGdlID49IDE1LCBub3QgaWUgPiAw)).

## Test

For automated tests, run `npm run test:automated` (append `:watch` for watcher
support).

## License

<!-- prettier-ignore-start -->

**Original module license:** Copyright (c) 2010 "Cowboy" Ben Alman (Dual licensed under the MIT and GPL licenses. http://benalman.com/about/license/)  
**This module license:** MIT © [Ivan Nikolić](http://ivannikolic.com)

[ci]: https://github.com/niksy/throttle-debounce/actions?query=workflow%3ACI
[ci-img]: https://github.com/niksy/throttle-debounce/workflows/CI/badge.svg?branch=master
[browserstack]: https://www.browserstack.com/
[browserstack-img]: https://img.shields.io/badge/browser%20testing-BrowserStack-informational?logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NCA2NCI+CiAgPGRlZnMvPgogIDxyYWRpYWxHcmFkaWVudCBpZD0iYSIgY3g9IjIwLjk0Mjk3NiIgY3k9IjI4LjA5NDY3ODczIiByPSIzLjc5MTM0MTQxIiBncmFkaWVudFVuaXRzPSJ1c2VyU3BhY2VPblVzZSI+CiAgICA8c3RvcCBvZmZzZXQ9IjAiIHN0b3AtY29sb3I9IiM3OTc5NzkiLz4KICAgIDxzdG9wIG9mZnNldD0iMSIgc3RvcC1jb2xvcj0iIzRjNGM0YyIvPgogIDwvcmFkaWFsR3JhZGllbnQ+CiAgPGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTI5LjcyOTIwNCAtNTcuMTg3NjExKSBzY2FsZSgyLjk3MjkyKSI+CiAgICA8Y2lyY2xlIGN4PSIyMC43ODkiIGN5PSIzMC4wMjUiIHI9IjEwLjczOSIgZmlsbD0iI2Y0Yjk2MCIvPgogICAgPGNpcmNsZSBjeD0iMTkuNyIgY3k9IjI4LjkzNiIgcj0iOS43IiBmaWxsPSIjZTY2ZjMyIi8+CiAgICA8Y2lyY2xlIGN4PSIyMS4wMzYiIGN5PSIyNy42OTkiIHI9IjguNDEzIiBmaWxsPSIjZTQzYzQxIi8+CiAgICA8Y2lyY2xlIGN4PSIyMS42NzkiIGN5PSIyOC4zNDIiIHI9IjcuNzIiIGZpbGw9IiNiZGQwNDEiLz4KICAgIDxjaXJjbGUgY3g9IjIxLjEzNSIgY3k9IjI4LjkzNiIgcj0iNy4xNzYiIGZpbGw9IiM2ZGI1NGMiLz4KICAgIDxjaXJjbGUgY3g9IjE5Ljk5NyIgY3k9IjI3Ljc0OCIgcj0iNS45ODgiIGZpbGw9IiNhZWRhZTYiLz4KICAgIDxjaXJjbGUgY3g9IjIwLjkzNyIgY3k9IjI2Ljc1OCIgcj0iNS4wNDgiIGZpbGw9IiM1NmI4ZGUiLz4KICAgIDxjaXJjbGUgY3g9IjIxLjU4IiBjeT0iMjcuNDUxIiByPSI0LjQwNSIgZmlsbD0iIzAwYjFkNSIvPgogICAgPGNpcmNsZSBjeD0iMjAuOTM3IiBjeT0iMjguMDQ1IiByPSIzLjc2MSIgZmlsbD0idXJsKCNhKSIvPgogICAgPGNpcmNsZSBjeD0iMjAuOTM3IiBjeT0iMjguMDQ1IiByPSIzLjc2MSIgZmlsbD0iIzIyMWYxZiIvPgogICAgPGVsbGlwc2UgY3g9Ii0xNS4xNTkiIGN5PSIzMS40MDEiIGZpbGw9IiNmZmYiIHJ4PSIxLjE4OCIgcnk9Ii43NDIiIHRyYW5zZm9ybT0icm90YXRlKC02NS44MzQpIi8+CiAgPC9nPgo8L3N2Zz4K
[awesome]: https://github.com/parro-it/awesome-micro-npm-packages
[awesome-img]: https://awesome.re/mentioned-badge.svg
[jquery-throttle-debounce]: https://github.com/cowboy/jquery-throttle-debounce

<!-- prettier-ignore-end -->
