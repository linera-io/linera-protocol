# rc-resize-observer

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/rc-resize-observer.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-resize-observer
[github-actions-image]: https://github.com/react-component/resize-observer/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/resize-observer/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/resize-observer/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/resize-observer/branch/master
[download-image]: https://img.shields.io/npm/dm/rc-resize-observer.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-resize-observer

Resize observer for React.

## Live Demo

https://react-component.github.io/resize-observer/

## Install

[![rc-resize-observer](https://nodei.co/npm/rc-resize-observer.png)](https://npmjs.org/package/rc-resize-observer)

## Usage

```js
import ResizeObserver from 'rc-resize-observer';
import { render } from 'react-dom';

render(
  <ResizeObserver
    onResize={() => {
      console.log('resized!');
    }}
  >
    <textarea />
  </ResizeObserver>,
  mountNode,
);
```

## API

| Property | Type                        | Default | Description                     |
| -------- | --------------------------- | ------- | ------------------------------- |
| disabled | boolean                     | false   |                                 |
| onResize | ({ width, height }) => void | -       | Trigger when child node resized |

## Development

```
npm install
npm start
```

## License

rc-resize-observer is released under the MIT license.
