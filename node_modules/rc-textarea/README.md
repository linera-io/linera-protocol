# rc-textarea

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![npm download][download-image]][download-url] [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-textarea.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-textarea
[github-actions-image]: https://github.com/react-component/textarea/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/textarea/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/textarea/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/textarea/branch/master
[download-image]: https://img.shields.io/npm/dm/rc-textarea.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-textarea
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-textarea
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-textarea

Pretty Textarea react component used in [ant.design](https://ant.design).

## Live Demo

https://react-component.github.io/textarea/

## Install

[![rc-textarea](https://nodei.co/npm/rc-textarea.png)](https://npmjs.org/package/rc-textarea)

## Usage

```js
import Textarea from 'rc-textarea';
import { render } from 'react-dom';

render(<Textarea />, mountNode);
```

## API

| Property     | Type                        | Default     | Description                                                                                    |
| ------------ | --------------------------- | ----------- | ---------------------------------------------------------------------------------------------- |
| prefixCls    | string                      | rc-textarea |                                                                                                |
| className    | string                      | ''          | additional class name of textarea                                                              |
| style        | React.CSSProperties         | -           | style properties of textarea                                                                   |
| autoSize     | boolean \| object           | -           | Height autosize feature, can be set to `true\|false` or an object `{ minRows: 2, maxRows: 6 }` |
| onPressEnter | function(e)                 | -           | The callback function that is triggered when Enter key is pressed                              |
| onResize     | function({ width, height }) | -           | The callback function that is triggered when resize                                            |

## Development

```
npm install
npm start
```

## License

rc-textarea is released under the MIT license.
