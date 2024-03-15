# rc-rate

React Rate Component

[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-rate.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-rate
[github-actions-image]: https://github.com/react-component/rate/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/rate/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/rate/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/rate/branch/master
[david-url]: https://david-dm.org/react-component/rate
[david-image]: https://david-dm.org/react-component/rate/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/rate?type=dev
[david-dev-image]: https://david-dm.org/react-component/rate/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-rate.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-rate
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-rate
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-rate
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Screenshots

<img src="https://img.alicdn.com/tps/TB1ijlpLVXXXXb8XpXXXXXXXXXX-466-172.png" width="288"/>

## Changelog

- [CHANGELOG](./CHANGELOG.md)

## Development

```
npm install
npm start
```

## Example

- Local: http://localhost:9001/

- Online: http://react-component.github.io/rate/

## install

[![rc-rate](https://nodei.co/npm/rc-rate.png)](https://npmjs.org/package/rc-rate)

## Usage

```js
import React from 'react';
import ReactDOM from 'react-dom';
import Rate from 'rc-rate';

ReactDOM.render(
  <Rate />,
  document.getElementById('root')
)
```

### with [styled-components](https://github.com/styled-components/styled-components)
```js
import React from 'react';
import ReactDOM from 'react-dom';
import Rate from 'rc-rate';
import styled from 'styled-components';

const StyledRate = styled(Rate)`
  &.rc-rate {
    font-size: ${({ size }) => size}px;
  }
`

ReactDOM.render(
  <StyledRate size="24" />,
  document.getElementById('root')
)
```

## API

### props

| name          | type                              | default       | description                                           |
| ------------- | --------------------------------- | ------------- | ----------------------------------------------------- |
| count         | number                            | 5             | Star numbers                                          |
| value         | number                            | -             | Controlled value                                      |
| defaultValue  | number                            | 0             | Initial value                                         |
| allowHalf     | boolean                           | false         | Support half star                                     |
| allowClear    | boolean                           | true          | Reset when click again                                |
| style         | object                            | {}            |                                                       |
| onChange      | function                          | (value) => {} | `onChange` will be triggered when click               |
| onHoverChange | function                          | (value) => {} | `onHoverChange` will be triggered when hover on stars |
| character     | ReactNode \| (props) => ReactNode | ★             | The each character of rate                            |
| disabled      | boolean                           | false         |                                                       |
| direction     | string                            | `ltr`         | The direction of rate                                 |

## Test Case

```
npm test
npm run chrome-test
```

## Coverage

```
npm run coverage
```

open coverage/ dir

## License

rc-rate is released under the MIT license.
