# rc-drawer

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![codecov](https://codecov.io/gh/react-component/drawer/branch/master/graph/badge.svg)](https://codecov.io/gh/react-component/drawer) [![node version][node-image]][node-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/rc-drawer.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-drawer
[github-actions-image]: https://github.com/react-component/drawer/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/drawer/actions
[node-image]: https://img.shields.io/badge/node.js-%3E=_0.10-green.svg?style=flat-square
[node-url]: http://nodejs.org/download/
[download-image]: https://img.shields.io/npm/dm/rc-drawer.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-drawer

## Example

https://drawer-react-component.vercel.app/

## Usage

```js
import Drawer from 'rc-drawer';
import React from 'react';
import ReactDom from 'react-dom';

ReactDom.render(
  <Drawer>
    {menu children}
  </Drawer>
, mountNode);
```

## Install

[![rc-drawer](https://nodei.co/npm/rc-drawer.png)](https://npmjs.org/package/rc-drawer)

## Browser Support

| ![IE](https://github.com/alrra/browser-logos/blob/master/src/edge/edge_48x48.png?raw=true) | ![Chrome](https://github.com/alrra/browser-logos/blob/master/src/chrome/chrome_48x48.png?raw=true) | ![Firefox](https://github.com/alrra/browser-logos/blob/master/src/firefox/firefox_48x48.png?raw=true) | ![Opera](https://github.com/alrra/browser-logos/blob/master/src/opera/opera_48x48.png?raw=true) | ![Safari](https://github.com/alrra/browser-logos/blob/master/src/safari/safari_48x48.png?raw=true) |
| ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| IE 10+ ✔                                                                                  | Chrome 31.0+ ✔                                                                                    | Firefox 31.0+ ✔                                                                                      | Opera 30.0+ ✔                                                                                  | Safari 7.0+ ✔                                                                                     |

## API

| props              | type                                                                        | default                                | description                                                                   |
| ------------------ | --------------------------------------------------------------------------- | -------------------------------------- | ----------------------------------------------------------------------------- |
| className          | string                                                                      | null                                   | -                                                                             |
| classNames         | { mask?: string; content?: string; wrapper?: string; }                      | -                                      | pass className to target area                                                 |
| styles             | { mask?: CSSProperties; content?: CSSProperties; wrapper?: CSSProperties; } | -                                      | pass style to target area                                                     |
| prefixCls          | string                                                                      | 'drawer'                               | prefix class                                                                  |
| width              | string \| number                                                            | null                                   | drawer content wrapper width, drawer level transition width                   |
| height             | string \| number                                                            | null                                   | drawer content wrapper height, drawer level transition height                 |
| open               | boolean                                                                     | false                                  | open or close menu                                                            |
| defaultOpen        | boolean                                                                     | false                                  | default open menu                                                             |
| placement          | string                                                                      | `left`                                 | `left` `top` `right` `bottom`                                                 |
| level              | string \| array                                                             | `all`                                  | With the drawer level element. `all`/ null / className / id / tagName / array |
| levelMove          | number \| array \| func                                                     | null                                   | level move value. default is drawer width                                     |
| duration           | string                                                                      | `.3s`                                  | level animation duration                                                      |
| ease               | string                                                                      | `cubic-bezier(0.78, 0.14, 0.15, 0.86)` | level animation timing function                                               |
| getContainer       | string \| func \| HTMLElement                                               | `body`                                 | Return the mount node for Drawer. if is `null` use React.creactElement        |
| showMask           | boolean                                                                     | true                                   | mask is show                                                                  |
| maskClosable       | boolean                                                                     | true                                   | Clicking on the mask (area outside the Drawer) to close the Drawer or not.    |
| maskStyle          | CSSProperties                                                               | null                                   | mask style                                                                    |
| afterVisibleChange | func                                                                        | null                                   | transition end callback(open)                                                 |
| onClose            | func                                                                        | null                                   | close click function                                                          |
| keyboard           | boolean                                                                     | true                                   | Whether support press esc to close                                            |
| autoFocus          | boolean                                                                     | true                                   | Whether focusing on the drawer after it opened                                |
| onMouseEnter       | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse enter drawer panel                                         |
| onMouseOver        | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse over drawer panel                                          |
| onMouseLeave       | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse leave drawer panel                                         |
| onClick            | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse click drawer panel                                         |
| onKeyDown          | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse keydown on drawer panel                                    |
| onKeyUp            | React.MouseEventHandler\<HTMLDivElement\>                                   | -                                      | Trigger when mouse keyup on drawer panel                                      |

> 2.0 Rename `onMaskClick` -> `onClose`, add `maskClosable`.

## Development

```
npm install
npm start
```
