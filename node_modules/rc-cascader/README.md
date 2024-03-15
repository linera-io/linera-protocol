# rc-cascader

React Cascader Component

[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-cascader.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-cascader
[travis-image]: https://img.shields.io/travis/react-component/cascader/master?style=flat-square
[travis-url]: https://travis-ci.com/react-component/cascader
[github-actions-image]: https://github.com/react-component/cascader/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/cascader/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/cascader/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/cascader
[david-url]: https://david-dm.org/react-component/cascader
[david-image]: https://david-dm.org/react-component/cascader/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/cascader?type=dev
[david-dev-image]: https://david-dm.org/react-component/cascader/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-cascader.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-cascader
[bundlephobia-url]: https://bundlephobia.com/package/rc-cascader
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-cascader
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Browser Support

| [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt="IE / Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>IE / Edge | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/firefox/firefox_48x48.png" alt="Firefox" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Firefox | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/chrome/chrome_48x48.png" alt="Chrome" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Chrome | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/safari/safari_48x48.png" alt="Safari" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Safari | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/electron/electron_48x48.png" alt="Electron" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Electron |
| --- | --- | --- | --- | --- |
| IE11, Edge | last 2 versions | last 2 versions | last 2 versions | last 2 versions |

## Screenshots

<img src="https://os.alipayobjects.com/rmsportal/TYFXEbuQXIaMqQF.png" width="288"/>

## Example

https://cascader-react-component.vercel.app

## Install

[![rc-cascader](https://nodei.co/npm/rc-cascader.png)](https://npmjs.org/package/rc-cascader)

```bash
$ npm install rc-cascader --save
```

## Usage

```js
import React from 'react';
import Cascader from 'rc-cascader';

const options = [{
  'label': '福建',
  'value': 'fj',
  'children': [{
    'label': '福州',
    'value': 'fuzhou',
    'children': [{
      'label': '马尾',
      'value': 'mawei',
    }],
  }, {
    'label': '泉州',
    'value': 'quanzhou',
  }],
}, {
  'label': '浙江',
  'value': 'zj',
  'children': [{
    'label': '杭州',
    'value': 'hangzhou',
    'children': [{
      'label': '余杭',
      'value': 'yuhang',
    }],
  }],
}, {
  'label': '北京',
  'value': 'bj',
  'children': [{
    'label': '朝阳区',
    'value': 'chaoyang',
  }, {
    'label': '海淀区',
    'value': 'haidian',
  }],
}];

React.render(
  <Cascader options={options}>
    ...
  </Cascader>
, container);
```

## API

### props

<table class="table table-bordered table-striped">
  <thead>
  <tr>
    <th style="width: 100px;">name</th>
    <th style="width: 50px;">type</th>
    <th style="width: 50px;">default</th>
    <th>description</th>
  </tr>
  </thead>
  <tbody>
    <tr>
      <td>autoClearSearchValue</td>
      <td>boolean</td>
      <td>true</td>
      <td>Whether the current search will be cleared on selecting an item. Only applies when checkable</td>
    </tr>
    <tr>
      <td>options</td>
      <td>Object</td>
      <td></td>
      <td>The data options of cascade</td>
    </tr>
    <tr>
      <td>value</td>
      <td>Array</td>
      <td></td>
      <td>selected value</td>
    </tr>
    <tr>
      <td>defaultValue</td>
      <td>Array</td>
      <td></td>
      <td>initial selected value</td>
    </tr>
    <tr>
      <td>onChange</td>
      <td>Function(value, selectedOptions)</td>
      <td></td>
      <td>callback when finishing cascader select</td>
    </tr>
    <tr>
      <td>changeOnSelect</td>
      <td>Boolean</td>
      <td>false</td>
      <td>change value on each selection</td>
    </tr>
    <tr>
      <td>loadData</td>
      <td>Function(selectedOptions)</td>
      <td></td>
      <td>callback when click any option, use for loading more options</td>
    </tr>
    <tr>
      <td>expandTrigger</td>
      <td>String</td>
      <td>'click'</td>
      <td>expand current item when click or hover</td>
    </tr>
    <tr>
      <td>open</td>
      <td>Boolean</td>
      <td></td>
      <td>visibility of popup overlay</td>
    </tr>
    <tr>
      <td>onDropdownVisibleChange</td>
      <td>Function(visible)</td>
      <td></td>
      <td>callback when popup overlay's visibility changed</td>
    </tr>
    <tr>
      <td>transitionName</td>
      <td>String</td>
      <td></td>
      <td>transition className like "slide-up"</td>
    </tr>
    <tr>
      <td>prefixCls</td>
      <td>String</td>
      <td>rc-cascader</td>
      <td>prefix className of popup overlay</td>
    </tr>
    <tr>
      <td>dropdownClassName</td>
      <td>String</td>
      <td></td>
      <td>additional className of popup overlay</td>
    </tr>
    <tr>
      <td>popupPlacement</td>
      <td>String</td>
      <td>bottomLeft</td>
      <td>use preset popup align config from builtinPlacements：bottomRight topRight bottomLeft topLeft</td>
    </tr>
    <tr>
      <td>getPopupContainer</td>
      <td>function(trigger:Node):Node</td>
      <td>() => document.body</td>
      <td>container which popup select menu rendered into</td>
    </tr>
    <tr>
      <td>dropdownMenuColumnStyle</td>
      <td>Object</td>
      <td></td>
      <td>style object for each cascader pop menu</td>
    </tr>
    <tr>
      <td>fieldNames</td>
      <td>Object</td>
      <td>{ label: 'label', value: 'value', children: 'children' }</td>
      <td>custom field name for label and value and children</td>
    </tr>
    <tr>
      <td>expandIcon</td>
      <td>ReactNode</td>
      <td>></td>
      <td>specific the default expand icon</td>
    </tr>
    <tr>
      <td>loadingIcon</td>
      <td>ReactNode</td>
      <td>></td>
      <td>specific the default loading icon</td>
    </tr>
    <tr>
      <td>hidePopupOnSelect</td>
      <td>Boolean</td>
      <td>>true</td>
      <td>hide popup on select</td>
    </tr>
  </tbody>
</table>

### option

<table class="table table-bordered table-striped">
  <thead>
  <tr>
    <th style="width: 100px;">name</th>
    <th style="width: 50px;">type</th>
    <th style="width: 50px;">default</th>
    <th>description</th>
  </tr>
  </thead>
  <tbody>
    <tr>
      <td>label</td>
      <td>String</td>
      <td></td>
      <td>option text to display</td>
    </tr>
    <tr>
      <td>value</td>
      <td>String</td>
      <td></td>
      <td>option value as react key</td>
    </tr>
    <tr>
      <td>disabled</td>
      <td>Boolean</td>
      <td></td>
      <td>disabled option</td>
    </tr>
    <tr>
      <td>children</td>
      <td>Array</td>
      <td></td>
      <td>children options</td>
    </tr>
  </tbody>
</table>

## Development

```bash
$ npm install
$ npm start
```

## Test Case

```bash
$ npm test
```

## Coverage

```bash
$ npm run coverage
```

## License

rc-cascader is released under the MIT license.
