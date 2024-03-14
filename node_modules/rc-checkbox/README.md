# rc-checkbox

---

Checkbox ui component for react.

[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-checkbox.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-checkbox
[travis-image]: https://img.shields.io/travis/react-component/checkbox/master?style=flat-square
[travis-url]: https://travis-ci.com/react-component/checkbox
[github-actions-image]: https://github.com/react-component/checkbox/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/checkbox/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/checkbox/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/checkbox
[david-url]: https://david-dm.org/react-component/checkbox
[david-image]: https://david-dm.org/react-component/checkbox/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/checkbox?type=dev
[david-dev-image]: https://david-dm.org/react-component/checkbox/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-checkbox.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-checkbox
[bundlephobia-url]: https://bundlephobia.com/package/rc-checkbox
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-checkbox
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Install

[![rc-checkbox](https://nodei.co/npm/rc-checkbox.png)](https://npmjs.org/package/rc-checkbox)

## Usage

```js
import checkbox from 'rc-checkbox';

export default () => <checkbox />;
```

## Compatibility

| [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt="IE / Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>IE / Edge | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/firefox/firefox_48x48.png" alt="Firefox" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Firefox | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/chrome/chrome_48x48.png" alt="Chrome" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Chrome | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/safari/safari_48x48.png" alt="Safari" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Safari | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/electron/electron_48x48.png" alt="Electron" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Electron |
| --- | --- | --- | --- | --- |
| IE11, Edge | last 2 versions | last 2 versions | last 2 versions | last 2 versions |

## API

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
      <td>prefixCls</td>
      <td>String</td>
      <td>rc-checkbox</td>
      <td></td>
    </tr>
    <tr>
      <td>className</td>
      <td>String</td>
      <td>''</td>
      <td>additional class name of root node</td>
    </tr>
      <tr>
      <td>name</td>
      <td>String</td>
      <td></td>
      <td>same with native input checkbox</td>
    </tr>
    <tr>
      <td>checked</td>
      <td>enum: 0,1,2</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <td>defaultChecked</td>
      <td>enum: 0,1,2</td>
      <td>0</td>
      <td>same with native input checkbox</td>
    <tr>
      <td>onChange</td>
      <td>Function(e:Event, checked:Number)</td>
      <td></td>
      <td>called when checkbox is changed. e is native event, checked is original checked state.</td>
    </tr>
  </tbody>
</table>

## Development

```
npm install
npm start
```

Online demo: http://react-component.github.io/checkbox/

## License

rc-checkbox is released under the MIT license.
