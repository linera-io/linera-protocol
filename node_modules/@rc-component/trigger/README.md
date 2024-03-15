# @rc-component/trigger

React Trigger Component

[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Test coverage][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-checkbox.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-checkbox
[github-actions-image]: https://github.com/react-component/checkbox/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/checkbox/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/checkbox/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/checkbox/branch/master
[david-url]: https://david-dm.org/react-component/checkbox
[david-image]: https://david-dm.org/react-component/checkbox/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/checkbox?type=dev
[david-dev-image]: https://david-dm.org/react-component/checkbox/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-checkbox.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-checkbox
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-checkbox
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-checkbox
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square
[dumi-url]: https://github.com/umijs/dumi

## Install

[![@rc-component/trigger](https://nodei.co/npm/@rc-component/trigger.png)](https://npmjs.org/package/@rc-component/trigger)

## Usage

Include the default [styling](https://github.com/react-component/trigger/blob/master/assets/index.less#L4:L11) and then:

```js
import React from 'react';
import ReactDOM from 'react-dom';
import Trigger from '@rc-component/trigger';

ReactDOM.render((
  <Trigger
    action={['click']}
    popup={<span>popup</span>}
    popupAlign={{
      points: ['tl', 'bl'],
      offset: [0, 3]
    }}
  >
    <a href='#'>hover</a>
  </Trigger>
), container);
```

## Compatibility

| [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt="IE / Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>IE / Edge | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/firefox/firefox_48x48.png" alt="Firefox" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Firefox | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/chrome/chrome_48x48.png" alt="Chrome" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Chrome | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/safari/safari_48x48.png" alt="Safari" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Safari | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/electron/electron_48x48.png" alt="Electron" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Electron |
| --- | --- | --- | --- | --- |
| IE11, Edge | last 2 versions | last 2 versions | last 2 versions | last 2 versions |

## Example

http://localhost:9001

## Development

```
npm install
npm start
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
          <td>alignPoint</td>
          <td>bool</td>
          <td>false</td>
          <td>Popup will align with mouse position (support action of 'click', 'hover' and 'contextMenu')</td>
        </tr>
        <tr>
          <td>popupClassName</td>
          <td>string</td>
          <td></td>
          <td>additional className added to popup</td>
        </tr>
        <tr>
          <td>forceRender</td>
          <td>boolean</td>
          <td>false</td>
          <td>whether render popup before first show</td>
        </tr>
        <tr>
          <td>destroyPopupOnHide</td>
          <td>boolean</td>
          <td>false</td>
          <td>whether destroy popup when hide</td>
        </tr>
        <tr>
          <td>getPopupClassNameFromAlign</td>
          <td>getPopupClassNameFromAlign(align: Object):String</td>
          <td></td>
          <td>additional className added to popup according to align</td>
        </tr>
        <tr>
          <td>action</td>
          <td>string[]</td>
          <td>['hover']</td>
          <td>which actions cause popup shown. enum of 'hover','click','focus','contextMenu'</td>
        </tr>
        <tr>
          <td>mouseEnterDelay</td>
          <td>number</td>
          <td>0</td>
          <td>delay time to show when mouse enter. unit: s.</td>
        </tr>
        <tr>
          <td>mouseLeaveDelay</td>
          <td>number</td>
          <td>0.1</td>
          <td>delay time to hide when mouse leave. unit: s.</td>
        </tr>
        <tr>
          <td>popupStyle</td>
          <td>Object</td>
          <td></td>
          <td>additional style of popup</td>
        </tr>
        <tr>
          <td>prefixCls</td>
          <td>String</td>
          <td>rc-trigger-popup</td>
          <td>prefix class name</td>
        </tr>
        <tr>
          <td>popupTransitionName</td>
          <td>String|Object</td>
          <td></td>
          <td>https://github.com/react-component/animate</td>
        </tr>
        <tr>
          <td>maskTransitionName</td>
          <td>String|Object</td>
          <td></td>
          <td>https://github.com/react-component/animate</td>
        </tr>
        <tr>
          <td>onPopupVisibleChange</td>
          <td>Function</td>
          <td></td>
          <td>call when popup visible is changed</td>
        </tr>
        <tr>
          <td>mask</td>
          <td>boolean</td>
          <td>false</td>
          <td>whether to support mask</td>
        </tr>
        <tr>
          <td>maskClosable</td>
          <td>boolean</td>
          <td>true</td>
          <td>whether to support click mask to hide</td>
        </tr>
        <tr>
          <td>popupVisible</td>
          <td>boolean</td>
          <td></td>
          <td>whether popup is visible</td>
        </tr>
        <tr>
          <td>zIndex</td>
          <td>number</td>
          <td></td>
          <td>popup's zIndex</td>
        </tr>
        <tr>
          <td>defaultPopupVisible</td>
          <td>boolean</td>
          <td></td>
          <td>whether popup is visible initially</td>
        </tr>
        <tr>
          <td>popupAlign</td>
          <td>Object: alignConfig of [dom-align](https://github.com/yiminghe/dom-align)</td>
          <td></td>
          <td>popup 's align config</td>
        </tr>
        <tr>
          <td>onPopupAlign</td>
          <td>function(popupDomNode, align)</td>
          <td></td>
          <td>callback when popup node is aligned</td>
        </tr>
        <tr>
          <td>popup</td>
          <td>React.Element | function() => React.Element</td>
          <td></td>
          <td>popup content</td>
        </tr>
        <tr>
          <td>getPopupContainer</td>
          <td>getPopupContainer(): HTMLElement</td>
          <td></td>
          <td>function returning html node which will act as popup container</td>
        </tr>
        <tr>
          <td>getDocument</td>
          <td>getDocument(): HTMLElement</td>
          <td></td>
          <td>function returning document node which will be attached click event to close trigger</td>
        </tr>
        <tr>
          <td>popupPlacement</td>
          <td>string</td>
          <td></td>
          <td>use preset popup align config from builtinPlacements, can be merged by popupAlign prop</td>
        </tr>
        <tr>
          <td>builtinPlacements</td>
          <td>object</td>
          <td></td>
          <td>builtin placement align map. used by placement prop</td>
        </tr>
        <tr>
          <td>stretch</td>
          <td>string</td>
          <td></td>
          <td>Let popup div stretch with trigger element. enums of 'width', 'minWidth', 'height', 'minHeight'. (You can also mixed with 'height minWidth')</td>
        </tr>
    </tbody>
</table>


## Test Case

```
npm test
npm run coverage
```

open coverage/ dir

## License

rc-trigger is released under the MIT license.
