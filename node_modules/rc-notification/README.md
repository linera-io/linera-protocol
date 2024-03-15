# rc-notification

React Notification UI Component

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Test coverage][coveralls-image]][coveralls-url] [![npm download][download-image]][download-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-notification.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-notification
[github-actions-image]: https://github.com/react-component/notification/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/notification/actions
[coveralls-image]: https://img.shields.io/coveralls/react-component/notification.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/react-component/notification?branch=master
[download-image]: https://img.shields.io/npm/dm/rc-notification.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-notification
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-notification
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-notification

## Install

[![rc-notification](https://nodei.co/npm/rc-notification.png)](https://npmjs.org/package/rc-notification)

## Usage

```js
import Notification from 'rc-notification';

Notification.newInstance({}, notification => {
  notification.notice({
    content: 'content'
  });
});
```

## Compatibility

| [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/edge/edge_48x48.png" alt="IE / Edge" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>IE / Edge | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/firefox/firefox_48x48.png" alt="Firefox" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Firefox | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/chrome/chrome_48x48.png" alt="Chrome" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Chrome | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/safari/safari_48x48.png" alt="Safari" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Safari | [<img src="https://raw.githubusercontent.com/alrra/browser-logos/master/src/electron/electron_48x48.png" alt="Electron" width="24px" height="24px" />](http://godban.github.io/browsers-support-badges/)<br>Electron |
| --- | --- | --- | --- | --- |
| IE11, Edge | last 2 versions | last 2 versions | last 2 versions | last 2 versions |

## Example

http://localhost:8001

online example: https://notification-react-component.vercel.app

## API

### Notification.newInstance(props, (notification) => void) => void

props details:

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
          <td></td>
          <td>prefix class name for notification container</td>
        </tr>
        <tr>
          <td>style</td>
          <td>Object</td>
          <td>{'top': 65, left: '50%'}</td>
          <td>additional style for notification container.</td>
        </tr>
        <tr>
          <td>getContainer</td>
          <td>getContainer(): HTMLElement</td>
          <td></td>
          <td>function returning html node which will act as notification container</td>
        </tr>
        <tr>
          <td>maxCount</td>
          <td>number</td>
          <td></td>
          <td>max notices show, drop first notice if exceed limit</td>
        </tr>
    </tbody>
</table>

### notification.notice(props)

props details:

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
          <td>content</td>
          <td>React.Element</td>
          <td></td>
          <td>content of notice</td>
        </tr>
        <tr>
          <td>key</td>
          <td>String</td>
          <td></td>
          <td>id of this notice</td>
        </tr>
        <tr>
          <td>closable</td>
          <td>Boolean</td>
          <td></td>
          <td>whether show close button</td>
        </tr>
        <tr>
          <td>onClose</td>
          <td>Function</td>
          <td></td>
          <td>called when notice close</td>
        </tr>
        <tr>
          <td>duration</td>
          <td>number</td>
          <td>1.5</td>
          <td>after duration of time, this notice will disappear.(seconds)</td>
        </tr>
        <tr>
          <td>style</td>
          <td>Object</td>
          <td> { right: '50%' } </td>
          <td>additional style for single notice node.</td>
        </tr>
        <tr>
          <td>closeIcon</td>
          <td>ReactNode</td>
          <td></td>
          <td>specific the close icon.</td>
        </tr>
        <tr>
          <td>props</td>
          <td>Object</td>
          <td></td>
          <td>An object that can contain <code>data-*</code>, <code>aria-*</code>, or <code>role</code> props, to be put on the notification <code>div</code>. This currently only allows <code>data-testid</code> instead of <code>data-*</code> in TypeScript. See https://github.com/microsoft/TypeScript/issues/28960.</td>
        </tr>
    </tbody>
</table>

### notification.removeNotice(key:string)

remove single notice with specified key

### notification.destroy()

destroy current notification

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

rc-notification is released under the MIT license.
