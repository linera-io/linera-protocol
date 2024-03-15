# rc-dialog

react dialog component

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Test coverage][coveralls-image]][coveralls-url] [![npm download][download-image]][download-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-dialog.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-dialog
[github-actions-image]: https://github.com/react-component/dialog/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/dialog/actions
[circleci-image]: https://img.shields.io/circleci/react-component/dialog/master?style=flat-square
[circleci-url]: https://circleci.com/gh/react-component/dialog
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/dialog/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/dialog
[download-image]: https://img.shields.io/npm/dm/rc-dialog.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-dialog
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-dialog
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-dialog

## Screenshot

<img src="http://gtms04.alicdn.com/tps/i4/TB1dp5lHXXXXXbmXpXXyVug.FXX-664-480.png" />

## Example

http://localhost:8007/examples/

online example: https://dialog.react-component.vercel.app/

## Install

[![rc-dialog](https://nodei.co/npm/rc-dialog.png)](https://npmjs.org/package/rc-dialog)

## Usage

```js
var Dialog = require('rc-dialog');

ReactDOM.render(
  <Dialog title={title} onClose={callback1} visible>
      <p>first dialog</p>
  </Dialog>
), document.getElementById('t1'));

// use dialog
```

## API

### rc-dialog

| Name                   | Type                           | Default   | Description                                                                     | Version |
| ---------------------- | ------------------------------ | --------- | ------------------------------------------------------------------------------- | ------- |
| prefixCls              | String                         | rc-dialog | The dialog dom node's prefixCls                                                 |         |
| className              | String                         |           | additional className for dialog                                                 |         |
| classNames             | { mask?: string; wrapper?: string; header?: string; body?: string; footer?: string} |           | pass className to target area |         |
| styles                 | { mask?: CSSProperties; wrapper?: CSSProperties; header?: CSSProperties; body?: CSSProperties; footer?: CSSProperties} |    | pass styles to target area |         |
| style                  | Object                         | {}        | Root style for dialog element.Such as width, height                             |         |
| zIndex                 | Number                         |           |                                                                                 |         |
| visible                | Boolean                        | false     | current dialog's visible status                                                 |         |
| animation              | String                         |           | part of dialog animation css class name                                         |         |
| maskAnimation          | String                         |           | part of dialog's mask animation css class name                                  |         |
| transitionName         | String                         |           | dialog animation css class name                                                 |         |
| maskTransitionName     | String                         |           | mask animation css class name                                                   |         |
| title                  | String\|React.Element          |           | Title of the dialog                                                             |         |
| footer                 | React.Element                  |           | footer of the dialog                                                            |         |
| closable               | Boolean                        | true      | whether show close button                                                       |         |
| mask                   | Boolean                        | true      | whether show mask                                                               |         |
| maskClosable           | Boolean                        | true      | whether click mask to close                                                     |         |
| keyboard               | Boolean                        | true      | whether support press esc to close                                              |         |
| mousePosition          | {x:number,y:number}            |           | set pageX and pageY of current mouse(it will cause transform origin to be set). |         |
| onClose                | function()                     |           | called when click close button or mask                                          |         |
| afterClose             | function()                     |           | called when close animation end                                                 |         |
| getContainer           | function(): HTMLElement        |           | to determine where Dialog will be mounted                                       |         |
| destroyOnClose         | Boolean                        | false     | to unmount child compenents on onClose                                          |         |
| closeIcon              | ReactNode                      |           | specific the close icon.                                                        |         |
| forceRender            | Boolean                        | false     | Create dialog dom node before dialog first show                                 |         |
| focusTriggerAfterClose | Boolean                        | true      | focus trigger element when dialog closed                                        |         |
| modalRender            | (node: ReactNode) => ReactNode |           | Custom modal content render                                                     | 8.3.0   |

## Development

```
npm install
npm start
```



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

rc-dialog is released under the MIT license.
