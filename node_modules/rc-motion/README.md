# rc-motion

[![NPM version][npm-image]][npm-url] [![npm download][download-image]][download-url] [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![Dependencies][david-image]](david-url) [![DevDependencies][david-dev-image]][david-dev-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-motion.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-motion
[github-actions-image]: https://github.com/react-component/motion/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/motion/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/motion/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/motion/branch/master
[david-url]: https://david-dm.org/react-component/motion
[david-image]: https://david-dm.org/react-component/motion/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/motion?type=dev
[david-dev-image]: https://david-dm.org/react-component/motion/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-motion.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-motion
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-motion
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-motion

React lifecycle controlled motion library.

## Live Demo

https://react-component.github.io/motion/

## Install

[![rc-motion](https://nodei.co/npm/rc-motion.png)](https://npmjs.org/package/rc-motion)

## Example

```js
import CSSMotion from 'rc-motion';

export default ({ visible }) => (
  <CSSMotion visible={visible} motionName="my-motion">
    {({ className, style }) => <div className={className} style={style} />}
  </CSSMotion>
);
```

## API

### CSSMotion

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| motionName | string | - | Config motion name, will dynamic update when status changed |
| visible | boolean | true | Trigger motion events |
| motionAppear | boolean | true | Use motion when appear |
| motionEnter | boolean | true | Use motion when enter |
| motionLeave | boolean | true | Use motion when leave |
| motionLeaveImmediately | boolean | - | Will trigger leave even on mount |
| motionDeadline | number | - | Trigger motion status change even when motion event not fire |
| removeOnLeave | boolean | true | Remove element when motion leave end |
| leavedClassName | string | - | Set leaved element className |
| onAppearStart | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when appear start, return style will patch to element |
| onEnterStart | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when enter start, return style will patch to element |
| onLeaveStart | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when leave start, return style will patch to element |
| onAppearActive | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when appear active, return style will patch to element |
| onEnterActive | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when enter active, return style will patch to element |
| onLeaveActive | (HTMLElement, Event) => CSSProperties \| void; | - | Trigger when leave active, return style will patch to element |
| onAppearEnd | (HTMLElement, Event) => boolean \| void; | - | Trigger when appear end, will not finish when return false |
| onEnterEnd | (HTMLElement, Event) => boolean \| void; | - | Trigger when enter end, will not finish when return false |
| onLeaveEnd | (HTMLElement, Event) => boolean \| void; | - | Trigger when leave end, will not finish when return false |

### CSSMotionList

extends all the props from [CSSMotion](#CSSMotion)

| Property  | Type                          | Default | Description       |
| --------- | ----------------------------- | ------- | ----------------- |
| keys      | React.Key[]                   | -       | Motion list keys  |
| component | string \| React.ComponentType | div     | wrapper component |

## Development

```
npm install
npm start
```

## License

rc-motion is released under the MIT license.
