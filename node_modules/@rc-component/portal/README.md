# rc-portal

React 18 supported Portal Component.

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/rc-portal.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-portal
[github-actions-image]: https://github.com/react-component/portal/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/portal/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/portal/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/portal
[download-image]: https://img.shields.io/npm/dm/rc-portal.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-portal

## Development

```bash
npm install
npm start
open http://localhost:8000
```

## Feature

- React life cycle support portal component

## Install

[![rc-portal](https://nodei.co/npm/rc-portal.png)](https://npmjs.org/package/rc-portal)

## Usage

```js | pure
import Portal from 'rc-portal';

const Demo = () => {
  return <Portal open>Hello World</Portal>;
};

export default Demo;
```

## 🔥 API

We use typescript to create the Type definition. You can view directly in IDE. But you can still check the type definition [here](https://github.com/react-component/portal/blob/master/src/interface.ts).

### Portal

| Prop         | Description                        | Type                     | Default       |
| ------------ | ---------------------------------- | ------------------------ | ------------- |
| getContainer | Customize portal container element | Element \| () => Element | document.body |
| open         | Show the portal                    | boolean                  | false         |
| autoLock     | Lock screen scroll when open       | boolean                  | false         |
