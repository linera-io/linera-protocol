# rc-segmented

[![NPM version][npm-image]][npm-url] [![npm download][download-image]][download-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-segmented.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-segmented
[github-actions-image]: https://github.com/react-component/segmented/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/segmented/actions
[codecov-image]: https://codecov.io/gh/react-component/segmented/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/react-component/segmented/branch/master
[download-image]: https://img.shields.io/npm/dm/rc-segmented.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-segmented
[bundlephobia-url]: https://bundlephobia.com/package/rc-segmented
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-segmented

React Segmented Control.

![](https://gw.alipayobjects.com/mdn/rms_50855f/afts/img/A*bmGGQpnWs0oAAAAAAAAAAAAAARQnAQ)

## Live Demo

https://react-component.github.io/segmented/

## Install

[![rc-segmented](https://nodei.co/npm/rc-segmented.png)](https://npmjs.org/package/rc-segmented)

## Usage

```js
import Segmented from 'rc-segmented';
import 'rc-segmented/assets/index.css'; // import 'rc-segmented/asssets/index.less';
import { render } from 'react-dom';

render(
  <Segmented
    options={['Antd', 'Antv', 'Egg.js']}
    onChange={(value) => handleValueChange(value)}
  />,
  mountNode,
);
```

## API

Please note that **onChange** API
 changed on v2.0.0+

| Property     | Type                                                           | Default      | Description                        |
| --------- | --------- | --------- | --------- |
| prefixCls    | string                                                         | rc-segmented | prefixCls of this component |
| className    | string                                                         | ''           | additional class name of segmented |
| style        | React.CSSProperties                                            |              | style properties of segmented      |
| options      | Array<string \| number \| [SegmentedOption](#SegmentedOption)> | []           | options for choices                |
| value        | string \| number                                               |              | value of segmented                 |
| defaultValue | string \| number                                               |              | defaultValue of segmented          |
| value | string \| number                                               |              | currently selected value of segmented          |
| onChange     | (value: string \| number) => void                                               |              | defaultValue of segmented          |
| disabled     | boolean                                                        | false        | disabled status of segmented       |

### SegmentedOption

| Property  | Type      | Default | Description                               |
| --------- | --------- | --------- | --------- |
| label     | ReactNode |         | label of segmented option                 |
| value     | string \| number   |  | value of segmented option |
| className | string    | ''      | additional class name of segmented option |
| disabled  | boolean   | false   | disabled status of segmented option       |

## Development

```
npm install
npm start
```

## License

rc-segmented is released under the MIT license.
