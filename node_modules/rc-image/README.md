# rc-image

React Image.

<!-- prettier-ignore -->
[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-image.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-image
[github-actions-image]: https://github.com/react-component/image/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/image/actions
[travis-image]: https://img.shields.io/travis/react-component/image/master?style=flat-square
[travis-url]: https://travis-ci.org/react-component/image
[circleci-image]: https://img.shields.io/circleci/build/github/react-component/image/master?style=flat-square
[circleci-url]: https://circleci.com/gh/react-component/image
[coveralls-image]: https://img.shields.io/coveralls/react-component/image.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/react-component/image?branch=master
[codecov-image]: https://img.shields.io/codecov/c/gh/react-component/image?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/image
[david-url]: https://david-dm.org/react-component/image
[david-image]: https://david-dm.org/react-component/image/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/image?type=dev
[david-dev-image]: https://david-dm.org/react-component/image/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-image.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-image
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-image
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-image
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Feature

- [x] Placeholder
- [x] Preview
- [x] Rotate
- [x] Zoom
- [x] Flip
- [x] Fallback
- [x] Multiple Preview

## install

[![rc-image](https://nodei.co/npm/rc-image.png)](https://npmjs.org/package/rc-image)

## Usage

```bash
npm install
npm start
```

```js
import Image from 'rc-image';

export default () => (
  <Image src="https://zos.alipayobjects.com/rmsportal/jkjgkEfvpUPVyRjUImniVslZfWPnJuuZ.png" />
);
```

## API

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| preview | boolean \| [PreviewType](#PreviewType) | true | Whether to show preview |
| prefixCls | string | rc-image | Classname prefix |
| placeholder | boolean \| ReactElement | - | if `true` will set default placeholder or use `ReactElement` set customize placeholder |
| fallback | string | - | Load failed src |
| previewPrefixCls | string | rc-image-preview | Preview classname prefix |
| onError | (event: Event) => void | - | Load failed callback |

### PreviewType

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| visible | boolean | - | Whether the preview is open or not |
| closeIcon | React.ReactNode | - | Custom close icon |
| src | string | - | Customize preview src |
| movable | boolean | true | Enable drag |
| scaleStep | number | 0.5 | The number to which the scale is increased or decreased |
| minScale | number | 1 | Min scale |
| maxScale | number | 50 | Max scale |
| forceRender | boolean | - | Force render preview |
| getContainer | string \| HTMLElement \| (() => HTMLElement) \| false | document.body | Return the mount node for preview |
| imageRender | (originalNode: React.ReactElement, info: { transform: [TransformType](#TransformType) }) => React.ReactNode | - | Customize image |
| toolbarRender | (originalNode: React.ReactElement, info: Omit<[ToolbarRenderInfoType](#ToolbarRenderInfoType), 'current' \| 'total'>) => React.ReactNode | - | Customize toolbar |
| onVisibleChange | (visible: boolean, prevVisible: boolean) => void | - | Callback when visible is changed |
| onTransform | { transform: [TransformType](#TransformType), action: [TransformAction](#TransformAction) } | - | Callback when transform is changed |

## Image.PreviewGroup

preview the merged src

```js
import Image from 'rc-image';

export default () => (
  <Image.PreviewGroup>
    <Image src="https://zos.alipayobjects.com/rmsportal/jkjgkEfvpUPVyRjUImniVslZfWPnJuuZ.png" />
    <Image src="https://gw.alipayobjects.com/mdn/rms_08e378/afts/img/A*P0S-QIRUbsUAAAAAAAAAAABkARQnAQ" />
  </Image.PreviewGroup>
);
```

### API

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| preview | boolean \| [PreviewGroupType](#PreviewGroupType) | true | Whether to show preview, <br> current: If Preview the show img index, default 0 |
| previewPrefixCls | string | rc-image-preview | Preview classname prefix |
| icons | { [iconKey]?: ReactNode } | - | Icons in the top operation bar, iconKey: 'rotateLeft' \| 'rotateRight' \| 'zoomIn' \| 'zoomOut' \| 'close' \| 'left' \| 'right' |
| fallback | string | - | Load failed src |
| items | (string \| { src: string, alt: string, crossOrigin: string, ... })[] | - | preview group |

### PreviewGroupType

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| visible | boolean | - | Whether the preview is open or not |
| movable | boolean | true | Enable drag |
| current | number | - | Current index |
| closeIcon | React.ReactNode | - | Custom close icon |
| scaleStep | number | 0.5 | The number to which the scale is increased or decreased |
| minScale | number | 1 | Min scale |
| maxScale | number | 50 | Max scale |
| forceRender | boolean | - | Force render preview |
| getContainer | string \| HTMLElement \| (() => HTMLElement) \| false | document.body | Return the mount node for preview |
| countRender | (current: number, total: number) => ReactNode | - | Customize count |
| imageRender | (originalNode: React.ReactElement, info: { transform: [TransformType](#TransformType), current: number }) => React.ReactNode | - | Customize image |
| toolbarRender | (originalNode: React.ReactElement, info: [ToolbarRenderInfoType](#ToolbarRenderInfoType)) => React.ReactNode | - | Customize toolbar |
| onVisibleChange | (visible: boolean, prevVisible: boolean, current: number) => void | - | Callback when visible is changed |
| onTransform | { transform: [TransformType](#TransformType), action: [TransformAction](#TransformAction) } | - | Callback when transform is changed |

### TransformType

```typescript
{
  x: number;
  y: number;
  rotate: number;
  scale: number;
  flipX: boolean;
  flipY: boolean;
}
```

### TransformAction

```typescript
type TransformAction =
  | 'flipY'
  | 'flipX'
  | 'rotateLeft'
  | 'rotateRight'
  | 'zoomIn'
  | 'zoomOut'
  | 'close'
  | 'prev'
  | 'next'
  | 'wheel'
  | 'doubleClick'
  | 'move'
  | 'dragRebound';
```

### ToolbarRenderInfoType

```typescript
{
  icons: {
    flipYIcon: React.ReactNode;
    flipXIcon: React.ReactNode;
    rotateLeftIcon: React.ReactNode;
    rotateRightIcon: React.ReactNode;
    zoomOutIcon: React.ReactNode;
    zoomInIcon: React.ReactNode;
  };
  actions: {
    onFlipY: () => void;
    onFlipX: () => void;
    onRotateLeft: () => void;
    onRotateRight: () => void;
    onZoomOut: () => void;
    onZoomIn: () => void;
  };
  transform: {
    x: number;
    y: number;
    rotate: number;
    scale: number;
    flipX: boolean;
    flipY: boolean;
  },
  current: number;
  total: number;
}
```

## Example

http://localhost:8003/examples/

## Test Case

```
npm test
```

## Coverage

```
npm run coverage
```

## License

rc-image is released under the MIT license.
