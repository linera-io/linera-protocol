<h1 align="center">Ant Design Colors</h1>

<div align="center">

:art: Color palettes calculator of [Ant Design](https://ant.design/docs/spec/colors).

[![CI status][github-action-image]][github-action-url]
[![codecov][codecov-image]][codecov-url]
[![NPM version][npm-image]][npm-url]
[![NPM downloads][download-image]][download-url]
[![][bundlephobia-image]][bundlephobia-url]

![](https://raw.githubusercontent.com/andreasbm/readme/master/assets/lines/rainbow.png)

[npm-image]: http://img.shields.io/npm/v/@ant-design/colors.svg?style=flat-square
[npm-url]: http://npmjs.org/package/@ant-design/colors
[github-action-image]: https://github.com/ant-design/ant-design-colors/actions/workflows/ci.yml/badge.svg
[github-action-url]: https://github.com/ant-design/ant-design-colors/actions/workflows/ci.yml
[codecov-image]: https://img.shields.io/codecov/c/github/ant-design/ant-design-colors/main.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/ant-design/ant-design-colors/tree/main
[download-image]: https://img.shields.io/npm/dm/@ant-design/colors.svg?style=flat-square
[download-url]: https://npmjs.org/package/@ant-design/colors
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/@ant-design/colors?style=flat-square
[bundlephobia-url]: https://bundlephobia.com/package/@ant-design/colors
</div>

![](https://user-images.githubusercontent.com/507615/55726820-43e68400-5a43-11e9-8541-b0fc28b78f37.png)

## Install

```bash
$ npm install @ant-design/colors
// or
$ yarn add @ant-design/colors
```

## Usage

```bash
$ npm install @ant-design/colors --save
```

```js
import {
  red,
  volcano,
  gold,
  yellow,
  lime,
  green,
  cyan,
  blue,
  geekblue,
  purple,
  magenta,
  grey,
} from '@ant-design/colors';

console.log(blue); // ['#E6F4FF', '#BAE0FF', '#91CAFF', '#69B1FF', '#4096FF', '#1677FF', '#0958D9', '#003EB3', '#002C8C', '#001D66']
console.log(blue.primary); // '#1677FF'
```

```js
import { generate, presetPalettes } from '@ant-design/colors';

// Generate color palettes by a given color
const colors = generate('#1890ff');
console.log(colors); // ['#E6F7FF', '#BAE7FF', '#91D5FF', ''#69C0FF', '#40A9FF', '#1890FF', '#096DD9', '#0050B3', '#003A8C', '#002766']
console.log(presetPalettes);
/*
{
  red: [...],
  volcano: [...],
  orange: [...],
  gold: [...],
  yellow: [...],
  lime: [...],
  green: [...],
  cyan: [...],
  blue: [...],
  geekblue: [...],
  purple: [...],
  magenta: [...],
}
*/
```

```js
import { generate, presetDarkPalettes } from '@ant-design/colors';

// Generate dark color palettes by a given color
const colors = generate('#1890ff', {
  theme: 'dark',
  backgroundColor: '#141414',
});
console.log(colors); // ['#111d2c', '#112a45', '#15395b', '#164c7e', '#1765ad', '#177ddc', '#3c9ae8', '#65b7f3', '#8dcff8', '#b7e3fa']
console.log(presetDarkPalettes);
/*
{
  red: [...],
  volcano: [...],
  orange: [...],
  gold: [...],
  yellow: [...],
  lime: [...],
  green: [...],
  cyan: [...],
  blue: [...],
  geekblue: [...],
  purple: [...],
  magenta: [...],
}
*/
```

## Articles

- [Ant Design Colors](https://ant.design/docs/spec/colors)
- [Ant Design 色板生成算法演进之路](https://zhuanlan.zhihu.com/p/32422584)
