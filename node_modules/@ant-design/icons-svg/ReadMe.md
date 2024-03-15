<h1 align="center">
Ant Design Icons
</h1>

<p align="center">
⭐ The abstract node of the Ant Design SVG icons.
</p>

<div align="center">

[![NPM version](https://img.shields.io/npm/v/@ant-design/icons-svg.svg?style=flat)](https://npmjs.org/package/@ant-design/icons-svg)
[![NPM downloads](http://img.shields.io/npm/dm/@ant-design/icons-svg.svg?style=flat)](https://npmjs.org/package/@ant-design/icons-svg)

</div>

Check [all icons list](https://github.com/ant-design/ant-design-icons/issues/227).

## Install

```bash
# use yarn
$ yarn add @ant-design/icons-svg

# or use npm
$ npm install @ant-design/icons-svg --save
```

## Use Library Adapter

- React: See [@ant-design/icons](../icons-react) to learn about detail usage.

## Contribution Guide 贡献指南

See contribution guide. [English](./docs/ContributionGuide.md) ｜ [中文](./docs/ContributionGuide.zh-CN.md)

## Get started

```ts
import { AccountBookOutlined } from '@ant-design/icons-svg';
// or
// import AccountBookOutlined from '@ant-design/icons-svg/es/asn/AccountBookOutlined';

console.log(AccountBookOutlined);
// ==>
// {
//   name: 'account-book',
//   theme: 'outlined',
//   icon: {
//     tag: 'svg',
//     attrs: {
//       viewBox: '64 64 896 896',
//       focusable: 'false'
//     },
//     children: [
//       {
//         tag: 'path',
//         attrs: {
//           d:
//             'M880 184H712v-64c0-4.4-3.6-8-8-8h- ...'
//         }
//       }
//     ]
//   }
// };
```

- Interfaces

This library export all SVG files as `IconDefinition`.

```ts
// types.d.ts
export declare type ThemeType = 'filled' | 'outlined' | 'twotone';

export interface AbstractNode {
  tag: string;
  attrs: {
    [key: string]: string;
  };
  children?: AbstractNode[];
}

export interface IconDefinition {
  name: string; // kebab-case-style
  theme: ThemeType;
  icon:
    | ((primaryColor: string, secondaryColor: string) => AbstractNode)
    | AbstractNode;
}
```

## Render Helpers

```ts
import { AccountBookFilled } from '@ant-design/icons-svg';
import { renderIconDefinitionToSVGElement } from '@ant-design/icons-svg/es/helpers';

const svgHTMLString = renderIconDefinitionToSVGElement(AccountBookFilled, {
  extraSVGAttrs: { width: '1em', height: '1em', fill: 'currentColor' }
});

console.log(svgHTMLString);
// ==>
// '<svg viewBox="64 64 896 896" width="1em" height="1em" fill="currentColor"><path d="M880 184H712v-64c0-4.4-3.6-8-8-8h-56c-4.4 0-8 3.6-8 8v64H384v-64c0-4.4-3.6-8-8-8h-56c-4.4 0-8 3.6-8 8v64H144c-17.7 0-32 14.3-32 32v664c0 17.7 14.3 32 32 32h736c17.7 0 32-14.3 32-32V216c0-17.7-14.3-32-32-32zM648.3 426.8l-87.7 161.1h45.7c5.5 0 10 4.5 10 10v21.3c0 5.5-4.5 10-10 10h-63.4v29.7h63.4c5.5 0 10 4.5 10 10v21.3c0 5.5-4.5 10-10 10h-63.4V752c0 5.5-4.5 10-10 10h-41.3c-5.5 0-10-4.5-10-10v-51.8h-63.1c-5.5 0-10-4.5-10-10v-21.3c0-5.5 4.5-10 10-10h63.1v-29.7h-63.1c-5.5 0-10-4.5-10-10v-21.3c0-5.5 4.5-10 10-10h45.2l-88-161.1c-2.6-4.8-.9-10.9 4-13.6 1.5-.8 3.1-1.2 4.8-1.2h46c3.8 0 7.2 2.1 8.9 5.5l72.9 144.3 73.2-144.3a10 10 0 0 1 8.9-5.5h45c5.5 0 10 4.5 10 10 .1 1.7-.3 3.3-1.1 4.8z" /></svg>'
```

- Interfaces

```ts
declare function renderIconDefinitionToSVGElement(
  icon: IconDefinition,
  options?: HelperRenderOptions
): string;

interface HelperRenderOptions {
  placeholders?: {
    primaryColor?: string; // default #333
    secondaryColor?: string; // default #E6E6E6
  };
  extraSVGAttrs?: {
    [key: string]: string;
  };
}
```
