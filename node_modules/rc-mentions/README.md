# rc-mentions

[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-mentions.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-mentions
[travis-image]: https://img.shields.io/travis/react-component/mentions/master?style=flat-square
[travis-url]: https://travis-ci.com/react-component/mentions
[github-actions-image]: https://github.com/react-component/mentions/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/mentions/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/mentions/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/mentions
[david-url]: https://david-dm.org/react-component/mentions
[david-image]: https://david-dm.org/react-component/mentions/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/mentions?type=dev
[david-dev-image]: https://david-dm.org/react-component/mentions/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-mentions.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-mentions
[bundlephobia-url]: https://bundlephobia.com/package/rc-mentions
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-mentions
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Screenshots

<img src="https://user-images.githubusercontent.com/5378891/57270992-2fd48780-70c0-11e9-91ae-c614d0b49a45.png" />

## Feature

- support ie9,ie9+,chrome,firefox,safari

### Keyboard

- Open mentions (focus input || focus and click)
- KeyDown/KeyUp/Enter to navigate menu

## install

[![rc-mentions](https://nodei.co/npm/rc-mentions.png)](https://npmjs.org/package/rc-mentions)

## Usage

### basic use

```js
/**
 * inline: true
 */
import Mentions from 'rc-mentions';

const { Option } = Mentions;

var Demo = (
  <Mentions>
    <Option value="light">Light</Option>
    <Option value="bamboo">Bamboo</Option>
    <Option value="cat">Cat</Option>
  </Mentions>
);
React.render(<Demo />, container);
```

## API

### Mentions props

| name              | description                                                                                             | type                                                       | default     |
| ----------------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ----------- |
| autoFocus         | Auto get focus when component mounted                                                                   | `boolean`                                                  | `false`     |
| defaultValue      | Default value                                                                                           | `string`                                                   | -           |
| filterOption      | Customize filter option logic                                                                           | `false \| (input: string, option: OptionProps) => boolean` | -           |
| notFoundContent   | Set mentions content when not match                                                                     | `ReactNode`                                                | 'Not Found' |
| placement         | Set popup placement                                                                                     | `'top' \| 'bottom'`                                        | 'bottom'    |
| direction         | Set popup direction                                                                                     | `'ltr' \| 'rtl'`                                           | 'ltr'       |
| prefix            | Set trigger prefix keyword                                                                              | `string \| string[]`                                       | '@'         |
| rows              | Set row count                                                                                           | `number`                                                   | 1           |
| split             | Set split string before and after selected mention                                                      | `string`                                                   | ' '         |
| validateSearch    | Customize trigger search logic                                                                          | `(text: string, props: MentionsProps) => void`             | -           |
| value             | Set value of mentions                                                                                   | `string`                                                   | -           |
| onChange          | Trigger when value changed                                                                              | `(text: string) => void`                                   | -           |
| onKeyDown         | Trigger when user hits a key                                                                            | `React.KeyboardEventHandler<HTMLTextAreaElement>`          | -           |
| onKeyUp           | Trigger when user releases a key                                                                        | `React.KeyboardEventHandler<HTMLTextAreaElement>`          | -           |
| onSelect          | Trigger when user select the option                                                                     | `(option: OptionProps, prefix: string) => void`            | -           |
| onSearch          | Trigger when prefix hit                                                                                 | `(text: string, prefix: string) => void`                   | -           |
| onFocus           | Trigger when mentions get focus                                                                         | `React.FocusEventHandler<HTMLTextAreaElement>`             | -           |
| onBlur            | Trigger when mentions lose focus                                                                        | `React.FocusEventHandler<HTMLTextAreaElement>`             | -           |
| getPopupContainer | DOM Container for suggestions                                                                           | `() => HTMLElement`                                        | -           |
| autoSize          | Textarea height autosize feature, can be set to `true\|false` or an object `{ minRows: 2, maxRows: 6 }` | `boolean \| object`                                        | -           |
| onPressEnter      | The callback function that is triggered when Enter key is pressed                                       | `function(e)`                                              | -           |
| onResize          | The callback function that is triggered when textarea resize                                            | `function({ width, height })`                              | -           |

### Methods

| name    | description          |
| ------- | -------------------- |
| focus() | Component get focus  |
| blur()  | Component lose focus |

## Development

```
npm install
npm start
```

## Example

http://localhost:9001/

online example: http://react-component.github.io/mentions/

## Test Case

```
npm test
```

## Coverage

```
npm run coverage
```

## License

rc-mentions is released under the MIT license.
