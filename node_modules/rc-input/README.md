# rc-input ⌨️

[![NPM version][npm-image]][npm-url] [![npm download][download-image]][download-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![Dependencies][david-image]](david-url) [![DevDependencies][david-dev-image]][david-dev-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-input.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-input
[github-actions-image]: https://github.com/react-component/input/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/input/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/input/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/input/branch/master
[david-url]: https://david-dm.org/react-component/input
[david-image]: https://david-dm.org/react-component/input/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/input?type=dev
[david-dev-image]: https://david-dm.org/react-component/input/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-input.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-input
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-input
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-input

## Install

[![rc-input](https://nodei.co/npm/rc-input.png)](https://npmjs.org/package/rc-input)

## Usage

```js
import Input from 'rc-input';
import { render } from 'react-dom';

render(<Input placeholder="input" allowClear />, mountNode);
```

## API

| Property              | Type                                                                               | Default  | Description                                                                                                                                                         |
| --------------------- | ---------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| prefixCls             | string                                                                             | rc-input |                                                                                                                                                                     |
| className             | string                                                                             | ''       | additional class name of input                                                                                                                                      |
| style                 | React.CSSProperties                                                                |          | style properties of input                                                                                                                                           |
| affixWrapperClassName | string                                                                             | -        | className with 'rc-input-affix-wrapper'                                                                                                                             |
| groupClassName        | string                                                                             | -        | className with 'rc-input-group-wrapper'                                                                                                                             |
| wrapperClassName      | string                                                                             | -        | className with 'rc-input-wrapper'                                                                                                                                   |
| addonAfter            | ReactNode                                                                          | -        | The label text displayed after (on the right side of) the input field                                                                                               |
| addonBefore           | ReactNode                                                                          | -        | The label text displayed before (on the left side of) the input field                                                                                               |
| allowClear            | boolean &#124; { clearIcon: ReactNode }                                                                            | false    | If allow to remove input content with clear icon                                                                                                                    |
| bordered              | boolean                                                                            | true     | Whether has border style                                                                                                                                            |
| defaultValue          | string                                                                             | -        | The initial input content                                                                                                                                           |
| disabled              | boolean                                                                            | false    | Whether the input is disabled                                                                                                                                       |
| id                    | string                                                                             | -        | The ID for input                                                                                                                                                    |
| maxLength             | number                                                                             | -        | The max length                                                                                                                                                      |
| showCount             | boolean &#124; { formatter: ({ value: string, count: number, maxLength?: number }) => ReactNode } | false    | Whether show text count                                                                                                                                             |
| prefix                | ReactNode                                                                          | -        | The prefix icon for the Input                                                                                                                                       |
| suffix                | ReactNode                                                                          | -        | The suffix icon for the Input                                                                                                                                       |
| type                  | string                                                                             | `text`   | The type of input, see: [MDN](https://developer.mozilla.org/docs/Web/HTML/Element/input#Form_%3Cinput%3E_types)( use `Input.TextArea` instead of `type="textarea"`) |
| value                 | string                                                                             | -        | The input content value                                                                                                                                             |
| onChange              | function(e)                                                                        | -        | Callback when user input                                                                                                                                            |
| onPressEnter          | function(e)                                                                        | -        | The callback function that is triggered when Enter key is pressed                                                                                                   |

## inputRef

```tsx | pure
const inputRef = useRef(null);

useEffect(() => {
  inputRef.current.focus();// the input will get focus
  inputRef.current.blur();// the input will lose focus
  console.log(inputRef.current.input);// The origin input element
}, []);
// ....
<Input ref={inputRef} />
```

| Property | Type                                    | Description                       |
| -------- | --------------------------------------- | --------------------------------- |
| focus    | `(options?: InputFocusOptions) => void` | The input get focus when called   |
| blur     | `() => void`                            | The input loses focus when called |
| input    | `HTMLInputElement \| null`              | The origin input element          |



## Development

```
npm install
npm start
```

## License

rc-input is released under the MIT license.
