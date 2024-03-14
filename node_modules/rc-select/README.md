# rc-select

---

React Select

<!-- prettier-ignore -->
[![NPM version][npm-image]][npm-url]
[![npm download][download-image]][download-url]
[![build status][github-actions-image]][github-actions-url]
[![Codecov][codecov-image]][codecov-url]
[![bundle size][bundlephobia-image]][bundlephobia-url]
[![dumi][dumi-image]][dumi-url]

[npm-image]: http://img.shields.io/npm/v/rc-select.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-select
[travis-image]: https://img.shields.io/travis/react-component/select/master?style=flat-square
[travis-url]: https://travis-ci.com/react-component/select
[github-actions-image]: https://github.com/react-component/select/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/select/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/select/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/select
[david-url]: https://david-dm.org/react-component/select
[david-image]: https://david-dm.org/react-component/select/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/select?type=dev
[david-dev-image]: https://david-dm.org/react-component/select/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-select.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-select
[bundlephobia-url]: https://bundlephobia.com/package/rc-select
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-select
[dumi-url]: https://github.com/umijs/dumi
[dumi-image]: https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square

## Screenshots

<img src="https://gw.alipayobjects.com/zos/antfincdn/d13eUZlgdJ/tupian.png" />

## Feature

- support IE11+,Chrome,Firefox,Safari

### Keyboard

- Open select (focus input || focus and click)
- KeyDown/KeyUp/Enter to navigate menu

## install

[![rc-select](https://nodei.co/npm/rc-select.png)](https://npmjs.org/package/rc-select)

## Usage

### basic use

```jsx | pure
import Select, { Option } from 'rc-select';
import 'rc-select/assets/index.css';

export default () => (
  <Select>
    <Option value="jack">jack</Option>
    <Option value="lucy">lucy</Option>
    <Option value="yiminghe">yiminghe</Option>
  </Select>
);
```

## API

### Select props

<!-- prettier-ignore -->
| name | description | type | default |
| --- | --- | --- | --- |
| id | html id to set on the component wrapper | String | '' |
| className | additional css class of root dom node | String | '' |
| data-\* | html data attributes to set on the component wrapper | String | '' |
| prefixCls | prefix class | String | '' |
| animation | dropdown animation name. only support slide-up now | String | '' |
| transitionName | dropdown css animation name | String | '' |
| choiceTransitionName | css animation name for selected items at multiple mode | String | '' |
| dropdownMatchSelectWidth | whether dropdown's width is same with select | boolean | true |
| dropdownClassName | additional className applied to dropdown | String | - |
| dropdownStyle | additional style applied to dropdown | React.CSSProperties | {} |
| dropdownAlign | additional align applied to dropdown | [AlignType](https://github.com/react-component/trigger/blob/728d7e92394aa4b3214650f743fc47e1382dfa68/src/interface.ts#L25-L80) | {} |
| dropdownMenuStyle | additional style applied to dropdown menu | Object | React.CSSProperties |
| notFoundContent | specify content to show when no result matches. | ReactNode | 'Not Found' |
| tokenSeparators | separator used to tokenize on tag/multiple mode | string[]? |  |
| open | control select open | boolean |  |
| defaultOpen | control select default open | boolean |  |
| placeholder | select placeholder | React Node |  |
| showSearch | whether show search input in single mode | boolean | true |
| allowClear | whether allowClear | boolean | { clearIcon?: ReactNode } | false |
| tags | when tagging is enabled the user can select from pre-existing options or create a new tag by picking the first choice, which is what the user has typed into the search box so far. | boolean | false |
| tagRender | render custom tags. | (props: CustomTagProps) => ReactNode | - |
| maxTagTextLength | max tag text length to show | number | - |
| maxTagCount | max tag count to show | number | - |
| maxTagPlaceholder | placeholder for omitted values | ReactNode/function(omittedValues) | - |
| combobox | enable combobox mode(can not set multiple at the same time) | boolean | false |
| multiple | whether multiple select | boolean | false |
| disabled | whether disabled select | boolean | false |
| filterOption | whether filter options by input value. default filter by option's optionFilterProp prop's value | boolean | true/Function(inputValue:string, option:Option) |
| optionFilterProp | which prop value of option will be used for filter if filterOption is true | String | 'value' |
| filterSort | Sort function for search options sorting, see [Array.sort](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/sort)'s compareFunction. | Function(optionA:Option, optionB: Option) | - |
| optionLabelProp | render option value or option children as content of select | String: 'value'/'children' | 'value' |
| defaultValue | initial selected option(s) | String \| String[] | - |
| value | current selected option(s) | String \| String[] \| {key:String, label:React.Node} \| {key:String, label:React.Node}[] | - |
| labelInValue | whether to embed label in value, see above value type. Not support `combobox` mode | boolean | false |
| backfill | whether backfill select option to search input (Only works in single and combobox mode) | boolean | false |
| onChange | called when select an option or input value change(combobox) | function(value, option:Option \| Option[]) | - |
| onSearch | called when input changed | function | - |
| onBlur | called when blur | function | - |
| onFocus | called when focus | function | - |
| onPopupScroll | called when menu is scrolled | function | - |
| onSelect | called when a option is selected. param is option's value and option instance | Function(value, option:Option) | - |
| onDeselect | called when a option is deselected. param is option's value. only called for multiple or tags | Function(value, option:Option) | - |
| onInputKeyDown | called when key down on input | Function(event) | - |
| defaultActiveFirstOption | whether active first option by default | boolean | true |
| getPopupContainer | container which popup select menu rendered into | function(trigger:Node):Node | function(){return document.body;} |
| getInputElement | customize input element | function(): Element | - |
| showAction | actions trigger the dropdown to show | String[]? | - |
| autoFocus | focus select after mount | boolean | - |
| autoClearSearchValue | auto clear search input value when multiple select is selected/deselected | boolean | true |
| suffixIcon | specify the select arrow icon | ReactNode | - |
| clearIcon | specify the clear icon | ReactNode | - |
| removeIcon | specify the remove icon | ReactNode | - |
| menuItemSelectedIcon | specify the item selected icon | ReactNode \| (props: MenuItemProps) => ReactNode | - |
| dropdownRender | render custom dropdown menu | (menu: React.Node) => ReactNode | - |
| loading | show loading icon in arrow | boolean | false |
| virtual | Disable virtual scroll | boolean | true |
| direction | direction of dropdown | 'ltr' \| 'rtl' | 'ltr' |
| optionRender | Custom rendering options | (oriOption: FlattenOptionData\<BaseOptionType\> , info: { index: number }) => React.ReactNode | - |
| labelRender  | Custom rendering label   |  (props: LabelInValueType) => React.ReactNode   | - |
| maxCount | The max number of items can be selected | number | - |

### Methods

| name  | description               | parameters | return |
| ----- | ------------------------- | ---------- | ------ |
| focus | focus select programmably | -          | -      |
| blur  | blur select programmably  | -          | -      |

### Option props

| name | description | type | default |
| --- | --- | --- | --- |
| className | additional class to option | String | '' |
| disabled | no effect for click or keydown for this item | boolean | false |
| key | if react want you to set key, then key is same as value, you can omit value | String/number | - |
| value | default filter by this attribute. if react want you to set key, then key is same as value, you can omit value | String/number | - |
| title | if you are not satisfied with auto-generated `title` which is show while hovering on selected value, you can customize it with this property | String | - |

### OptGroup props

| name | description | type | default |
| --- | --- | --- | --- |
| label | group label | String/React.Element | - |
| key | - | String | - |
| value | default filter by this attribute. if react want you to set key, then key is same as value, you can omit value | String | - |
| className | same as `Option props` | String | '' |
| title | same as `Option props`| String | - |

## Development

```
npm install
npm start
```

## Example

local example: http://localhost:9001/

online example: http://select.react-component.now.sh/

## Test Case

```
npm test
```

## Coverage

```
npm run coverage
```

## License

rc-select is released under the MIT license.
