# rc-tree-select

React TreeSelect Component

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Test coverage][coveralls-image]][coveralls-url] [![Dependencies][david-image]][david-url] [![DevDependencies][david-dev-image]][david-dev-url] [![npm download][download-image]][download-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-tree-select.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-tree-select
[github-actions-image]: https://github.com/react-component/tree-select/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/tree-select/actions
[circleci-image]: https://img.shields.io/circleci/react-component/tree-select/master?style=flat-square
[circleci-url]: https://circleci.com/gh/react-component/tree-select
[coveralls-image]: https://img.shields.io/coveralls/react-component/tree-select.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/react-component/tree-select?branch=master
[david-url]: https://david-dm.org/react-component/tree-select
[david-image]: https://david-dm.org/react-component/tree-select/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/tree-select?type=dev
[david-dev-image]: https://david-dm.org/react-component/tree-select/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-tree-select.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-tree-select
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-tree-select
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-tree-select

## Screenshots

<img src="https://os.alipayobjects.com/rmsportal/HUhyhmpWyiGKnZF.png" width="288"/>


## Development

```
npm install
npm start
```

## Example

http://localhost:8000/examples/

online example: https://tree-select-react-component.vercel.app/

## install

[![rc-tree-select](https://nodei.co/npm/rc-tree-select.png)](https://npmjs.org/package/rc-tree-select)

## API

### TreeSelect props

| name     | description    | type     | default      |
|----------|----------------|----------|--------------|
|className | additional css class of root dom node | String | '' |
|prefixCls | prefix class | String | '' |
|animation | dropdown animation name. only support slide-up now | String | '' |
|transitionName | dropdown css animation name | String | '' |
|choiceTransitionName | css animation name for selected items at multiple mode | String | '' |
|dropdownMatchSelectWidth | whether dropdown's with is same with select. Default set `min-width` same as input | bool | true |
|dropdownClassName | additional className applied to dropdown | String | - |
|dropdownStyle | additional style applied to dropdown | Object | {} |
|dropdownPopupAlign | specify alignment for dropdown (alignConfig of [dom-align](https://github.com/yiminghe/dom-align)) | Object | - |
|onDropdownVisibleChange | control dropdown visible | function | `() => { return true; }` |
|notFoundContent | specify content to show when no result matches. | String | 'Not Found' |
|showSearch | whether show search input in single mode | bool | true |
|allowClear | whether allowClear | bool | false |
|maxTagTextLength | max tag text length to show | number | - |
|maxTagCount | max tag count to show | number | - |
|maxTagPlaceholder | placeholder for omitted values | ReactNode/function(omittedValues) | - |
|multiple | whether multiple select (true when enable treeCheckable) | bool | false |
|disabled | whether disabled select | bool | false |
|searchValue | work with `onSearch` to make search value controlled. | string | '' |
|defaultValue | initial selected treeNode(s) | same as value type | - |
|value | current selected treeNode(s). | normal: String/Array<String>. labelInValue: {value:String,label:React.Node}/Array<{value,label}>. treeCheckStrictly(halfChecked default false): {value:String,label:React.Node, halfChecked}/Array<{value,label,halfChecked}>. | - |
|labelInValue| whether to embed label in value, see above value type | Bool | false |
|onChange | called when select treeNode or input value change | function(value, label(null), extra) | - |
|onSelect | called when select treeNode | function(value, node, extra) | - |
|onSearch | called when input changed | function | - |
|onTreeExpand | called when tree node expand | function(expandedKeys) | - |
|showCheckedStrategy | `TreeSelect.SHOW_ALL`: show all checked treeNodes (Include parent treeNode). `TreeSelect.SHOW_PARENT`: show checked treeNodes (Just show parent treeNode). Default just show child. | enum{TreeSelect.SHOW_ALL, TreeSelect.SHOW_PARENT, TreeSelect.SHOW_CHILD } | TreeSelect.SHOW_CHILD |
|treeIcon | show tree icon | bool | false |
|treeLine | show tree line | bool | false |
|treeDefaultExpandAll | default expand all treeNode | bool | false |
|treeDefaultExpandedKeys | default expanded treeNode keys | Array<String> | - |
|treeExpandedKeys | set tree expanded keys | Array<String> | - |
|treeExpandAction | Tree open logic, optional: false \| `click` \| `doubleClick`, same as `expandAction` of `rc-tree` | string \| boolean | `click` |
|treeCheckable | whether tree show checkbox (select callback will not fire) | bool | false |
|treeCheckStrictly | check node precisely, parent and children nodes are not associated| bool | false |
|filterTreeNode | whether filter treeNodes by input value. default filter by treeNode's treeNodeFilterProp prop's value | bool/Function(inputValue:string, treeNode:TreeNode) | Function |
|treeNodeFilterProp | which prop value of treeNode will be used for filter if filterTreeNode return true | String | 'value' |
|treeNodeLabelProp | which prop value of treeNode will render as content of select | String | 'title' |
|treeData | treeNodes data Array, if set it then you need not to construct children TreeNode. (value should be unique across the whole array) | array<{value,label,children, [disabled,selectable]}> | [] |
|treeDataSimpleMode | enable simple mode of treeData.(treeData should be like this: [{id:1, pId:0, value:'1', label:"test1",...},...], `pId` is parent node's id) | bool/object{id:'id', pId:'pId', rootPId:null} | false |
|loadData | load data asynchronously | function(node) | - |
|getPopupContainer | container which popup select menu rendered into | function(trigger:Node):Node | function(){return document.body;} |
|autoClearSearchValue | auto clear search input value when multiple select is selected/deselected | boolean | true |
| suffixIcon | specify the select arrow icon | ReactNode \| (props: TreeProps) => ReactNode | - |
| clearIcon | specify the clear icon | ReactNode \| (props: TreeProps) => ReactNode | - |
| removeIcon | specify the remove icon | ReactNode \| (props: TreeProps) => ReactNode | - |
|switcherIcon| specify the switcher icon | ReactNode \| (props: TreeProps) => ReactNode | - |
|virtual| Disable virtual when `false` | false | - |


### TreeNode props
> note: you'd better to use `treeData` instead of using TreeNode.

| name     | description    | type     | default      |
|----------|----------------|----------|--------------|
|disabled | disable treeNode | bool | false |
|key | it's value must be unique across the tree's all TreeNode, you must set it  | String | - |
|value | default as treeNodeFilterProp (be unique across the tree's all TreeNode) | String | '' |
|title | tree/subTree's title | String/element | '---' |
|isLeaf | whether it's leaf node | bool | false |


## note
1. Optimization tips(when there are large amounts of data, like more than 5000 nodes)
    - Do not Expand all nodes.
    - Recommend not exist many `TreeSelect` components in a page at the same time.
    - Recommend not use `treeCheckable` mode, or use `treeCheckStrictly`.
2. In `treeCheckable` mode, It has the same effect when click `x`(node in Selection box) or uncheck in the treeNode(in dropdown panel), but the essence is not the same. So, even if both of them trigger `onChange` method, but the parameters (the third parameter) are different. （中文：在`treeCheckable`模式下，已选择节点上的`x`删除操作、和相应 treeNode 节点上 checkbox 的 uncheck 操作，最终效果相同，但本质不一样。前者跟弹出的 tree 组件可以“毫无关系”（例如 dropdown 没展开过，tree 也就没渲染好），而后者是 tree 组件上的节点 uncheck 事件。所以、即便两者都会触发`onChange`方法、但它们的参数（第三个参数）是不同的。）

## Test Case

http://localhost:8000/tests/runner.html?coverage

## Coverage

http://localhost:8000/node_modules/rc-server/node_modules/node-jscover/lib/front-end/jscoverage.html?w=http://localhost:8000/tests/runner.html?coverage

## License

rc-tree-select is released under the MIT license.
