# rc-table

React table component with useful functions.

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Test coverage][codecov-image]][codecov-url] [![Dependencies][david-image]][david-url] [![DevDependencies][david-dev-image]][david-dev-url] [![npm download][download-image]][download-url] [![bundle size][bundlephobia-image]][bundlephobia-url]

[npm-image]: http://img.shields.io/npm/v/rc-table.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-table
[github-actions-image]: https://github.com/react-component/table/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/table/actions
[coveralls-image]: https://img.shields.io/coveralls/react-component/table.svg?style=flat-square
[coveralls-url]: https://coveralls.io/r/react-component/table?branch=master
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/table/master.svg?style=flat-square
[codecov-url]: https://codecov.io/gh/react-component/table/branch/master
[david-url]: https://david-dm.org/react-component/table
[david-image]: https://david-dm.org/react-component/table/status.svg?style=flat-square
[david-dev-url]: https://david-dm.org/react-component/table?type=dev
[david-dev-image]: https://david-dm.org/react-component/table/dev-status.svg?style=flat-square
[download-image]: https://img.shields.io/npm/dm/rc-table.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-table
[bundlephobia-url]: https://bundlephobia.com/result?p=rc-table
[bundlephobia-image]: https://badgen.net/bundlephobia/minzip/rc-table

## install

[![rc-table](https://nodei.co/npm/rc-table.png)](https://npmjs.org/package/rc-table)

## Development

```
npm install
npm start
```

## Example

https://table-react-component.vercel.app/

## Usage

```js
import Table from 'rc-table';

const columns = [
  {
    title: 'Name',
    dataIndex: 'name',
    key: 'name',
    width: 100,
  },
  {
    title: 'Age',
    dataIndex: 'age',
    key: 'age',
    width: 100,
  },
  {
    title: 'Address',
    dataIndex: 'address',
    key: 'address',
    width: 200,
  },
  {
    title: 'Operations',
    dataIndex: '',
    key: 'operations',
    render: () => <a href="#">Delete</a>,
  },
];

const data = [
  { name: 'Jack', age: 28, address: 'some where', key: '1' },
  { name: 'Rose', age: 36, address: 'some where', key: '2' },
];

React.render(<Table columns={columns} data={data} />, mountNode);
```

## API

### Properties

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| tableLayout | `auto` \| `fixed` | `auto` \| `fixed` for any columns is fixed or ellipsis or header is fixed | https://developer.mozilla.org/en-US/docs/Web/CSS/table-layout |
| prefixCls | String | rc-table |  |
| className | String |  | additional className |
| id | String |  | identifier of the container div |
| useFixedHeader | Boolean | false | whether use separator table for header. better set width for columns |
| scroll | Object | {x: false, y: false} | whether table can be scroll in x/y direction, `x` or `y` can be a number that indicated the width and height of table body |
| expandable | Object |  | Config expand props |
| expandable.defaultExpandAllRows | Boolean | false | Expand All Rows initially |
| expandable.defaultExpandedRowKeys | String[] | [] | initial expanded rows keys |
| expandable.expandedRowKeys | String[] |  | current expanded rows keys |
| expandable.expandedRowRender | Function(recode, index, indent, expanded):ReactNode |  | Content render to expanded row |
| expandable.expandedRowClassName | Function(recode, index, indent):string |  | get expanded row's className |
| expandable.expandRowByClick | boolean |  | Support expand by click row |
| expandable.expandIconColumnIndex | Number | 0 | The index of expandIcon which column will be inserted when expandIconAsCell is false |
| expandable.expandIcon | props => ReactNode |  | Customize expand icon |
| expandable.indentSize | Number | 15 | indentSize for every level of data.i.children, better using with column.width specified |
| expandable.rowExpandable | (record) => boolean |  | Config row support expandable |
| expandable.onExpand | Function(expanded, record) |  | function to call when click expand icon |
| expandable.onExpandedRowsChange | Function(expandedRows) |  | function to call when the expanded rows change |
| expandable.fixed | String \| Boolean | - | this expand icon will be fixed when table scroll horizontally: true or `left` or `right` and `expandIconColumnIndex` need to stay first or last |
| rowKey | string or Function(record, index):string | 'key' | If rowKey is string, `record[rowKey]` will be used as key. If rowKey is function, the return value of `rowKey(record, index)` will be use as key. |
| rowClassName | string or Function(record, index, indent):string |  | get row's className |
| rowRef | Function(record, index, indent):string |  | get row's ref key |
| data | Object[] |  | data record array to be rendered |
| onRow | Function(record, index) |  | Set custom props per each row. |
| onHeaderRow | Function(record, index) |  | Set custom props per each header row. |
| showHeader | Boolean | true | whether table head is shown |
| hidden | Boolean | `false` | Hidden column. |
| title | Function(currentData) |  | table title render function |
| footer | Function(currentData) |  | table footer render function |
| emptyText | React.Node or Function | `No Data` | Display text when data is empty |
| columns | Object[] |  | The columns config of table, see table below |
| components | Object |  | Override table elements, see [#171](https://github.com/react-component/table/pull/171) for more details |
| sticky | boolean \| {offsetHeader?: number, offsetScroll?: number, getContainer?: () => Window \| HTMLElement } | false | stick header and scroll bar |
| summary | (data: readonly RecordType[]) => React.ReactNode | - | `summary` attribute in `table` component is used to define the summary row. |

## Column Props

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| key | String |  | key of this column |
| className | String |  | className of this column |
| colSpan | Number |  | thead colSpan of this column |
| title | React Node |  | title of this column |
| dataIndex | String |  | display field of the data record |
| width | String \| Number |  | width of the specific proportion calculation according to the width of the columns |
| fixed | String \| Boolean |  | this column will be fixed when table scroll horizontally: true or 'left' or 'right' |
| align | String |  | specify how cell content is aligned |
| ellipsis | Boolean |  | specify whether cell content be ellipsized |
| rowScope | 'row' \| 'rowgroup' |  | Set scope attribute for all cells in this column |
| onCell | Function(record, index) |  | Set custom props per each cell. |
| onHeaderCell | Function(record) |  | Set custom props per each header cell. |
| render | Function(value, row, index) |  | The render function of cell, has three params: the text of this cell, the record of this row, the index of this row, it's return an object:{ children: value, props: { colSpan: 1, rowSpan:1 } } ==> 'children' is the text of this cell, props is some setting of this cell, eg: 'colspan' set td colspan, 'rowspan' set td rowspan |

## Summary Props

### Table.Summary

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| key | String |  | key of this summary |
| fixed | boolean \| 'top' \| 'bottom' | - | `true` fixes the summary row at the bottom of the table. `top` fixes the summary row at the top of the table, while `bottom` fixes it at the bottom. `undefined` or `false` makes the summary row scrollable along with the table. |

### Table.Summary.Row

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| key | String |  | key of this summary |
| className | String | - | className of this summary row |
| style | React.CSSProperties | - | style of this summary row |
| onClick | (e?: React.MouseEvent\<HTMLElement>) => void | - | The `onClick` attribute in `Table.Summary.Row` component can be used to set a click event handler for the summary row. |

## License

rc-table is released under the MIT license.
