# rc-collapse

rc-collapse ui component for react

[![NPM version][npm-image]][npm-url] [![build status][github-actions-image]][github-actions-url] [![Test coverage][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/rc-collapse.svg?style=flat-square
[npm-url]: http://npmjs.org/package/rc-collapse
[github-actions-image]: https://github.com/react-component/collapse/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/collapse/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/collapse/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/collapse
[download-image]: https://img.shields.io/npm/dm/rc-collapse.svg?style=flat-square
[download-url]: https://npmjs.org/package/rc-collapse

## Live Demo

http://react-component.github.io/collapse/

## Install

[![rc-collapse](https://nodei.co/npm/rc-collapse.png)](https://npmjs.org/package/rc-collapse)

## Usage

```js
var Collapse = require('rc-collapse');
var Panel = Collapse.Panel;
var React = require('react');
var ReactDOM = require('react-dom');
require('rc-collapse/assets/index.css');

var App = (
  <Collapse accordion={true}>
    <Panel header="hello" headerClass="my-header-class">
      this is panel content
    </Panel>
    <Panel header="title2">this is panel content2 or other</Panel>
  </Collapse>
);
ReactDOM.render(App, container);
```

## Features

- support ie8,ie8+,chrome,firefox,safari

## API

### Collapse props

<table class="table table-bordered table-striped">
    <thead>
    <tr>
        <th style="width: 100px;">name</th>
        <th style="width: 50px;">type</th>
        <th>default</th>
        <th>description</th>
    </tr>
    </thead>
    <tbody>
      <tr>
          <td>activeKey</td>
          <td>String|Array<String></td>
          <th>The first panel key</th>
          <td>current active Panel key</td>
      </tr>
      <tr>
        <td>className</td>
        <td>String or object</td>
        <th></th>
        <td>custom className to apply</td>
      </tr>
      <tr>
          <td>defaultActiveKey</td>
          <td>String|Array<String></td>
          <th>null</th>
          <td>default active key</td>
      </tr>
      <tr>
          <td>destroyInactivePanel</td>
          <td>Boolean</td>
          <th>false</th>
          <td>If destroy the panel which not active, default false. </td>
      </tr>
      <tr>
          <td>accordion</td>
          <td>Boolean</td>
          <th>false</th>
          <td>accordion mode, default is null, is collapse mode</td>
      </tr>
      <tr>
          <td>onChange</td>
          <td>Function(key)</td>
          <th>noop</th>
          <td>called when collapse Panel is changed</td>
      </tr>
      <tr>
          <td>expandIcon</td>
          <td>(props: PanelProps) => ReactNode</td>
          <th></th>
          <td>specific the custom expand icon.</td>
      </tr>
      <tr>
          <td>collapsible</td>
          <td>'header' | 'icon' | 'disabled'</td>
          <th>-</th>
          <td>specify whether the panel of children is collapsible or the area of collapsible.</td>
      </tr>
      <tr>
          <td>items</td>
          <td>
            <a href="./src/interface.ts#ItemType">interface.ts#ItemType</a>
          </td>
          <th>-</th>
          <td>collapse items content</td>
      </tr>
    </tbody>
</table>

If `accordion` is null or false, every panel can open. Opening another panel will not close any of the other panels. `activeKey` should be an string, if passing an array (the first item in the array will be used).

If `accordion` is true, only one panel can be open. Opening another panel will cause the previously opened panel to close. `activeKey` should be an string, if passing an array (the first item in the array will be used).

### Collapse.Panel props

> **deprecated** use `items` instead, will be removed in `v4.0.0`

<table class="table table-bordered table-striped">
    <thead>
    <tr>
        <th style="width: 100px;">name</th>
        <th style="width: 50px;">type</th>
        <th>default</th>
        <th>description</th>
    </tr>
    </thead>
    <tbody>
      <tr>
          <td>header</td>
          <td>String or node</td>
          <th></th>
          <td>header content of Panel</td>
      </tr>
      <tr>
          <td>headerClass</td>
          <td>String</td>
          <th>' '</th>
          <td>custom className to apply to header</td>
      </tr>
      <tr>
          <td>showArrow</td>
          <td>boolean</td>
          <th>true</th>
          <td>show arrow beside header</td>
      </tr>
      <tr>
        <td>className</td>
        <td>String or object</td>
        <th></th>
        <td>custom className to apply</td>
      </tr>
      <tr>
        <td>style</td>
        <td>object</td>
        <th></th>
        <td>custom style</td>
      </tr>
      <tr>
        <td>openMotion</td>
        <td>object</td>
        <th></th>
        <td>set the animation of open behavior, [more](https://github.com/react-component/motion). Different with v2, closed pane use a `rc-collapse-content-hidden` class to set `display: none` for hidden.</td>
      </tr>
      <tr>
        <td>forceRender</td>
        <td>boolean</td>
        <th>false</th>
        <td>forced render of content in panel, not lazy render after clicking on header</td>
      </tr>
      <tr>
          <td>extra</td>
          <td>String | ReactNode</td>
          <th></th>
          <td>Content to render in the right of the panel header</td>
      </tr>
      <tr>
          <td>collapsible</td>
          <td>'header' | 'icon' | 'disabled'</td>
          <th>-</th>
          <td>specify whether the panel be collapsible or the area of collapsible.</td>
      </tr>
    </tbody>
</table>

> `disabled` is removed since 3.0.0, please use `collapsible=disabled` replace it.

#### key

If `key` is not provided, the panel's index will be used instead.

#### KeyBoard Event

By default, Collapse will listen `onKeyDown`(<3.7.0 `onKeyPress`) event with `enter` key to toggle panel's active state when `collapsible` is not `disabled`. If you want to disable this behavior, you can prevent the event from bubbling like this:

```jsx
const App = () => {
  const items: CollapseProps['items'] = [
    {
      label: <input onKeyDown={(e) => e.stopPropagation()} />,
      children: 'content',
    },
    {
      label: (
        <div onKeyDown={(e) => e.stopPropagation()}>
          <CustomComponent />
        </div>
      ),
      children: 'content',
    },
    {
      label: 'title 2',
      children: 'content 2',
      collapsible: 'disabled',
    },
    {
      label: 'title 3',
      children: 'content 3',
      onItemClick: console.log,
    },
  ];

  return <Collapse items={items} />;
};
```

## Development

```bash
npm install
npm start
```

## Test Case

```bash
npm test
```

## Coverage

```bash
npm test -- --coverage
```

## License

rc-collapse is released under the MIT license.
