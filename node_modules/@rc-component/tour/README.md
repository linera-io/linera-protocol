# @rc-component/tour

React 18 supported Tour Component.

[![NPM version][npm-image]][npm-url] [![dumi](https://img.shields.io/badge/docs%20by-dumi-blue?style=flat-square)](https://github.com/umijs/dumi) [![build status][github-actions-image]][github-actions-url] [![Codecov][codecov-image]][codecov-url] [![npm download][download-image]][download-url]

[npm-image]: http://img.shields.io/npm/v/@rc-component/tour.svg?style=flat-square
[npm-url]: http://npmjs.org/package/@rc-component/tour
[github-actions-image]: https://github.com/react-component/tour/workflows/CI/badge.svg
[github-actions-url]: https://github.com/react-component/tour/actions
[codecov-image]: https://img.shields.io/codecov/c/github/react-component/tour/master.svg?style=flat-square
[codecov-url]: https://app.codecov.io/gh/react-component/tour
[download-image]: https://img.shields.io/npm/dm/@rc-component/tour.svg?style=flat-square
[download-url]: https://npmjs.org/package/@rc-component/tour

## Development

```bash
npm install
npm start
open http://localhost:8000
```

## Feature

- React life cycle support tour component

## Install

[![@rc-component/tour](https://nodei.co/npm/@rc-component/tour.png)](https://www.npmjs.com/package/@rc-component/tour)

## Usage

<!-- prettier-ignore -->
```js | pure
import Tour from '@rc-component/tour';

const Demo = () => {
    const createBtnRef = useRef<HTMLButtonElement>(null);
    const updateBtnRef = useRef<HTMLButtonElement>(null);
    const deleteBtnRef = useRef<HTMLButtonElement>(null);
    return (
        <div style={{ margin: 20 }}>
            <div>
                <button
                    className="ant-target"
                    ref={createBtnRef}
                    style={{ marginLeft: 100 }}
                >
                    Create
                </button>
                <div style={{ height: 200 }} />
                <button className="ant-target" ref={updateBtnRef}>
                    Update
                </button>
                <button className="ant-target" ref={deleteBtnRef}>
                    Delete
                </button>
            </div>

            <div style={{ height: 200 }} />

            <Tour
                defaultCurrent={0}
                steps={[
                    {
                        title: '创建',
                        description: '创建一条数据',
                        target: () => createBtnRef.current,
                        mask: true,
                    },
                    {
                        title: '更新',
                        description: (
                            <div>
                                <span>更新一条数据</span>
                                <button>帮助文档</button>
                            </div>
                        ),
                        target: () => updateBtnRef.current,
                    },
                    {
                        title: '删除',
                        description: (
                            <div>
                                <span>危险操作：删除一条数据</span>
                                <button>帮助文档</button>
                            </div>
                        ),
                        target: () => deleteBtnRef.current,
                        mask: true,
                        style: { color: 'red' },
                    },
                ]}
            />
        </div>
    );
};

export default Demo;
```

## 🔥 API

We use typescript to create the Type definition. You can view directly in IDE. But you can still check the type definition [here](https://github.com/react-component/tour/blob/master/src/interface.ts).

### Tour

<!-- prettier-ignore -->
| 属性 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| closeIcon | `React.ReactNode` | - | 自定义关闭按钮 |
| steps | `TourStepProps[]` | - | 引导步骤 |
| open | `boolean` | `true` | 受控打开引导（与 `current` 受控分开） |
| current | `number` | 0 | 受控当前处于哪一步 |
| defaultCurrent | `number` | 0 | 默认处于哪一步 |
| gap | `{ offset?: number \| [number, number]; radius?: number }` | - | 控制引导显示间距 |
| onChange | `(current: number) => void` | - | 步骤改变时的回调，`current`为改变前的步骤，`next`为改变后的步骤 |
| onClose | `(current: number) => void` | - | 关闭引导时的回调 |
| onFinish | `() => void` | - | 完成引导时的回调 |
| mask | `boolean \| { style?: React.CSSProperties; color?: string; }` | `true` | 整体是否启用蒙层，也可以传入自定义样式修改蒙层样式 |
| animated | `boolean \| { placeholder: boolean }` | `false` | 是否启用目标遮罩动画 |
| arrow | `boolean \| { pointAtCenter: boolean}` | `true` | 整体是否显示箭头，包含是否指向元素中心的配置 |
| scrollIntoViewOptions | `boolean \| ScrollIntoViewOptions` | `true` | 是否支持当前元素滚动到视窗内，也可传入配置指定滚动视窗的相关参数 |
| onPopupAlign | `function(popupDomNode, align)` | - | 当弹出框对齐后回调 |
| zIndex | `number` | 1001 | 弹层的层级 |

### TourStep

<!-- prettier-ignore -->
| 属性 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| closeIcon | `React.ReactNode` | - | 自定义关闭按钮 |
| target | `() => HTMLElement` \|  `HTMLElement` | - | 获取引导卡片指向的元素 |
| arrow | `boolean` \| `{ pointAtCenter: boolean}` | `true` | 是否显示箭头，包含是否指向元素中心的配置 |
| placement | `left` \| `leftTop` \| `leftBottom` \| `right` \| `rightTop` \| `rightBottom` \| `top` \| `topLeft` \| `topRight` \| `bottom`  \| `bottomLeft` \| `bottomRight` | `bottom` | 引导卡片相对于目标元素的位置 |
| onClose | `() => void` | - | 关闭引导时的回调函数 |
| mask | `boolean \| { style?: React.CSSProperties; color?: string; }` | `true` | 是否启用蒙层，也可以传入自定义样式修改蒙层样式，默认跟随 Tour 的 `mask` 属性 |
| renderPanel | `(props: TourStepProps, current: number) => ReactNode;` |  | 渲染 popoup 弹窗方法 |
| className | `string` | - | - |
| style | `React.CSSProperties` | - | - |
| scrollIntoViewOptions | `boolean \| ScrollIntoViewOptions` | `true` | 是否支持当前元素滚动到视窗内，也可传入配置指定滚动视窗的相关参数，默认跟随 Tour 的 `scrollIntoViewOptions` 属性 |
