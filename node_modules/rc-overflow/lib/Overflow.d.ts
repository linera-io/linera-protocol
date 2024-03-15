import * as React from 'react';
import type { ComponentType } from './RawItem';
import RawItem from './RawItem';
declare const RESPONSIVE: "responsive";
declare const INVALIDATE: "invalidate";
export { OverflowContext } from './context';
export type { ComponentType } from './RawItem';
export interface OverflowProps<ItemType> extends React.HTMLAttributes<any> {
    prefixCls?: string;
    className?: string;
    style?: React.CSSProperties;
    data?: ItemType[];
    itemKey?: React.Key | ((item: ItemType) => React.Key);
    /** Used for `responsive`. It will limit render node to avoid perf issue */
    itemWidth?: number;
    renderItem?: (item: ItemType) => React.ReactNode;
    /** @private Do not use in your production. Render raw node that need wrap Item by developer self */
    renderRawItem?: (item: ItemType, index: number) => React.ReactElement;
    maxCount?: number | typeof RESPONSIVE | typeof INVALIDATE;
    renderRest?: React.ReactNode | ((omittedItems: ItemType[]) => React.ReactNode);
    /** @private Do not use in your production. Render raw node that need wrap Item by developer self */
    renderRawRest?: (omittedItems: ItemType[]) => React.ReactElement;
    suffix?: React.ReactNode;
    component?: ComponentType;
    itemComponent?: ComponentType;
    /** @private This API may be refactor since not well design */
    onVisibleChange?: (visibleCount: number) => void;
    /** When set to `full`, ssr will render full items by default and remove at client side */
    ssr?: 'full';
}
type ForwardOverflowType = <ItemType = any>(props: React.PropsWithChildren<OverflowProps<ItemType>> & {
    ref?: React.Ref<HTMLDivElement>;
}) => React.ReactElement;
type FilledOverflowType = ForwardOverflowType & {
    Item: typeof RawItem;
    RESPONSIVE: typeof RESPONSIVE;
    /** Will work as normal `component`. Skip patch props like `prefixCls`. */
    INVALIDATE: typeof INVALIDATE;
};
declare const _default: FilledOverflowType;
export default _default;
