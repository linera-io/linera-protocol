import * as React from 'react';
import type { ComponentType } from './RawItem';
export interface ItemProps<ItemType> extends React.HTMLAttributes<any> {
    prefixCls: string;
    item?: ItemType;
    className?: string;
    style?: React.CSSProperties;
    renderItem?: (item: ItemType) => React.ReactNode;
    responsive?: boolean;
    /**
     * @private To make node structure stable. We need keep wrap with ResizeObserver.
     * But disable it when it's no need to real measure.
     */
    responsiveDisabled?: boolean;
    itemKey?: React.Key;
    registerSize: (key: React.Key, width: number | null) => void;
    children?: React.ReactNode;
    display: boolean;
    order: number;
    component?: ComponentType;
    invalidate?: boolean;
}
declare const Item: React.ForwardRefExoticComponent<ItemProps<unknown> & React.RefAttributes<any>>;
export default Item;
