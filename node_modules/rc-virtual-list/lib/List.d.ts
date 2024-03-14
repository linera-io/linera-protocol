import * as React from 'react';
import type { InnerProps } from './Filler';
import type { ScrollBarDirectionType } from './ScrollBar';
import type { RenderFunc, ExtraRenderInfo } from './interface';
import type { ScrollPos, ScrollTarget } from './hooks/useScrollTo';
export interface ScrollInfo {
    x: number;
    y: number;
}
export type ScrollConfig = ScrollTarget | ScrollPos;
export type ScrollTo = (arg: number | ScrollConfig) => void;
export type ListRef = {
    scrollTo: ScrollTo;
    getScrollInfo: () => ScrollInfo;
};
export interface ListProps<T> extends Omit<React.HTMLAttributes<any>, 'children'> {
    prefixCls?: string;
    children: RenderFunc<T>;
    data: T[];
    height?: number;
    itemHeight?: number;
    /** If not match virtual scroll condition, Set List still use height of container. */
    fullHeight?: boolean;
    itemKey: React.Key | ((item: T) => React.Key);
    component?: string | React.FC<any> | React.ComponentClass<any>;
    /** Set `false` will always use real scroll instead of virtual one */
    virtual?: boolean;
    direction?: ScrollBarDirectionType;
    /**
     * By default `scrollWidth` is same as container.
     * When set this, it will show the horizontal scrollbar and
     * `scrollWidth` will be used as the real width instead of container width.
     * When set, `virtual` will always be enabled.
     */
    scrollWidth?: number;
    styles?: {
        horizontalScrollBar?: React.CSSProperties;
        horizontalScrollBarThumb?: React.CSSProperties;
        verticalScrollBar?: React.CSSProperties;
        verticalScrollBarThumb?: React.CSSProperties;
    };
    onScroll?: React.UIEventHandler<HTMLElement>;
    /**
     * Given the virtual offset value.
     * It's the logic offset from start position.
     */
    onVirtualScroll?: (info: ScrollInfo) => void;
    /** Trigger when render list item changed */
    onVisibleChange?: (visibleList: T[], fullList: T[]) => void;
    /** Inject to inner container props. Only use when you need pass aria related data */
    innerProps?: InnerProps;
    /** Render extra content into Filler */
    extraRender?: (info: ExtraRenderInfo) => React.ReactNode;
}
export declare function RawList<T>(props: ListProps<T>, ref: React.Ref<ListRef>): React.JSX.Element;
declare const _default: <Item = any>(props: ListProps<Item> & {
    ref?: React.Ref<ListRef>;
}) => React.ReactElement;
export default _default;
