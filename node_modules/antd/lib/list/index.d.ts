import * as React from 'react';
import type { RowProps } from '../grid';
import type { PaginationConfig } from '../pagination';
import type { SpinProps } from '../spin';
export type { ListItemMetaProps, ListItemProps } from './Item';
export type { ListConsumerProps } from './context';
export type ColumnCount = number;
export type ColumnType = 'gutter' | 'column' | 'xs' | 'sm' | 'md' | 'lg' | 'xl' | 'xxl';
export interface ListGridType {
    gutter?: RowProps['gutter'];
    column?: ColumnCount;
    xs?: ColumnCount;
    sm?: ColumnCount;
    md?: ColumnCount;
    lg?: ColumnCount;
    xl?: ColumnCount;
    xxl?: ColumnCount;
}
export type ListSize = 'small' | 'default' | 'large';
export type ListItemLayout = 'horizontal' | 'vertical';
export interface ListProps<T> {
    bordered?: boolean;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
    dataSource?: T[];
    extra?: React.ReactNode;
    grid?: ListGridType;
    id?: string;
    itemLayout?: ListItemLayout;
    loading?: boolean | SpinProps;
    loadMore?: React.ReactNode;
    pagination?: PaginationConfig | false;
    prefixCls?: string;
    rowKey?: ((item: T) => React.Key) | keyof T;
    renderItem?: (item: T, index: number) => React.ReactNode;
    size?: ListSize;
    split?: boolean;
    header?: React.ReactNode;
    footer?: React.ReactNode;
    locale?: ListLocale;
}
export interface ListLocale {
    emptyText: React.ReactNode;
}
declare function List<T>({ pagination, prefixCls: customizePrefixCls, bordered, split, className, rootClassName, style, children, itemLayout, loadMore, grid, dataSource, size: customizeSize, header, footer, loading, rowKey, renderItem, locale, ...rest }: ListProps<T>): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
declare namespace List {
    var displayName: string;
    var Item: import("./Item").ListItemTypeProps;
}
export default List;
