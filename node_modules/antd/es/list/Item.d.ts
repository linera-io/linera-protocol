import type { CSSProperties, FC, ForwardRefExoticComponent, HTMLAttributes, ReactNode } from 'react';
import React from 'react';
export interface ListItemProps extends HTMLAttributes<HTMLDivElement> {
    className?: string;
    children?: ReactNode;
    prefixCls?: string;
    style?: CSSProperties;
    extra?: ReactNode;
    actions?: ReactNode[];
    colStyle?: CSSProperties;
}
export interface ListItemMetaProps {
    avatar?: ReactNode;
    className?: string;
    children?: ReactNode;
    description?: ReactNode;
    prefixCls?: string;
    style?: CSSProperties;
    title?: ReactNode;
}
export declare const Meta: FC<ListItemMetaProps>;
export interface ListItemTypeProps extends ForwardRefExoticComponent<ListItemProps & React.RefAttributes<HTMLElement>> {
    Meta: typeof Meta;
}
declare const Item: ListItemTypeProps;
export default Item;
