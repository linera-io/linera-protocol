import * as React from 'react';
export interface CellProps {
    itemPrefixCls: string;
    span: number;
    className?: string;
    component: string;
    style?: React.CSSProperties;
    labelStyle?: React.CSSProperties;
    contentStyle?: React.CSSProperties;
    bordered?: boolean;
    label?: React.ReactNode;
    content?: React.ReactNode;
    colon?: boolean;
    type?: 'label' | 'content' | 'item';
}
declare const Cell: React.FC<CellProps>;
export default Cell;
