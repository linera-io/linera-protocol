import React from 'react';
export declare const OverflowContext: React.Context<{
    prefixCls: string;
    responsive: boolean;
    order: number;
    registerSize: (key: React.Key, width: number | null) => void;
    display: boolean;
    invalidate: boolean;
    item?: any;
    itemKey?: React.Key;
    className?: string;
}>;
