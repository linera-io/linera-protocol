import type { DisplayValueType, Mode, RenderNode } from '../interface';
import React from 'react';
export declare const useAllowClear: (prefixCls: string, onClearMouseDown: React.MouseEventHandler<HTMLSpanElement>, displayValues: DisplayValueType[], allowClear?: boolean | {
    clearIcon?: RenderNode;
}, clearIcon?: RenderNode, disabled?: boolean, mergedSearchValue?: string, mode?: Mode) => {
    allowClear: boolean;
    clearIcon: React.JSX.Element;
};
