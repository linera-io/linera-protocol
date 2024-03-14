import type { ReactNode } from 'react';
import * as React from 'react';
/**
 * Used for `useFilledProps` since it already in the React.useMemo
 */
export declare function fillClearIcon(prefixCls: string, allowClear?: boolean | {
    clearIcon?: ReactNode;
}, clearIcon?: ReactNode): string | number | true | Iterable<ReactNode> | React.JSX.Element;
