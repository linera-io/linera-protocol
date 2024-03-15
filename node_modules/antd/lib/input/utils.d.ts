import type { ReactNode } from 'react';
import type { InputProps } from './Input';
export declare function hasPrefixSuffix(props: {
    prefix?: ReactNode;
    suffix?: ReactNode;
    allowClear?: InputProps['allowClear'];
    showCount?: InputProps['showCount'];
}): boolean;
