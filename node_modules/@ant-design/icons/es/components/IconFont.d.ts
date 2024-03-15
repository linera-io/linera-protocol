import * as React from 'react';
import type { IconBaseProps } from './Icon';
export interface CustomIconOptions {
    scriptUrl?: string | string[];
    extraCommonProps?: Record<string, unknown>;
}
export interface IconFontProps<T extends string = string> extends IconBaseProps {
    type: T;
}
export default function create<T extends string = string>(options?: CustomIconOptions): React.FC<IconFontProps<T>>;
