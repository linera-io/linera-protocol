import React from 'react';
import type { StarProps } from './Star';
export interface RateProps extends Pick<StarProps, 'count' | 'character' | 'characterRender' | 'allowHalf' | 'disabled'> {
    value?: number;
    defaultValue?: number;
    allowClear?: boolean;
    style?: React.CSSProperties;
    prefixCls?: string;
    onChange?: (value: number) => void;
    onHoverChange?: (value: number) => void;
    className?: string;
    tabIndex?: number;
    onFocus?: () => void;
    onBlur?: () => void;
    onKeyDown?: React.KeyboardEventHandler<HTMLUListElement>;
    onMouseEnter?: React.MouseEventHandler<HTMLUListElement>;
    onMouseLeave?: React.MouseEventHandler<HTMLUListElement>;
    id?: string;
    autoFocus?: boolean;
    direction?: string;
}
export interface RateRef {
    focus: VoidFunction;
    blur: VoidFunction;
}
declare const _default: React.ForwardRefExoticComponent<RateProps & React.RefAttributes<RateRef>>;
export default _default;
