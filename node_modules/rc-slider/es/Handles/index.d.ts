import * as React from 'react';
import type { HandleProps } from './Handle';
import type { OnStartMove } from '../interface';
export interface HandlesProps {
    prefixCls: string;
    style?: React.CSSProperties | React.CSSProperties[];
    values: number[];
    onStartMove: OnStartMove;
    onOffsetChange: (value: number | 'min' | 'max', valueIndex: number) => void;
    onFocus?: (e: React.FocusEvent<HTMLDivElement>) => void;
    onBlur?: (e: React.FocusEvent<HTMLDivElement>) => void;
    handleRender?: HandleProps['render'];
    draggingIndex: number;
    onChangeComplete?: () => void;
}
export interface HandlesRef {
    focus: (index: number) => void;
}
declare const Handles: React.ForwardRefExoticComponent<HandlesProps & React.RefAttributes<HandlesRef>>;
export default Handles;
