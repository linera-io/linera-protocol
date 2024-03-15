import * as React from 'react';
import type { OnStartMove } from '../interface';
interface RenderProps {
    index: number;
    prefixCls: string;
    value: number;
    dragging: boolean;
}
export interface HandleProps {
    prefixCls: string;
    style?: React.CSSProperties;
    value: number;
    valueIndex: number;
    dragging: boolean;
    onStartMove: OnStartMove;
    onOffsetChange: (value: number | 'min' | 'max', valueIndex: number) => void;
    onFocus?: (e: React.FocusEvent<HTMLDivElement>) => void;
    onBlur?: (e: React.FocusEvent<HTMLDivElement>) => void;
    render?: (origin: React.ReactElement<HandleProps>, props: RenderProps) => React.ReactElement;
    onChangeComplete?: () => void;
}
declare const Handle: React.ForwardRefExoticComponent<HandleProps & React.RefAttributes<HTMLDivElement>>;
export default Handle;
