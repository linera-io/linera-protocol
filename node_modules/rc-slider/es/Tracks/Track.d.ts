import * as React from 'react';
import type { OnStartMove } from '../interface';
export interface TrackProps {
    prefixCls: string;
    style?: React.CSSProperties;
    /** Replace with origin prefix concat className */
    replaceCls?: string;
    start: number;
    end: number;
    index: number;
    onStartMove?: OnStartMove;
}
export default function Track(props: TrackProps): React.JSX.Element;
