import * as React from 'react';
import type { OnStartMove } from '../interface';
export interface TrackProps {
    prefixCls: string;
    style?: React.CSSProperties | React.CSSProperties[];
    values: number[];
    onStartMove?: OnStartMove;
    startPoint?: number;
}
export default function Tracks(props: TrackProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
