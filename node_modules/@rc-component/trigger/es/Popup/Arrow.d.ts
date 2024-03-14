import * as React from 'react';
import type { AlignType, ArrowPos, ArrowTypeOuter } from '../interface';
export interface ArrowProps {
    prefixCls: string;
    align: AlignType;
    arrow: ArrowTypeOuter;
    arrowPos: ArrowPos;
}
export default function Arrow(props: ArrowProps): React.JSX.Element;
