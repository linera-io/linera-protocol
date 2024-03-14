import React from 'react';
import type { PosInfo } from './hooks/useTarget';
export interface MaskProps {
    prefixCls?: string;
    pos: PosInfo;
    rootClassName?: string;
    showMask?: boolean;
    style?: React.CSSProperties;
    fill?: string;
    open?: boolean;
    animated?: boolean | {
        placeholder: boolean;
    };
    zIndex?: number;
    disabledInteraction?: boolean;
}
declare const Mask: (props: MaskProps) => React.JSX.Element;
export default Mask;
