import type { CSSMotionProps } from 'rc-motion';
import * as React from 'react';
export interface MaskProps {
    prefixCls: string;
    open?: boolean;
    zIndex?: number;
    mask?: boolean;
    motion?: CSSMotionProps;
}
export default function Mask(props: MaskProps): React.JSX.Element;
