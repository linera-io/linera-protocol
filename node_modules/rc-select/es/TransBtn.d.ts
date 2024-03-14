import * as React from 'react';
import type { RenderNode } from './BaseSelect';
export interface TransBtnProps {
    className: string;
    customizeIcon: RenderNode;
    customizeIconProps?: any;
    onMouseDown?: React.MouseEventHandler<HTMLSpanElement>;
    onClick?: React.MouseEventHandler<HTMLSpanElement>;
    children?: React.ReactNode;
}
declare const TransBtn: React.FC<TransBtnProps>;
export default TransBtn;
