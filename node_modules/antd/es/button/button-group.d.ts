import * as React from 'react';
import type { SizeType } from '../config-provider/SizeContext';
export interface ButtonGroupProps {
    size?: SizeType;
    style?: React.CSSProperties;
    className?: string;
    prefixCls?: string;
    children?: React.ReactNode;
}
export declare const GroupSizeContext: React.Context<SizeType>;
declare const ButtonGroup: React.FC<ButtonGroupProps>;
export default ButtonGroup;
