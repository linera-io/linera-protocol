import * as React from 'react';
import type { PresetColorType } from '../_util/colors';
import type { LiteralUnion } from '../_util/type';
type RibbonPlacement = 'start' | 'end';
export interface RibbonProps {
    className?: string;
    prefixCls?: string;
    style?: React.CSSProperties;
    text?: React.ReactNode;
    color?: LiteralUnion<PresetColorType>;
    children?: React.ReactNode;
    placement?: RibbonPlacement;
    rootClassName?: string;
}
declare const Ribbon: React.FC<RibbonProps>;
export default Ribbon;
