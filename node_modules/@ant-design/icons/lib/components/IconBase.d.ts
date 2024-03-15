import * as React from 'react';
import type { IconDefinition } from '@ant-design/icons-svg/lib/types';
export interface IconProps {
    icon: IconDefinition;
    className?: string;
    onClick?: React.MouseEventHandler<SVGSVGElement>;
    style?: React.CSSProperties;
    primaryColor?: string;
    secondaryColor?: string;
    focusable?: string;
}
export interface TwoToneColorPaletteSetter {
    primaryColor: string;
    secondaryColor?: string;
}
export interface TwoToneColorPalette extends TwoToneColorPaletteSetter {
    calculated?: boolean;
}
declare function setTwoToneColors({ primaryColor, secondaryColor }: TwoToneColorPaletteSetter): void;
declare function getTwoToneColors(): TwoToneColorPalette;
interface IconBaseComponent<P> extends React.FC<P> {
    getTwoToneColors: typeof getTwoToneColors;
    setTwoToneColors: typeof setTwoToneColors;
}
declare const IconBase: IconBaseComponent<IconProps>;
export default IconBase;
