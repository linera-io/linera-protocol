import type { CSSProperties, MouseEventHandler } from 'react';
import React from 'react';
import type { ColorPickerProps } from '../ColorPicker';
import type { ColorPickerBaseProps } from '../interface';
interface colorTriggerProps extends Pick<ColorPickerBaseProps, 'prefixCls' | 'colorCleared' | 'disabled' | 'format'> {
    color: Exclude<ColorPickerBaseProps['color'], undefined>;
    open?: boolean;
    showText?: ColorPickerProps['showText'];
    className?: string;
    style?: CSSProperties;
    onClick?: MouseEventHandler<HTMLDivElement>;
    onMouseEnter?: MouseEventHandler<HTMLDivElement>;
    onMouseLeave?: MouseEventHandler<HTMLDivElement>;
}
declare const ColorTrigger: React.ForwardRefExoticComponent<colorTriggerProps & React.RefAttributes<HTMLDivElement>>;
export default ColorTrigger;
