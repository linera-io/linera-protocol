import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorHsbInputProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorHsbInput: FC<ColorHsbInputProps>;
export default ColorHsbInput;
