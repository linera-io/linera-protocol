import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorHexInputProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorHexInput: FC<ColorHexInputProps>;
export default ColorHexInput;
