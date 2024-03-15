import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorRgbInputProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorRgbInput: FC<ColorRgbInputProps>;
export default ColorRgbInput;
