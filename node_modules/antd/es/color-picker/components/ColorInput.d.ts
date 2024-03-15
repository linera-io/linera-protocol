import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorInputProps extends Pick<ColorPickerBaseProps, 'prefixCls' | 'format' | 'onFormatChange' | 'disabledAlpha'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorInput: FC<ColorInputProps>;
export default ColorInput;
