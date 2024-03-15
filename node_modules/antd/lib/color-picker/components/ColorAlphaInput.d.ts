import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorAlphaInputProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorAlphaInput: FC<ColorAlphaInputProps>;
export default ColorAlphaInput;
