import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
interface ColorClearProps extends Pick<ColorPickerBaseProps, 'prefixCls' | 'colorCleared'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorClear: FC<ColorClearProps>;
export default ColorClear;
