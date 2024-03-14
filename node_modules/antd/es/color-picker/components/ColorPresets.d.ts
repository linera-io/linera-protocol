import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps, PresetsItem } from '../interface';
interface ColorPresetsProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    presets: PresetsItem[];
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const ColorPresets: FC<ColorPresetsProps>;
export default ColorPresets;
