import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
export interface PanelPresetsProps extends Pick<ColorPickerBaseProps, 'prefixCls' | 'presets'> {
    value?: Color;
    onChange?: (value: Color) => void;
}
declare const PanelPresets: FC;
export default PanelPresets;
