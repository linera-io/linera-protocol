import type { HsbaColorType } from '@rc-component/color-picker';
import type { FC } from 'react';
import type { Color } from '../color';
import type { ColorPickerBaseProps } from '../interface';
export interface PanelPickerProps extends Pick<ColorPickerBaseProps, 'prefixCls' | 'colorCleared' | 'allowClear' | 'disabledAlpha' | 'onChangeComplete'> {
    value?: Color;
    onChange?: (value?: Color, type?: HsbaColorType, pickColor?: boolean) => void;
    onClear?: () => void;
}
declare const PanelPicker: FC;
export default PanelPicker;
