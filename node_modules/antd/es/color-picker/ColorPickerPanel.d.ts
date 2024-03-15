import type { HsbaColorType } from '@rc-component/color-picker';
import type { FC } from 'react';
import type { Color } from './color';
import type { ColorPickerBaseProps } from './interface';
interface ColorPickerPanelProps extends ColorPickerBaseProps {
    onChange?: (value?: Color, type?: HsbaColorType, pickColor?: boolean) => void;
    onClear?: () => void;
}
declare const ColorPickerPanel: FC<ColorPickerPanelProps>;
export default ColorPickerPanel;
