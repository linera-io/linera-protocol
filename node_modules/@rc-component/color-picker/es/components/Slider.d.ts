import type { FC } from 'react';
import type { BaseColorPickerProps, HsbaColorType } from '../interface';
interface SliderProps extends BaseColorPickerProps {
    gradientColors: string[];
    direction?: string;
    type?: HsbaColorType;
    value?: string;
}
declare const Slider: FC<SliderProps>;
export default Slider;
