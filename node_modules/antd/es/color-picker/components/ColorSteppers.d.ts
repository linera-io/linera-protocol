import type { FC } from 'react';
import React from 'react';
import type { InputNumberProps } from '../../input-number';
import type { ColorPickerBaseProps } from '../interface';
interface ColorSteppersProps extends Pick<ColorPickerBaseProps, 'prefixCls'> {
    value?: number;
    min?: number;
    max?: number;
    onChange?: (value: number | null) => void;
    className?: string;
    prefix?: (prefixCls: string) => React.ReactNode;
    formatter?: InputNumberProps<number>['formatter'];
}
declare const ColorSteppers: FC<ColorSteppersProps>;
export default ColorSteppers;
