import * as React from 'react';
import type { ColProps } from '../grid/col';
import type { TooltipProps } from '../tooltip';
import type { FormLabelAlign } from './interface';
export type WrapperTooltipProps = TooltipProps & {
    icon?: React.ReactElement;
};
export type LabelTooltipType = WrapperTooltipProps | React.ReactNode;
export interface FormItemLabelProps {
    colon?: boolean;
    htmlFor?: string;
    label?: React.ReactNode;
    labelAlign?: FormLabelAlign;
    labelCol?: ColProps;
    tooltip?: LabelTooltipType;
}
declare const FormItemLabel: React.FC<FormItemLabelProps & {
    required?: boolean;
    prefixCls: string;
}>;
export default FormItemLabel;
