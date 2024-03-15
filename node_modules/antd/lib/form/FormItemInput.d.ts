import * as React from 'react';
import type { ColProps } from '../grid/col';
import type { ValidateStatus } from './FormItem';
interface FormItemInputMiscProps {
    prefixCls: string;
    children: React.ReactNode;
    errors: React.ReactNode[];
    warnings: React.ReactNode[];
    marginBottom?: number | null;
    onErrorVisibleChanged?: (visible: boolean) => void;
}
export interface FormItemInputProps {
    wrapperCol?: ColProps;
    extra?: React.ReactNode;
    status?: ValidateStatus;
    help?: React.ReactNode;
    fieldId?: string;
}
declare const FormItemInput: React.FC<FormItemInputProps & FormItemInputMiscProps>;
export default FormItemInput;
