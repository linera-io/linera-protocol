import * as React from 'react';
import type { FieldProps } from 'rc-field-form/lib/Field';
import type { FormInstance } from '../Form';
import type { FormItemInputProps } from '../FormItemInput';
import type { FormItemLabelProps, LabelTooltipType } from '../FormItemLabel';
import useFormItemStatus from '../hooks/useFormItemStatus';
declare const ValidateStatuses: readonly ["success", "warning", "error", "validating", ""];
export type ValidateStatus = (typeof ValidateStatuses)[number];
type RenderChildren<Values = any> = (form: FormInstance<Values>) => React.ReactNode;
type RcFieldProps<Values = any> = Omit<FieldProps<Values>, 'children'>;
type ChildrenType<Values = any> = RenderChildren<Values> | React.ReactNode;
export type FeedbackIcons = (itemStatus: {
    status: ValidateStatus;
    errors?: React.ReactNode[];
    warnings?: React.ReactNode[];
}) => {
    [key in ValidateStatus]?: React.ReactNode;
};
export interface FormItemProps<Values = any> extends Omit<FormItemLabelProps, 'requiredMark'>, FormItemInputProps, RcFieldProps<Values> {
    prefixCls?: string;
    noStyle?: boolean;
    style?: React.CSSProperties;
    className?: string;
    rootClassName?: string;
    children?: ChildrenType<Values>;
    id?: string;
    hasFeedback?: boolean | {
        icons: FeedbackIcons;
    };
    validateStatus?: ValidateStatus;
    required?: boolean;
    hidden?: boolean;
    initialValue?: any;
    messageVariables?: Record<string, string>;
    tooltip?: LabelTooltipType;
    /** @deprecated No need anymore */
    fieldKey?: React.Key | React.Key[];
}
declare function InternalFormItem<Values = any>(props: FormItemProps<Values>): React.ReactElement;
type InternalFormItemType = typeof InternalFormItem;
type CompoundedComponent = InternalFormItemType & {
    useStatus: typeof useFormItemStatus;
};
declare const FormItem: CompoundedComponent;
export default FormItem;
