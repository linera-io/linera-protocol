import * as React from 'react';
import type { StoreValue, ValidatorRule } from 'rc-field-form/lib/interface';
export interface FormListFieldData {
    name: number;
    key: number;
    /** @deprecated No need anymore Use key instead */
    fieldKey?: number;
}
export interface FormListOperation {
    add: (defaultValue?: StoreValue, insertIndex?: number) => void;
    remove: (index: number | number[]) => void;
    move: (from: number, to: number) => void;
}
export interface FormListProps {
    prefixCls?: string;
    name: string | number | (string | number)[];
    rules?: ValidatorRule[];
    initialValue?: any[];
    children: (fields: FormListFieldData[], operation: FormListOperation, meta: {
        errors: React.ReactNode[];
        warnings: React.ReactNode[];
    }) => React.ReactNode;
}
declare const FormList: React.FC<FormListProps>;
export default FormList;
