import type { FormInstance as RcFormInstance } from 'rc-field-form';
import type { NamePath, ScrollOptions } from '../interface';
export interface FormInstance<Values = any> extends RcFormInstance<Values> {
    scrollToField: (name: NamePath, options?: ScrollOptions) => void;
    getFieldInstance: (name: NamePath) => any;
}
export default function useForm<Values = any>(form?: FormInstance<Values>): [FormInstance<Values>];
