import type { Rule, RuleObject, RuleRender } from 'rc-field-form/lib/interface';
import ErrorList, { type ErrorListProps } from './ErrorList';
import InternalForm, { useForm, useWatch, type FormInstance, type FormProps } from './Form';
import Item, { type FormItemProps } from './FormItem';
import List, { type FormListFieldData, type FormListOperation, type FormListProps } from './FormList';
import { FormProvider } from './context';
import useFormInstance from './hooks/useFormInstance';
type InternalFormType = typeof InternalForm;
type CompoundedComponent = InternalFormType & {
    useForm: typeof useForm;
    useFormInstance: typeof useFormInstance;
    useWatch: typeof useWatch;
    Item: typeof Item;
    List: typeof List;
    ErrorList: typeof ErrorList;
    Provider: typeof FormProvider;
    /** @deprecated Only for warning usage. Do not use. */
    create: () => void;
};
declare const Form: CompoundedComponent;
export type { ErrorListProps, FormInstance, FormItemProps, FormListFieldData, FormListOperation, FormListProps, FormProps, Rule, RuleObject, RuleRender, };
export default Form;
