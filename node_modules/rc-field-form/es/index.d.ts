import * as React from 'react';
import { FormInstance } from './interface';
import Field from './Field';
import List from './List';
import useForm from './useForm';
import type { FormProps } from './Form';
import { FormProvider } from './FormContext';
import FieldContext from './FieldContext';
import ListContext from './ListContext';
import useWatch from './useWatch';
declare const InternalForm: <Values = any>(props: FormProps<Values> & {
    ref?: React.Ref<FormInstance<Values>>;
}) => React.ReactElement;
type InternalFormType = typeof InternalForm;
interface RefFormType extends InternalFormType {
    FormProvider: typeof FormProvider;
    Field: typeof Field;
    List: typeof List;
    useForm: typeof useForm;
    useWatch: typeof useWatch;
}
declare const RefForm: RefFormType;
export { Field, List, useForm, FormProvider, FieldContext, ListContext, useWatch };
export type { FormProps, FormInstance };
export default RefForm;
