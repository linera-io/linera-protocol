import * as React from 'react';
import type { ValidateMessages, FormInstance, FieldData, Store } from './interface';
export type Forms = Record<string, FormInstance>;
export interface FormChangeInfo {
    changedFields: FieldData[];
    forms: Forms;
}
export interface FormFinishInfo {
    values: Store;
    forms: Forms;
}
export interface FormProviderProps {
    validateMessages?: ValidateMessages;
    onFormChange?: (name: string, info: FormChangeInfo) => void;
    onFormFinish?: (name: string, info: FormFinishInfo) => void;
    children?: React.ReactNode;
}
export interface FormContextProps extends FormProviderProps {
    triggerFormChange: (name: string, changedFields: FieldData[]) => void;
    triggerFormFinish: (name: string, values: Store) => void;
    registerForm: (name: string, form: FormInstance) => void;
    unregisterForm: (name: string) => void;
}
declare const FormContext: React.Context<FormContextProps>;
declare const FormProvider: React.FunctionComponent<FormProviderProps>;
export { FormProvider };
export default FormContext;
