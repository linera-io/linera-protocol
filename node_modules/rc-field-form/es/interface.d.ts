import type { ReactElement } from 'react';
import type { DeepNamePath } from './namePathType';
import type { ReducerAction } from './useForm';
export type InternalNamePath = (string | number)[];
export type NamePath<T = any> = DeepNamePath<T>;
export type StoreValue = any;
export type Store = Record<string, StoreValue>;
export interface Meta {
    touched: boolean;
    validating: boolean;
    errors: string[];
    warnings: string[];
    name: InternalNamePath;
    validated: boolean;
}
export interface InternalFieldData extends Meta {
    value: StoreValue;
}
/**
 * Used by `setFields` config
 */
export interface FieldData extends Partial<Omit<InternalFieldData, 'name'>> {
    name: NamePath;
}
export type RuleType = 'string' | 'number' | 'boolean' | 'method' | 'regexp' | 'integer' | 'float' | 'object' | 'enum' | 'date' | 'url' | 'hex' | 'email';
type Validator = (rule: RuleObject, value: StoreValue, callback: (error?: string) => void) => Promise<void | any> | void;
export type RuleRender = (form: FormInstance) => RuleObject;
export interface ValidatorRule {
    warningOnly?: boolean;
    message?: string | ReactElement;
    validator: Validator;
}
interface BaseRule {
    warningOnly?: boolean;
    enum?: StoreValue[];
    len?: number;
    max?: number;
    message?: string | ReactElement;
    min?: number;
    pattern?: RegExp;
    required?: boolean;
    transform?: (value: StoreValue) => StoreValue;
    type?: RuleType;
    whitespace?: boolean;
    /** Customize rule level `validateTrigger`. Must be subset of Field `validateTrigger` */
    validateTrigger?: string | string[];
}
type AggregationRule = BaseRule & Partial<ValidatorRule>;
interface ArrayRule extends Omit<AggregationRule, 'type'> {
    type: 'array';
    defaultField?: RuleObject;
}
export type RuleObject = AggregationRule | ArrayRule;
export type Rule = RuleObject | RuleRender;
export interface ValidateErrorEntity<Values = any> {
    values: Values;
    errorFields: {
        name: InternalNamePath;
        errors: string[];
    }[];
    outOfDate: boolean;
}
export interface FieldEntity {
    onStoreChange: (store: Store, namePathList: InternalNamePath[] | null, info: ValuedNotifyInfo) => void;
    isFieldTouched: () => boolean;
    isFieldDirty: () => boolean;
    isFieldValidating: () => boolean;
    isListField: () => boolean;
    isList: () => boolean;
    isPreserve: () => boolean;
    validateRules: (options?: InternalValidateOptions) => Promise<RuleError[]>;
    getMeta: () => Meta;
    getNamePath: () => InternalNamePath;
    getErrors: () => string[];
    getWarnings: () => string[];
    props: {
        name?: NamePath;
        rules?: Rule[];
        dependencies?: NamePath[];
        initialValue?: any;
    };
}
export interface FieldError {
    name: InternalNamePath;
    errors: string[];
    warnings: string[];
}
export interface RuleError {
    errors: string[];
    rule: RuleObject;
}
export interface ValidateOptions {
    /**
     * Validate only and not trigger UI and Field status update
     */
    validateOnly?: boolean;
    /**
     * Recursive validate. It will validate all the name path that contains the provided one.
     * e.g. [['a']] will validate ['a'] , ['a', 'b'] and ['a', 1].
     */
    recursive?: boolean;
    /** Validate when a field is dirty (validated or touched) */
    dirty?: boolean;
}
export type ValidateFields<Values = any> = {
    (opt?: ValidateOptions): Promise<Values>;
    (nameList?: NamePath[], opt?: ValidateOptions): Promise<Values>;
};
export interface InternalValidateOptions extends ValidateOptions {
    triggerName?: string;
    validateMessages?: ValidateMessages;
}
export type InternalValidateFields<Values = any> = {
    (options?: InternalValidateOptions): Promise<Values>;
    (nameList?: NamePath[], options?: InternalValidateOptions): Promise<Values>;
};
interface ValueUpdateInfo {
    type: 'valueUpdate';
    source: 'internal' | 'external';
}
interface ValidateFinishInfo {
    type: 'validateFinish';
}
interface ResetInfo {
    type: 'reset';
}
interface RemoveInfo {
    type: 'remove';
}
interface SetFieldInfo {
    type: 'setField';
    data: FieldData;
}
interface DependenciesUpdateInfo {
    type: 'dependenciesUpdate';
    /**
     * Contains all the related `InternalNamePath[]`.
     * a <- b <- c : change `a`
     * relatedFields=[a, b, c]
     */
    relatedFields: InternalNamePath[];
}
export type NotifyInfo = ValueUpdateInfo | ValidateFinishInfo | ResetInfo | RemoveInfo | SetFieldInfo | DependenciesUpdateInfo;
export type ValuedNotifyInfo = NotifyInfo & {
    store: Store;
};
export interface Callbacks<Values = any> {
    onValuesChange?: (changedValues: any, values: Values) => void;
    onFieldsChange?: (changedFields: FieldData[], allFields: FieldData[]) => void;
    onFinish?: (values: Values) => void;
    onFinishFailed?: (errorInfo: ValidateErrorEntity<Values>) => void;
}
export type WatchCallBack = (values: Store, allValues: Store, namePathList: InternalNamePath[]) => void;
export interface WatchOptions<Form extends FormInstance = FormInstance> {
    form?: Form;
    preserve?: boolean;
}
export interface InternalHooks {
    dispatch: (action: ReducerAction) => void;
    initEntityValue: (entity: FieldEntity) => void;
    registerField: (entity: FieldEntity) => () => void;
    useSubscribe: (subscribable: boolean) => void;
    setInitialValues: (values: Store, init: boolean) => void;
    destroyForm: () => void;
    setCallbacks: (callbacks: Callbacks) => void;
    registerWatch: (callback: WatchCallBack) => () => void;
    getFields: (namePathList?: InternalNamePath[]) => FieldData[];
    setValidateMessages: (validateMessages: ValidateMessages) => void;
    setPreserve: (preserve?: boolean) => void;
    getInitialValue: (namePath: InternalNamePath) => StoreValue;
}
/** Only return partial when type is not any */
type RecursivePartial<T> = NonNullable<T> extends object ? {
    [P in keyof T]?: NonNullable<T[P]> extends (infer U)[] ? RecursivePartial<U>[] : NonNullable<T[P]> extends object ? RecursivePartial<T[P]> : T[P];
} : T;
export type FilterFunc = (meta: Meta) => boolean;
export type GetFieldsValueConfig = {
    strict?: boolean;
    filter?: FilterFunc;
};
export interface FormInstance<Values = any> {
    getFieldValue: (name: NamePath) => StoreValue;
    getFieldsValue: (() => Values) & ((nameList: NamePath[] | true, filterFunc?: FilterFunc) => any) & ((config: GetFieldsValueConfig) => any);
    getFieldError: (name: NamePath) => string[];
    getFieldsError: (nameList?: NamePath[]) => FieldError[];
    getFieldWarning: (name: NamePath) => string[];
    isFieldsTouched: ((nameList?: NamePath[], allFieldsTouched?: boolean) => boolean) & ((allFieldsTouched?: boolean) => boolean);
    isFieldTouched: (name: NamePath) => boolean;
    isFieldValidating: (name: NamePath) => boolean;
    isFieldsValidating: (nameList?: NamePath[]) => boolean;
    resetFields: (fields?: NamePath[]) => void;
    setFields: (fields: FieldData[]) => void;
    setFieldValue: (name: NamePath, value: any) => void;
    setFieldsValue: (values: RecursivePartial<Values>) => void;
    validateFields: ValidateFields<Values>;
    submit: () => void;
}
export type InternalFormInstance = Omit<FormInstance, 'validateFields'> & {
    validateFields: InternalValidateFields;
    /**
     * Passed by field context props
     */
    prefixName?: InternalNamePath;
    validateTrigger?: string | string[] | false;
    /**
     * Form component should register some content into store.
     * We pass the `HOOK_MARK` as key to avoid user call the function.
     */
    getInternalHooks: (secret: string) => InternalHooks | null;
    /** @private Internal usage. Do not use it in your production */
    _init?: boolean;
};
export type EventArgs = any[];
type ValidateMessage = string | (() => string);
export interface ValidateMessages {
    default?: ValidateMessage;
    required?: ValidateMessage;
    enum?: ValidateMessage;
    whitespace?: ValidateMessage;
    date?: {
        format?: ValidateMessage;
        parse?: ValidateMessage;
        invalid?: ValidateMessage;
    };
    types?: {
        string?: ValidateMessage;
        method?: ValidateMessage;
        array?: ValidateMessage;
        object?: ValidateMessage;
        number?: ValidateMessage;
        date?: ValidateMessage;
        boolean?: ValidateMessage;
        integer?: ValidateMessage;
        float?: ValidateMessage;
        regexp?: ValidateMessage;
        email?: ValidateMessage;
        url?: ValidateMessage;
        hex?: ValidateMessage;
    };
    string?: {
        len?: ValidateMessage;
        min?: ValidateMessage;
        max?: ValidateMessage;
        range?: ValidateMessage;
    };
    number?: {
        len?: ValidateMessage;
        min?: ValidateMessage;
        max?: ValidateMessage;
        range?: ValidateMessage;
    };
    array?: {
        len?: ValidateMessage;
        min?: ValidateMessage;
        max?: ValidateMessage;
        range?: ValidateMessage;
    };
    pattern?: {
        mismatch?: ValidateMessage;
    };
}
export {};
