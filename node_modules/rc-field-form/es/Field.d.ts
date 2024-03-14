import * as React from 'react';
import type { EventArgs, FormInstance, InternalFormInstance, InternalNamePath, Meta, NamePath, Rule, Store, StoreValue } from './interface';
export type ShouldUpdate<Values = any> = boolean | ((prevValues: Values, nextValues: Values, info: {
    source?: string;
}) => boolean);
interface ChildProps {
    [name: string]: any;
}
export type MetaEvent = Meta & {
    destroy?: boolean;
};
export interface InternalFieldProps<Values = any> {
    children?: React.ReactElement | ((control: ChildProps, meta: Meta, form: FormInstance<Values>) => React.ReactNode);
    /**
     * Set up `dependencies` field.
     * When dependencies field update and current field is touched,
     * will trigger validate rules and render.
     */
    dependencies?: NamePath[];
    getValueFromEvent?: (...args: EventArgs) => StoreValue;
    name?: InternalNamePath;
    normalize?: (value: StoreValue, prevValue: StoreValue, allValues: Store) => StoreValue;
    rules?: Rule[];
    shouldUpdate?: ShouldUpdate<Values>;
    trigger?: string;
    validateTrigger?: string | string[] | false;
    /**
     * Trigger will after configured milliseconds.
     */
    validateDebounce?: number;
    validateFirst?: boolean | 'parallel';
    valuePropName?: string;
    getValueProps?: (value: StoreValue) => Record<string, unknown>;
    messageVariables?: Record<string, string>;
    initialValue?: any;
    onReset?: () => void;
    onMetaChange?: (meta: MetaEvent) => void;
    preserve?: boolean;
    /** @private Passed by Form.List props. Do not use since it will break by path check. */
    isListField?: boolean;
    /** @private Passed by Form.List props. Do not use since it will break by path check. */
    isList?: boolean;
    /** @private Pass context as prop instead of context api
     *  since class component can not get context in constructor */
    fieldContext?: InternalFormInstance;
}
export interface FieldProps<Values = any> extends Omit<InternalFieldProps<Values>, 'name' | 'fieldContext'> {
    name?: NamePath<Values>;
}
export interface FieldState {
    resetCount: number;
}
declare function WrapperField<Values = any>({ name, ...restProps }: FieldProps<Values>): React.JSX.Element;
export default WrapperField;
