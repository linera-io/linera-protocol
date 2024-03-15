import * as React from 'react';
import type { InternalMode, PickerRef, SelectorProps } from '../../../interface';
import type { PickerProps } from '../../SinglePicker';
export interface SingleSelectorProps<DateType extends object = any> extends SelectorProps<DateType>, Pick<PickerProps, 'multiple' | 'maxTagCount'> {
    id?: string;
    value?: DateType[];
    onChange: (date: DateType[]) => void;
    internalPicker: InternalMode;
    disabled: boolean;
    /** All the field show as `placeholder` */
    allHelp: boolean;
    placeholder?: string;
    invalid: boolean;
    onInvalid: (valid: boolean) => void;
    removeIcon?: React.ReactNode;
}
declare const RefSingleSelector: React.ForwardRefExoticComponent<SingleSelectorProps<object> & React.RefAttributes<PickerRef>>;
export default RefSingleSelector;
