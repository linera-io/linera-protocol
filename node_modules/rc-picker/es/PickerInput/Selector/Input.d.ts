import * as React from 'react';
import type { PickerRef } from '../../interface';
export interface InputRef extends PickerRef {
    inputElement: HTMLInputElement;
}
export interface InputProps extends Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onChange'> {
    format?: string;
    validateFormat: (value: string) => boolean;
    active?: boolean;
    /** Used for single picker only */
    showActiveCls?: boolean;
    suffixIcon?: React.ReactNode;
    value?: string;
    onChange: (value: string) => void;
    onSubmit: VoidFunction;
    /** Meaning current is from the hover cell getting the placeholder text */
    helped?: boolean;
    /**
     * Trigger when input need additional help.
     * Like open the popup for interactive.
     */
    onHelp: () => void;
    preserveInvalidOnBlur?: boolean;
    invalid?: boolean;
    clearIcon?: React.ReactNode;
}
declare const Input: React.ForwardRefExoticComponent<InputProps & React.RefAttributes<InputRef>>;
export default Input;
