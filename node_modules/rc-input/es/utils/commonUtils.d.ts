import type React from 'react';
import type { BaseInputProps, InputProps } from '../interface';
export declare function hasAddon(props: BaseInputProps | InputProps): boolean;
export declare function hasPrefixSuffix(props: BaseInputProps | InputProps): boolean;
export declare function resolveOnChange<E extends HTMLInputElement | HTMLTextAreaElement>(target: E, e: React.ChangeEvent<E> | React.MouseEvent<HTMLElement, MouseEvent> | React.CompositionEvent<HTMLElement>, onChange: undefined | ((event: React.ChangeEvent<E>) => void), targetValue?: string): void;
export interface InputFocusOptions extends FocusOptions {
    cursor?: 'start' | 'end' | 'all';
}
export declare function triggerFocus(element?: HTMLInputElement | HTMLTextAreaElement, option?: InputFocusOptions): void;
