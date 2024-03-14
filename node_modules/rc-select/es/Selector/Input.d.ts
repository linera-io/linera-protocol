import * as React from 'react';
type InputRef = HTMLInputElement | HTMLTextAreaElement;
interface InputProps {
    prefixCls: string;
    id: string;
    inputElement: React.ReactElement;
    disabled: boolean;
    autoFocus: boolean;
    autoComplete: string;
    editable: boolean;
    activeDescendantId?: string;
    value: string;
    maxLength?: number;
    open: boolean;
    tabIndex: number;
    /** Pass accessibility props to input */
    attrs: Record<string, unknown>;
    onKeyDown: React.KeyboardEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
    onMouseDown: React.MouseEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
    onChange: React.ChangeEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
    onPaste: React.ClipboardEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
    onCompositionStart: React.CompositionEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
    onCompositionEnd: React.CompositionEventHandler<HTMLInputElement | HTMLTextAreaElement | HTMLElement>;
}
declare const RefInput: React.ForwardRefExoticComponent<InputProps & React.RefAttributes<InputRef>>;
export default RefInput;
