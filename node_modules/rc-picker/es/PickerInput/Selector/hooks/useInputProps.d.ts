import type { SelectorProps } from '../../../interface';
import type { InputProps } from '../Input';
export default function useInputProps<DateType extends object = any>(props: Pick<SelectorProps, 'maskFormat' | 'format' | 'generateConfig' | 'locale' | 'preserveInvalidOnBlur' | 'inputReadOnly' | 'required' | 'aria-required' | 'onSubmit' | 'onFocus' | 'onBlur' | 'onInputChange' | 'onInvalid' | 'onOpenChange' | 'onKeyDown' | 'activeHelp' | 'name' | 'autoComplete' | 'open' | 'picker'> & {
    id?: string | string[];
    value?: DateType[];
    invalid?: boolean | [boolean, boolean];
    placeholder?: string | [string, string];
    disabled?: boolean | [boolean, boolean];
    onChange: (value: DateType | null, index?: number) => void;
    allHelp: boolean;
    activeIndex?: number | null;
}, 
/** Used for SinglePicker */
postProps?: (info: {
    valueTexts: string[];
}) => Partial<InputProps>): readonly [(index?: number) => InputProps, (date: DateType) => string];
