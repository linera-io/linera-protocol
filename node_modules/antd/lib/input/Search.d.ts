import * as React from 'react';
import type { InputProps, InputRef } from './Input';
export interface SearchProps extends InputProps {
    inputPrefixCls?: string;
    onSearch?: (value: string, event?: React.ChangeEvent<HTMLInputElement> | React.MouseEvent<HTMLElement> | React.KeyboardEvent<HTMLInputElement>, info?: {
        source?: 'clear' | 'input';
    }) => void;
    enterButton?: React.ReactNode;
    loading?: boolean;
}
declare const Search: React.ForwardRefExoticComponent<SearchProps & React.RefAttributes<InputRef>>;
export default Search;
