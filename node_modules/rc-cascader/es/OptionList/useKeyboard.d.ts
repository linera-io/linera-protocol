import type { RefOptionListProps } from 'rc-select/lib/OptionList';
import * as React from 'react';
import type { DefaultOptionType, InternalFieldNames, SingleValueType } from '../Cascader';
declare const _default: (ref: React.Ref<RefOptionListProps>, options: DefaultOptionType[], fieldNames: InternalFieldNames, activeValueCells: React.Key[], setActiveValueCells: (activeValueCells: React.Key[]) => void, onKeyBoardSelect: (valueCells: SingleValueType, option: DefaultOptionType) => void, contextProps: {
    direction: 'ltr' | 'rtl';
    searchValue: string;
    toggleOpen: (open?: boolean) => void;
    open: boolean;
}) => void;
export default _default;
