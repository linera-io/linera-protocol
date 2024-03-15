import type { SelectProps } from 'rc-select';
import type { OptionProps } from 'rc-select/es/Option';
import React from 'react';
import type { PaginationLocale } from './interface';
interface InternalSelectProps extends SelectProps {
    /**
     * form antd v5.5.0, popupMatchSelectWidth default is true
     */
    popupMatchSelectWidth?: boolean;
}
interface OptionsProps {
    disabled?: boolean;
    locale: PaginationLocale;
    rootPrefixCls: string;
    selectPrefixCls?: string;
    pageSize: number;
    pageSizeOptions?: (string | number)[];
    goButton?: boolean | string;
    changeSize?: (size: number) => void;
    quickGo?: (value: number) => void;
    buildOptionText?: (value: string | number) => string;
    selectComponentClass: React.ComponentType<Partial<InternalSelectProps>> & {
        Option?: React.ComponentType<Partial<OptionProps>>;
    };
}
declare const Options: React.FC<OptionsProps>;
export default Options;
