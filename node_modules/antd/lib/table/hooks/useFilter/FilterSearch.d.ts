import * as React from 'react';
import type { FilterSearchType, TableLocale } from '../../interface';
interface FilterSearchProps<RecordType = any> {
    value: string;
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
    filterSearch: FilterSearchType<RecordType>;
    tablePrefixCls: string;
    locale: TableLocale;
}
declare function FilterSearch<RecordType>({ value, onChange, filterSearch, tablePrefixCls, locale, }: FilterSearchProps<RecordType>): React.JSX.Element | null;
export default FilterSearch;
