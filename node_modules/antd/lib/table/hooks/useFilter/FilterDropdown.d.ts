import * as React from 'react';
import type { FieldDataNode } from 'rc-tree';
import type { FilterState } from '.';
import type { ColumnFilterItem, ColumnType, FilterSearchType, FilterValue, GetPopupContainer, Key, TableLocale } from '../../interface';
type FilterTreeDataNode = FieldDataNode<{
    title: React.ReactNode;
    key: string;
}>;
export declare function flattenKeys(filters?: ColumnFilterItem[]): FilterValue;
export type TreeColumnFilterItem = ColumnFilterItem & FilterTreeDataNode;
export interface FilterDropdownProps<RecordType> {
    tablePrefixCls: string;
    prefixCls: string;
    dropdownPrefixCls: string;
    column: ColumnType<RecordType>;
    filterState?: FilterState<RecordType>;
    filterOnClose: boolean;
    filterMultiple: boolean;
    filterMode?: 'menu' | 'tree';
    filterSearch?: FilterSearchType<ColumnFilterItem | TreeColumnFilterItem>;
    columnKey: Key;
    children: React.ReactNode;
    triggerFilter: (filterState: FilterState<RecordType>) => void;
    locale: TableLocale;
    getPopupContainer?: GetPopupContainer;
    filterResetToDefaultFilteredValue?: boolean;
    rootClassName?: string;
}
declare function FilterDropdown<RecordType>(props: FilterDropdownProps<RecordType>): React.JSX.Element;
export default FilterDropdown;
