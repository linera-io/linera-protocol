import type { ColumnsType, ColumnType, FilterKey, FilterValue, GetPopupContainer, Key, TableLocale, TransformColumns } from '../../interface';
import { flattenKeys } from './FilterDropdown';
export interface FilterState<RecordType> {
    column: ColumnType<RecordType>;
    key: Key;
    filteredKeys?: FilterKey;
    forceFiltered?: boolean;
}
export declare function getFilterData<RecordType>(data: RecordType[], filterStates: FilterState<RecordType>[], childrenColumnName: string): RecordType[];
interface FilterConfig<RecordType> {
    prefixCls: string;
    dropdownPrefixCls: string;
    mergedColumns: ColumnsType<RecordType>;
    locale: TableLocale;
    onFilterChange: (filters: Record<string, FilterValue | null>, filterStates: FilterState<RecordType>[]) => void;
    getPopupContainer?: GetPopupContainer;
    rootClassName?: string;
}
declare function useFilter<RecordType>({ prefixCls, dropdownPrefixCls, mergedColumns: rawMergedColumns, onFilterChange, getPopupContainer, locale: tableLocale, rootClassName, }: FilterConfig<RecordType>): [
    TransformColumns<RecordType>,
    FilterState<RecordType>[],
    Record<string, FilterValue | null>
];
export { flattenKeys };
export default useFilter;
