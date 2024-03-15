import type { TooltipProps } from '../../tooltip';
import type { ColumnsType, ColumnTitleProps, ColumnType, Key, SorterResult, SortOrder, TableLocale, TransformColumns } from '../interface';
export interface SortState<RecordType> {
    column: ColumnType<RecordType>;
    key: Key;
    sortOrder: SortOrder | null;
    multiplePriority: number | false;
}
export declare function getSortData<RecordType>(data: readonly RecordType[], sortStates: SortState<RecordType>[], childrenColumnName: string): RecordType[];
interface SorterConfig<RecordType> {
    prefixCls: string;
    mergedColumns: ColumnsType<RecordType>;
    onSorterChange: (sorterResult: SorterResult<RecordType> | SorterResult<RecordType>[], sortStates: SortState<RecordType>[]) => void;
    sortDirections: SortOrder[];
    tableLocale?: TableLocale;
    showSorterTooltip?: boolean | TooltipProps;
}
export default function useFilterSorter<RecordType>({ prefixCls, mergedColumns, onSorterChange, sortDirections, tableLocale, showSorterTooltip, }: SorterConfig<RecordType>): [
    TransformColumns<RecordType>,
    SortState<RecordType>[],
    ColumnTitleProps<RecordType>,
    () => SorterResult<RecordType> | SorterResult<RecordType>[]
];
export {};
