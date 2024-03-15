import { type TableProps as RcTableProps } from 'rc-table';
import type { SizeType } from '../config-provider/SizeContext';
import type { SpinProps } from '../spin';
import type { TooltipProps } from '../tooltip';
import type { ColumnsType, FilterValue, GetPopupContainer, RefInternalTable, SorterResult, SortOrder, TableCurrentDataSource, TableLocale, TablePaginationConfig, TableRowSelection } from './interface';
export type { ColumnsType, TablePaginationConfig };
/** Same as `TableProps` but we need record parent render times */
export interface InternalTableProps<RecordType> extends TableProps<RecordType> {
    _renderTimes: number;
}
export interface TableProps<RecordType = any> extends Omit<RcTableProps<RecordType>, 'transformColumns' | 'internalHooks' | 'internalRefs' | 'data' | 'columns' | 'scroll' | 'emptyText'> {
    dropdownPrefixCls?: string;
    dataSource?: RcTableProps<RecordType>['data'];
    columns?: ColumnsType<RecordType>;
    pagination?: false | TablePaginationConfig;
    loading?: boolean | SpinProps;
    size?: SizeType;
    bordered?: boolean;
    locale?: TableLocale;
    rootClassName?: string;
    onChange?: (pagination: TablePaginationConfig, filters: Record<string, FilterValue | null>, sorter: SorterResult<RecordType> | SorterResult<RecordType>[], extra: TableCurrentDataSource<RecordType>) => void;
    rowSelection?: TableRowSelection<RecordType>;
    getPopupContainer?: GetPopupContainer;
    scroll?: RcTableProps<RecordType>['scroll'] & {
        scrollToFirstRowOnChange?: boolean;
    };
    sortDirections?: SortOrder[];
    showSorterTooltip?: boolean | TooltipProps;
    virtual?: boolean;
}
declare const _default: RefInternalTable;
export default _default;
