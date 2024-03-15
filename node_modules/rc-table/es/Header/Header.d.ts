import * as React from 'react';
import type { ColumnsType, ColumnType, GetComponentProps, StickyOffsets } from '../interface';
export interface HeaderProps<RecordType> {
    columns: ColumnsType<RecordType>;
    flattenColumns: readonly ColumnType<RecordType>[];
    stickyOffsets: StickyOffsets;
    onHeaderRow: GetComponentProps<readonly ColumnType<RecordType>[]>;
}
declare const _default: <RecordType extends unknown>(props: HeaderProps<RecordType>) => React.JSX.Element;
export default _default;
