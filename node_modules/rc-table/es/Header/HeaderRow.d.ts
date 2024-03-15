import * as React from 'react';
import type { CellType, ColumnType, CustomizeComponent, GetComponentProps, StickyOffsets } from '../interface';
export interface RowProps<RecordType> {
    cells: readonly CellType<RecordType>[];
    stickyOffsets: StickyOffsets;
    flattenColumns: readonly ColumnType<RecordType>[];
    rowComponent: CustomizeComponent;
    cellComponent: CustomizeComponent;
    onHeaderRow: GetComponentProps<readonly ColumnType<RecordType>[]>;
    index: number;
}
declare const HeaderRow: {
    <RecordType extends unknown>(props: RowProps<RecordType>): React.JSX.Element;
    displayName: string;
};
export default HeaderRow;
