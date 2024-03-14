import * as React from 'react';
import type { ColumnsType, ColumnType, Direction, FixedType, GetRowKey, Key, RenderExpandIcon, TriggerEventHandler } from '../../interface';
export declare function convertChildrenToColumns<RecordType>(children: React.ReactNode): ColumnsType<RecordType>;
/**
 * Parse `columns` & `children` into `columns`.
 */
declare function useColumns<RecordType>({ prefixCls, columns, children, expandable, expandedKeys, columnTitle, getRowKey, onTriggerExpand, expandIcon, rowExpandable, expandIconColumnIndex, direction, expandRowByClick, columnWidth, fixed, scrollWidth, clientWidth, }: {
    prefixCls?: string;
    columns?: ColumnsType<RecordType>;
    children?: React.ReactNode;
    expandable: boolean;
    expandedKeys: Set<Key>;
    columnTitle?: React.ReactNode;
    getRowKey: GetRowKey<RecordType>;
    onTriggerExpand: TriggerEventHandler<RecordType>;
    expandIcon?: RenderExpandIcon<RecordType>;
    rowExpandable?: (record: RecordType) => boolean;
    expandIconColumnIndex?: number;
    direction?: Direction;
    expandRowByClick?: boolean;
    columnWidth?: number | string;
    clientWidth: number;
    fixed?: FixedType;
    scrollWidth?: number;
}, transformColumns: (columns: ColumnsType<RecordType>) => ColumnsType<RecordType>): [
    columns: ColumnsType<RecordType>,
    flattenColumns: readonly ColumnType<RecordType>[],
    realScrollWidth: undefined | number,
    hasGapFixed: boolean
];
export default useColumns;
