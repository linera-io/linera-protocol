import * as React from 'react';
import useRowInfo from '../hooks/useRowInfo';
import type { ColumnType, CustomizeComponent, GetRowKey } from '../interface';
export interface BodyRowProps<RecordType> {
    record: RecordType;
    index: number;
    renderIndex: number;
    className?: string;
    style?: React.CSSProperties;
    rowComponent: CustomizeComponent;
    cellComponent: CustomizeComponent;
    scopeCellComponent: CustomizeComponent;
    indent?: number;
    rowKey: React.Key;
    getRowKey: GetRowKey<RecordType>;
}
export declare function getCellProps<RecordType>(rowInfo: ReturnType<typeof useRowInfo<RecordType>>, column: ColumnType<RecordType>, colIndex: number, indent: number, index: number): {
    key: React.Key;
    fixedInfo: import("../utils/fixUtil").FixedInfo;
    appendCellNode: React.ReactNode;
    additionalCellProps: React.TdHTMLAttributes<HTMLElement>;
};
declare function BodyRow<RecordType extends {
    children?: readonly RecordType[];
}>(props: BodyRowProps<RecordType>): React.JSX.Element;
declare namespace BodyRow {
    var displayName: string;
}
declare const _default: typeof BodyRow;
export default _default;
