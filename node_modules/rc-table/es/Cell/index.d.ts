import * as React from 'react';
import type { AlignType, CellEllipsisType, ColumnType, CustomizeComponent, DataIndex, DefaultRecordType, ScopeType } from '../interface';
export interface CellProps<RecordType extends DefaultRecordType> {
    prefixCls?: string;
    className?: string;
    record?: RecordType;
    /** `column` index is the real show rowIndex */
    index?: number;
    /** the index of the record. For the render(value, record, renderIndex) */
    renderIndex?: number;
    dataIndex?: DataIndex;
    render?: ColumnType<RecordType>['render'];
    component?: CustomizeComponent;
    children?: React.ReactNode;
    colSpan?: number;
    rowSpan?: number;
    scope?: ScopeType;
    ellipsis?: CellEllipsisType;
    align?: AlignType;
    shouldCellUpdate?: (record: RecordType, prevRecord: RecordType) => boolean;
    fixLeft?: number | false;
    fixRight?: number | false;
    firstFixLeft?: boolean;
    lastFixLeft?: boolean;
    firstFixRight?: boolean;
    lastFixRight?: boolean;
    allColsFixedLeft?: boolean;
    /** @private Used for `expandable` with nest tree */
    appendNode?: React.ReactNode;
    additionalProps?: React.TdHTMLAttributes<HTMLTableCellElement>;
    rowType?: 'header' | 'body' | 'footer';
    isSticky?: boolean;
}
declare function Cell<RecordType>(props: CellProps<RecordType>): React.JSX.Element;
declare const _default: React.MemoExoticComponent<typeof Cell>;
export default _default;
