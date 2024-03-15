/// <reference types="react" />
import type { TableContextProps } from '../context/TableContext';
export default function useRowInfo<RecordType>(record: RecordType, rowKey: React.Key, recordIndex: number, indent: number): Pick<TableContextProps, 'prefixCls' | 'fixedInfoList' | 'flattenColumns' | 'expandableType' | 'expandRowByClick' | 'onTriggerExpand' | 'rowClassName' | 'expandedRowClassName' | 'indentSize' | 'expandIcon' | 'expandedRowRender' | 'expandIconColumnIndex' | 'expandedKeys' | 'childrenColumnName' | 'onRow'> & {
    columnsKey: React.Key[];
    nestExpandable: boolean;
    expanded: boolean;
    hasNestChildren: boolean;
    record: RecordType;
    rowSupportExpand: boolean;
    expandable: boolean;
    rowProps: React.HTMLAttributes<any> & React.TdHTMLAttributes<any>;
};
