import type { CompareProps } from '@rc-component/context/lib/Immutable';
import * as React from 'react';
import type { Reference } from '../interface';
import Table, { type TableProps } from '../Table';
export interface VirtualTableProps<RecordType> extends Omit<TableProps<RecordType>, 'scroll'> {
    scroll: {
        x?: number;
        y: number;
    };
    listItemHeight?: number;
}
export type ForwardGenericVirtualTable = (<RecordType>(props: TableProps<RecordType> & React.RefAttributes<Reference>) => React.ReactElement) & {
    displayName?: string;
};
export declare function genVirtualTable(shouldTriggerRender?: CompareProps<typeof Table>): ForwardGenericVirtualTable;
declare const _default: ForwardGenericVirtualTable;
export default _default;
