import type * as React from 'react';
import type { ColumnProps } from './Column';
import type { ColumnType } from './interface';
export interface ColumnGroupProps<RecordType> extends Omit<ColumnType<RecordType>, 'children'> {
    children: React.ReactElement<ColumnProps<RecordType>> | React.ReactElement<ColumnProps<RecordType>>[];
}
/** This is a syntactic sugar for `columns` prop. So HOC will not work on this. */
declare function ColumnGroup<RecordType>(_: ColumnGroupProps<RecordType>): null;
export default ColumnGroup;
