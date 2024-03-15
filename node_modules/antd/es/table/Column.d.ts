import type { ColumnType } from './interface';
export interface ColumnProps<RecordType> extends ColumnType<RecordType> {
    children?: null;
}
/** This is a syntactic sugar for `columns` prop. So HOC will not work on this. */
declare function Column<RecordType>(_: ColumnProps<RecordType>): null;
export default Column;
