import { EXPAND_COLUMN, Summary } from 'rc-table';
import Column from './Column';
import ColumnGroup from './ColumnGroup';
import { SELECTION_ALL, SELECTION_COLUMN, SELECTION_INVERT, SELECTION_NONE } from './hooks/useSelection';
import type { RefTable } from './interface';
declare const ForwardTable: RefTable & {
    displayName?: string | undefined;
    SELECTION_COLUMN: typeof SELECTION_COLUMN;
    EXPAND_COLUMN: typeof EXPAND_COLUMN;
    SELECTION_ALL: typeof SELECTION_ALL;
    SELECTION_INVERT: typeof SELECTION_INVERT;
    SELECTION_NONE: typeof SELECTION_NONE;
    Column: typeof Column;
    ColumnGroup: typeof ColumnGroup;
    Summary: typeof Summary;
};
export default ForwardTable;
