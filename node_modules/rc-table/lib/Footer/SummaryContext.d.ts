import * as React from 'react';
import type { ColumnType, StickyOffsets } from '../interface';
type FlattenColumns<RecordType> = readonly (ColumnType<RecordType> & {
    scrollbar?: boolean;
})[];
declare const SummaryContext: React.Context<{
    stickyOffsets?: StickyOffsets;
    scrollColumnIndex?: number;
    flattenColumns?: FlattenColumns<any>;
}>;
export default SummaryContext;
