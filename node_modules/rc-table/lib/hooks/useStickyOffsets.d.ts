import type { ColumnType, Direction, StickyOffsets } from '../interface';
/**
 * Get sticky column offset width
 */
declare function useStickyOffsets<RecordType>(colWidths: number[], flattenColumns: readonly ColumnType<RecordType>[], direction: Direction): StickyOffsets;
export default useStickyOffsets;
