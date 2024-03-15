import type { ColumnType, Direction, StickyOffsets } from '../interface';
export default function useFixedInfo<RecordType>(flattenColumns: readonly ColumnType<RecordType>[], stickyOffsets: StickyOffsets, direction: Direction): import("../utils/fixUtil").FixedInfo[];
