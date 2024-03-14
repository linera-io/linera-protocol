import type { Direction, FixedType, StickyOffsets } from '../interface';
export interface FixedInfo {
    fixLeft: number | false;
    fixRight: number | false;
    lastFixLeft: boolean;
    firstFixRight: boolean;
    lastFixRight: boolean;
    firstFixLeft: boolean;
    isSticky: boolean;
}
export declare function getCellFixedInfo(colStart: number, colEnd: number, columns: readonly {
    fixed?: FixedType;
}[], stickyOffsets: StickyOffsets, direction: Direction): FixedInfo;
