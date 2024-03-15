declare const FORMAT_KEYS: string[];
export type FormatKey = (typeof FORMAT_KEYS)[number];
export interface Cell {
    text: string;
    mask: boolean;
    start: number;
    end: number;
}
export default class MaskFormat {
    format: string;
    maskFormat: string;
    cells: Cell[];
    maskCells: Cell[];
    constructor(format: string);
    getSelection(maskCellIndex: number): [start: number, end: number];
    /** Check given text match format */
    match(text: string): boolean;
    /** Get mask cell count */
    size(): number;
    getMaskCellIndex(anchorIndex: number): number;
}
export {};
