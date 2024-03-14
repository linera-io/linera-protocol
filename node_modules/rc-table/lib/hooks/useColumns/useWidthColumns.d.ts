import type { ColumnsType } from '../../interface';
/**
 * Fill all column with width
 */
export default function useWidthColumns(flattenColumns: ColumnsType<any>, scrollWidth: number, clientWidth: number): [columns: ColumnsType<any>, realScrollWidth: number];
