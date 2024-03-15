import type { InternalMarkObj } from '../Marks';
/** Format value align with step & marks */
type FormatValue = (value: number) => number;
type OffsetMode = 'unit' | 'dist';
export type OffsetValues = (values: number[], offset: number | 'min' | 'max', valueIndex: number, mode?: OffsetMode) => {
    value: number;
    values: number[];
};
export default function useOffset(min: number, max: number, step: number, markList: InternalMarkObj[], allowCross: boolean, pushable: false | number): [FormatValue, OffsetValues];
export {};
