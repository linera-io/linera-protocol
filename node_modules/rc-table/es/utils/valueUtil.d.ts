/// <reference types="react" />
import type { DataIndex, Key } from '../interface';
interface GetColumnKeyColumn {
    key?: Key;
    dataIndex?: DataIndex;
}
export declare function getColumnsKey(columns: readonly GetColumnKeyColumn[]): import("react").Key[];
export declare function validateValue<T>(val: T): boolean;
export {};
