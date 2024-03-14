import type { DefaultOptionType, FieldNames, InternalFieldNames, SingleValueType, ValueType } from '../Cascader';
export declare const VALUE_SPLIT = "__RC_CASCADER_SPLIT__";
export declare const SHOW_PARENT = "SHOW_PARENT";
export declare const SHOW_CHILD = "SHOW_CHILD";
/**
 * Will convert value to string, and join with `VALUE_SPLIT`
 */
export declare function toPathKey(value: SingleValueType): string;
/**
 * Batch convert value to string, and join with `VALUE_SPLIT`
 */
export declare function toPathKeys(value: SingleValueType[]): string[];
export declare function toPathValueStr(pathKey: string): string[];
export declare function fillFieldNames(fieldNames?: FieldNames): InternalFieldNames;
export declare function isLeaf(option: DefaultOptionType, fieldNames: FieldNames): any;
export declare function scrollIntoParentView(element: HTMLElement): void;
export declare function getFullPathKeys(options: DefaultOptionType[], fieldNames: FieldNames): any[];
export declare function toRawValues(value: ValueType): SingleValueType[];
