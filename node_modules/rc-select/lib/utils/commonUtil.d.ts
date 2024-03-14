import type { DisplayValueType } from '../BaseSelect';
export declare function toArray<T>(value: T | T[]): T[];
export declare const isClient: HTMLElement;
/** Is client side and not jsdom */
export declare const isBrowserClient: HTMLElement;
export declare function hasValue(value: any): boolean;
/** combo mode no value judgment function */
export declare function isComboNoValue(value: any): boolean;
export declare function getTitle(item: DisplayValueType): string;
