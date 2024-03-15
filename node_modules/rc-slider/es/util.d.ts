/// <reference types="react" />
import type { Direction } from './interface';
export declare function getOffset(value: number, min: number, max: number): number;
export declare function getDirectionStyle(direction: Direction, value: number, min: number, max: number): import("react").CSSProperties;
/** Return index value if is list or return value directly */
export declare function getIndex<T>(value: T | T[], index: number): T;
