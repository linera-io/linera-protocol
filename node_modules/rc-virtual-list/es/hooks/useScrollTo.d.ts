import * as React from 'react';
import type { GetKey } from '../interface';
import type CacheMap from '../utils/CacheMap';
export type ScrollAlign = 'top' | 'bottom' | 'auto';
export type ScrollPos = {
    left?: number;
    top?: number;
};
export type ScrollTarget = {
    index: number;
    align?: ScrollAlign;
    offset?: number;
} | {
    key: React.Key;
    align?: ScrollAlign;
    offset?: number;
};
export default function useScrollTo<T>(containerRef: React.RefObject<HTMLDivElement>, data: T[], heights: CacheMap, itemHeight: number, getKey: GetKey<T>, collectHeight: () => void, syncScrollTop: (newTop: number) => void, triggerFlash: () => void): (arg: number | ScrollTarget) => void;
