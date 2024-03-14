import type * as React from 'react';
/**
 * Get index with specific start index one by one. e.g.
 * min: 3, max: 9, start: 6
 *
 * Return index is:
 * [0]: 6
 * [1]: 7
 * [2]: 5
 * [3]: 8
 * [4]: 4
 * [5]: 9
 * [6]: 3
 */
export declare function getIndexByStartLoc(min: number, max: number, start: number, index: number): number;
/**
 * We assume that 2 list has only 1 item diff and others keeping the order.
 * So we can use dichotomy algorithm to find changed one.
 */
export declare function findListDiffIndex<T>(originList: T[], targetList: T[], getKey: (item: T) => React.Key): {
    index: number;
    multiple: boolean;
} | null;
