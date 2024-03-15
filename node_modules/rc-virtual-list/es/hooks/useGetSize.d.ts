import type CacheMap from '../utils/CacheMap';
import type { GetKey, GetSize } from '../interface';
/**
 * Size info need loop query for the `heights` which will has the perf issue.
 * Let cache result for each render phase.
 */
export declare function useGetSize<T>(mergedData: T[], getKey: GetKey<T>, heights: CacheMap, itemHeight: number): GetSize;
