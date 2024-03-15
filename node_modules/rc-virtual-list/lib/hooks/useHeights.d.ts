import type { GetKey } from '../interface';
import CacheMap from '../utils/CacheMap';
export default function useHeights<T>(getKey: GetKey<T>, onItemAdd?: (item: T) => void, onItemRemove?: (item: T) => void): [
    setInstanceRef: (item: T, instance: HTMLElement) => void,
    collectHeight: (sync?: boolean) => void,
    cacheMap: CacheMap,
    updatedMark: number
];
