import { type KeyType } from '../Cache';
export type ExtractStyle<CacheValue> = (cache: CacheValue, effectStyles: Record<string, boolean>, options?: {
    plain?: boolean;
}) => [order: number, styleId: string, style: string] | null;
export default function useGlobalCache<CacheType>(prefix: string, keyPath: KeyType[], cacheFn: () => CacheType, onCacheRemove?: (cache: CacheType, fromHMR: boolean) => void, onCacheEffect?: (cachedValue: CacheType) => void): CacheType;
