import type React from 'react';
declare class CacheMap {
    maps: Record<string, number>;
    id: number;
    constructor();
    set(key: React.Key, value: number): void;
    get(key: React.Key): number;
}
export default CacheMap;
