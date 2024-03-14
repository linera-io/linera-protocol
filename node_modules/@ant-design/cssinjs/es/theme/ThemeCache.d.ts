import type Theme from './Theme';
import type { DerivativeFunc } from './interface';
type DerivativeOptions = DerivativeFunc<any, any>[];
export declare function sameDerivativeOption(left: DerivativeOptions, right: DerivativeOptions): boolean;
export default class ThemeCache {
    static MAX_CACHE_SIZE: number;
    static MAX_CACHE_OFFSET: number;
    private readonly cache;
    private keys;
    private cacheCallTimes;
    constructor();
    size(): number;
    private internalGet;
    get(derivativeOption: DerivativeOptions): Theme<any, any> | undefined;
    has(derivativeOption: DerivativeOptions): boolean;
    set(derivativeOption: DerivativeOptions, value: Theme<any, any>): void;
    private deleteByPath;
    delete(derivativeOption: DerivativeOptions): Theme<any, any> | undefined;
}
export {};
