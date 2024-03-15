export declare const ATTR_CACHE_MAP = "data-ant-cssinjs-cache-path";
/**
 * This marks style from the css file.
 * Which means not exist in `<style />` tag.
 */
export declare const CSS_FILE_STYLE = "_FILE_STYLE__";
export declare function serialize(cachePathMap: Record<string, string>): string;
/**
 * @private Test usage only. Can save remove if no need.
 */
export declare function reset(mockCache?: Record<string, string>, fromFile?: boolean): void;
export declare function prepare(): void;
export declare function existPath(path: string): boolean;
export declare function getStyleAndHash(path: string): [style: string | null, hash: string];
