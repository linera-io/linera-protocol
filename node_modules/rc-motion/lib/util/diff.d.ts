/// <reference types="react" />
export declare const STATUS_ADD: "add";
export declare const STATUS_KEEP: "keep";
export declare const STATUS_REMOVE: "remove";
export declare const STATUS_REMOVED: "removed";
export type DiffStatus = typeof STATUS_ADD | typeof STATUS_KEEP | typeof STATUS_REMOVE | typeof STATUS_REMOVED;
export interface KeyObject {
    key: React.Key;
    status?: DiffStatus;
}
export declare function wrapKeyToObject(key: React.Key | KeyObject): {
    key: string;
    status?: DiffStatus;
};
export declare function parseKeys(keys?: any[]): {
    key: string;
    status?: DiffStatus;
}[];
export declare function diffKeys(prevKeys?: KeyObject[], currentKeys?: KeyObject[]): KeyObject[];
