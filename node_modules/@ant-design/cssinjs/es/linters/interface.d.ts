export interface LinterInfo {
    path?: string;
    hashId?: string;
    parentSelectors: string[];
}
export interface Linter {
    (key: string, value: string | number, info: LinterInfo): void;
}
