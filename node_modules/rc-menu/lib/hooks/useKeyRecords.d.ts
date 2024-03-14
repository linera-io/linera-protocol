export declare const OVERFLOW_KEY = "rc-menu-more";
export default function useKeyRecords(): {
    registerPath: (key: string, keyPath: string[]) => void;
    unregisterPath: (key: string, keyPath: string[]) => void;
    refreshOverflowKeys: (keys: string[]) => void;
    isSubPathKey: (pathKeys: string[], eventKey: string) => boolean;
    getKeyPath: (eventKey: string, includeOverflow?: boolean) => string[];
    getKeys: () => string[];
    getSubPathKeys: (key: string) => Set<string>;
};
