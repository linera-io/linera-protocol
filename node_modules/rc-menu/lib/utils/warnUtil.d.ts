/// <reference types="react" />
/**
 * `onClick` event return `info.item` which point to react node directly.
 * We should warning this since it will not work on FC.
 */
export declare function warnItemProp<T extends {
    item: React.ReactInstance;
}>({ item, ...restInfo }: T): T;
