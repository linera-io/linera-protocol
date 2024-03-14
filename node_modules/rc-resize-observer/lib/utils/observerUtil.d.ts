export type ResizeListener = (element: Element) => void;
declare function onResize(entities: ResizeObserverEntry[]): void;
export declare const _el: Map<Element, Set<ResizeListener>>;
export declare const _rs: typeof onResize;
export declare function observe(element: Element, callback: ResizeListener): void;
export declare function unobserve(element: Element, callback: ResizeListener): void;
export {};
