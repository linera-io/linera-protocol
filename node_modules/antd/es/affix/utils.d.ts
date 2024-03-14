export type BindElement = HTMLElement | Window | null | undefined;
export declare function getTargetRect(target: BindElement): DOMRect;
export declare function getFixedTop(placeholderRect: DOMRect, targetRect: DOMRect, offsetTop?: number): number | undefined;
export declare function getFixedBottom(placeholderRect: DOMRect, targetRect: DOMRect, offsetBottom?: number): number | undefined;
