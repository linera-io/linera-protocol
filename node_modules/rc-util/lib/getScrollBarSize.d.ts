type ScrollBarSize = {
    width: number;
    height: number;
};
export default function getScrollBarSize(fresh?: boolean): number;
export declare function getTargetScrollBarSize(target: HTMLElement): ScrollBarSize;
export {};
