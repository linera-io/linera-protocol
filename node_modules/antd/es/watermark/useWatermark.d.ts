import * as React from 'react';
/**
 * Base size of the canvas, 1 for parallel layout and 2 for alternate layout
 * Only alternate layout is currently supported
 */
export declare const BaseSize = 2;
export declare const FontGap = 3;
export type AppendWatermark = (base64Url: string, markWidth: number, container: HTMLElement) => void;
export default function useWatermark(markStyle: React.CSSProperties): [
    appendWatermark: AppendWatermark,
    removeWatermark: (container: HTMLElement) => void,
    isWatermarkEle: (ele: Node) => boolean
];
