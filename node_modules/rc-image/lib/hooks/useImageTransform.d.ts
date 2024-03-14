/// <reference types="react" />
export type TransformType = {
    x: number;
    y: number;
    rotate: number;
    scale: number;
    flipX: boolean;
    flipY: boolean;
};
export type TransformAction = 'flipY' | 'flipX' | 'rotateLeft' | 'rotateRight' | 'zoomIn' | 'zoomOut' | 'close' | 'prev' | 'next' | 'wheel' | 'doubleClick' | 'move' | 'dragRebound' | 'touchZoom';
export type UpdateTransformFunc = (newTransform: Partial<TransformType>, action: TransformAction) => void;
export type DispatchZoomChangeFunc = (ratio: number, action: TransformAction, centerX?: number, centerY?: number, isTouch?: boolean) => void;
export default function useImageTransform(imgRef: React.MutableRefObject<HTMLImageElement>, minScale: number, maxScale: number, onTransform: (info: {
    transform: TransformType;
    action: TransformAction;
}) => void): {
    transform: {
        x: number;
        y: number;
        rotate: number;
        scale: number;
        flipX: boolean;
        flipY: boolean;
    };
    resetTransform: (action: TransformAction) => void;
    updateTransform: UpdateTransformFunc;
    dispatchZoomChange: DispatchZoomChangeFunc;
};
