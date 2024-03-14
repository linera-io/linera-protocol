import type React from 'react';
import type { TransformType, UpdateTransformFunc, DispatchZoomChangeFunc } from './useImageTransform';
export default function useMouseEvent(imgRef: React.MutableRefObject<HTMLImageElement>, movable: boolean, visible: boolean, scaleStep: number, transform: TransformType, updateTransform: UpdateTransformFunc, dispatchZoomChange: DispatchZoomChangeFunc): {
    isMoving: boolean;
    onMouseDown: React.MouseEventHandler<HTMLDivElement>;
    onMouseMove: React.MouseEventHandler<HTMLBodyElement>;
    onMouseUp: React.MouseEventHandler<HTMLBodyElement>;
    onWheel: (event: React.WheelEvent<HTMLImageElement>) => void;
};
