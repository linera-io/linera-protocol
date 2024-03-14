import * as React from 'react';
import type { TransformType } from './hooks/useImageTransform';
import type { PreviewProps, ToolbarRenderInfoType } from './Preview';
interface OperationsProps extends Pick<PreviewProps, 'visible' | 'maskTransitionName' | 'getContainer' | 'prefixCls' | 'rootClassName' | 'icons' | 'countRender' | 'closeIcon' | 'onClose'> {
    showSwitch: boolean;
    showProgress: boolean;
    current: number;
    transform: TransformType;
    count: number;
    scale: number;
    minScale: number;
    maxScale: number;
    onSwitchLeft: React.MouseEventHandler<HTMLDivElement>;
    onSwitchRight: React.MouseEventHandler<HTMLDivElement>;
    onZoomIn: () => void;
    onZoomOut: () => void;
    onRotateRight: () => void;
    onRotateLeft: () => void;
    onFlipX: () => void;
    onFlipY: () => void;
    toolbarRender: (originalNode: React.ReactElement, info: ToolbarRenderInfoType | Omit<ToolbarRenderInfoType, 'current' | 'total'>) => React.ReactNode;
    zIndex?: number;
}
declare const Operations: React.FC<OperationsProps>;
export default Operations;
