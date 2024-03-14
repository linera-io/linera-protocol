import type { DialogProps as IDialogPropTypes } from 'rc-dialog';
import React from 'react';
import type { TransformAction, TransformType } from './hooks/useImageTransform';
export type ToolbarRenderInfoType = {
    icons: {
        flipYIcon: React.ReactNode;
        flipXIcon: React.ReactNode;
        rotateLeftIcon: React.ReactNode;
        rotateRightIcon: React.ReactNode;
        zoomOutIcon: React.ReactNode;
        zoomInIcon: React.ReactNode;
    };
    actions: {
        onFlipY: () => void;
        onFlipX: () => void;
        onRotateLeft: () => void;
        onRotateRight: () => void;
        onZoomOut: () => void;
        onZoomIn: () => void;
    };
    transform: TransformType;
    current: number;
    total: number;
};
export interface PreviewProps extends Omit<IDialogPropTypes, 'onClose'> {
    imgCommonProps?: React.ImgHTMLAttributes<HTMLImageElement>;
    src?: string;
    alt?: string;
    fallback?: string;
    movable?: boolean;
    rootClassName?: string;
    icons?: {
        rotateLeft?: React.ReactNode;
        rotateRight?: React.ReactNode;
        zoomIn?: React.ReactNode;
        zoomOut?: React.ReactNode;
        close?: React.ReactNode;
        left?: React.ReactNode;
        right?: React.ReactNode;
        flipX?: React.ReactNode;
        flipY?: React.ReactNode;
    };
    current?: number;
    count?: number;
    closeIcon?: React.ReactNode;
    countRender?: (current: number, total: number) => React.ReactNode;
    scaleStep?: number;
    minScale?: number;
    maxScale?: number;
    imageRender?: (originalNode: React.ReactElement, info: {
        transform: TransformType;
        current?: number;
    }) => React.ReactNode;
    onClose?: () => void;
    onTransform?: (info: {
        transform: TransformType;
        action: TransformAction;
    }) => void;
    toolbarRender?: (originalNode: React.ReactElement, info: ToolbarRenderInfoType) => React.ReactNode;
    onChange?: (current: any, prev: any) => void;
}
declare const Preview: React.FC<PreviewProps>;
export default Preview;
