import type { IDialogPropTypes } from 'rc-dialog/lib/IDialogPropTypes';
import type { GetContainer } from 'rc-util/lib/PortalWrapper';
import * as React from 'react';
import type { TransformType } from './hooks/useImageTransform';
import type { PreviewProps, ToolbarRenderInfoType } from './Preview';
import PreviewGroup from './PreviewGroup';
export interface ImagePreviewType extends Omit<IDialogPropTypes, 'mask' | 'visible' | 'closable' | 'prefixCls' | 'onClose' | 'afterClose' | 'wrapClassName'> {
    src?: string;
    visible?: boolean;
    minScale?: number;
    maxScale?: number;
    onVisibleChange?: (value: boolean, prevValue: boolean) => void;
    getContainer?: GetContainer | false;
    mask?: React.ReactNode;
    maskClassName?: string;
    icons?: PreviewProps['icons'];
    scaleStep?: number;
    movable?: boolean;
    imageRender?: (originalNode: React.ReactElement, info: {
        transform: TransformType;
    }) => React.ReactNode;
    onTransform?: PreviewProps['onTransform'];
    toolbarRender?: (originalNode: React.ReactElement, info: Omit<ToolbarRenderInfoType, 'current' | 'total'>) => React.ReactNode;
}
export interface ImageProps extends Omit<React.ImgHTMLAttributes<HTMLImageElement>, 'placeholder' | 'onClick'> {
    src?: string;
    wrapperClassName?: string;
    wrapperStyle?: React.CSSProperties;
    prefixCls?: string;
    previewPrefixCls?: string;
    placeholder?: React.ReactNode;
    fallback?: string;
    rootClassName?: string;
    preview?: boolean | ImagePreviewType;
    /**
     * @deprecated since version 3.2.1
     */
    onPreviewClose?: (value: boolean, prevValue: boolean) => void;
    onClick?: (e: React.MouseEvent<HTMLDivElement>) => void;
    onError?: (e: React.SyntheticEvent<HTMLImageElement, Event>) => void;
}
interface CompoundedComponent<P> extends React.FC<P> {
    PreviewGroup: typeof PreviewGroup;
}
declare const ImageInternal: CompoundedComponent<ImageProps>;
export default ImageInternal;
