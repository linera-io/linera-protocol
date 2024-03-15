import React from 'react';
import type { ModalProps } from './interface';
export declare function renderCloseIcon(prefixCls: string, closeIcon?: React.ReactNode): React.JSX.Element;
interface FooterProps {
    onOk?: React.MouseEventHandler<HTMLButtonElement | HTMLAnchorElement>;
    onCancel?: React.MouseEventHandler<HTMLButtonElement | HTMLAnchorElement>;
}
export declare const Footer: React.FC<FooterProps & Pick<ModalProps, 'footer' | 'okText' | 'okType' | 'cancelText' | 'confirmLoading' | 'okButtonProps' | 'cancelButtonProps'>>;
export {};
