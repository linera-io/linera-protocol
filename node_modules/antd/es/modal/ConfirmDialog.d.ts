import * as React from 'react';
import type { ThemeConfig } from '../config-provider';
import type { ModalFuncProps, ModalLocale } from './interface';
export interface ConfirmDialogProps extends ModalFuncProps {
    prefixCls: string;
    afterClose?: () => void;
    close?: (...args: any[]) => void;
    /**
     * `close` prop support `...args` that pass to the developer
     * that we can not break this.
     * Provider `onClose` for internal usage
     */
    onConfirm?: (confirmed: boolean) => void;
    autoFocusButton?: null | 'ok' | 'cancel';
    rootPrefixCls?: string;
    iconPrefixCls?: string;
    /**
     * Only passed by static method
     */
    theme?: ThemeConfig;
    /** @private Internal Usage. Do not override this */
    locale?: ModalLocale;
    /**
     * Do not throw if is await mode
     */
    isSilent?: () => boolean;
}
export declare function ConfirmContent(props: ConfirmDialogProps & {
    confirmPrefixCls: string;
}): React.JSX.Element;
declare const ConfirmDialogWrapper: React.FC<ConfirmDialogProps>;
export default ConfirmDialogWrapper;
