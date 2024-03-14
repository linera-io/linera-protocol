import type { FC } from 'react';
import React from 'react';
import type { ConfirmDialogProps } from '../ConfirmDialog';
export interface ConfirmCancelBtnProps extends Pick<ConfirmDialogProps, 'cancelButtonProps' | 'isSilent' | 'rootPrefixCls' | 'close' | 'onConfirm' | 'onCancel'> {
    autoFocusButton?: false | 'ok' | 'cancel' | null;
    cancelTextLocale?: string | number | true | React.ReactElement<any, string | React.JSXElementConstructor<any>> | Iterable<React.ReactNode>;
    mergedOkCancel?: boolean;
}
declare const ConfirmCancelBtn: FC;
export default ConfirmCancelBtn;
