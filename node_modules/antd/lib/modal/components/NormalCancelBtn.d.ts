import type { FC } from 'react';
import React from 'react';
import type { ModalProps } from '../interface';
export interface NormalCancelBtnProps extends Pick<ModalProps, 'cancelButtonProps' | 'onCancel'> {
    cancelTextLocale?: string | number | true | React.ReactElement<any, string | React.JSXElementConstructor<any>> | Iterable<React.ReactNode>;
}
declare const NormalCancelBtn: FC;
export default NormalCancelBtn;
