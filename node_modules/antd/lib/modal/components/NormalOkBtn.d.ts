import type { FC } from 'react';
import React from 'react';
import type { ModalProps } from '../interface';
export interface NormalOkBtnProps extends Pick<ModalProps, 'confirmLoading' | 'okType' | 'okButtonProps' | 'onOk'> {
    okTextLocale?: string | number | true | React.ReactElement<any, string | React.JSXElementConstructor<any>> | Iterable<React.ReactNode>;
}
declare const NormalOkBtn: FC;
export default NormalOkBtn;
