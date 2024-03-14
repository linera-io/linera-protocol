import React from 'react';
import type { ConfirmCancelBtnProps } from './components/ConfirmCancelBtn';
import type { ConfirmOkBtnProps } from './components/ConfirmOkBtn';
import type { NormalCancelBtnProps } from './components/NormalCancelBtn';
import type { NormalOkBtnProps } from './components/NormalOkBtn';
export type ModalContextProps = NormalCancelBtnProps & NormalOkBtnProps & ConfirmOkBtnProps & ConfirmCancelBtnProps;
export declare const ModalContext: React.Context<ModalContextProps>;
export declare const ModalContextProvider: React.Provider<ModalContextProps>;
