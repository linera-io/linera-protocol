"use client";

import React, { useContext } from 'react';
import ActionButton from '../../_util/ActionButton';
import { ModalContext } from '../context';
const ConfirmOkBtn = () => {
  const {
    autoFocusButton,
    close,
    isSilent,
    okButtonProps,
    rootPrefixCls,
    okTextLocale,
    okType,
    onConfirm,
    onOk
  } = useContext(ModalContext);
  return /*#__PURE__*/React.createElement(ActionButton, {
    isSilent: isSilent,
    type: okType || 'primary',
    actionFn: onOk,
    close: function () {
      close === null || close === void 0 ? void 0 : close.apply(void 0, arguments);
      onConfirm === null || onConfirm === void 0 ? void 0 : onConfirm(true);
    },
    autoFocus: autoFocusButton === 'ok',
    buttonProps: okButtonProps,
    prefixCls: `${rootPrefixCls}-btn`
  }, okTextLocale);
};
export default ConfirmOkBtn;