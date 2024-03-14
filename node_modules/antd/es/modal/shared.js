"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import React from 'react';
import CloseOutlined from "@ant-design/icons/es/icons/CloseOutlined";
import { DisabledContextProvider } from '../config-provider/DisabledContext';
import { useLocale } from '../locale';
import NormalCancelBtn from './components/NormalCancelBtn';
import NormalOkBtn from './components/NormalOkBtn';
import { ModalContextProvider } from './context';
import { getConfirmLocale } from './locale';
export function renderCloseIcon(prefixCls, closeIcon) {
  return /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-close-x`
  }, closeIcon || /*#__PURE__*/React.createElement(CloseOutlined, {
    className: `${prefixCls}-close-icon`
  }));
}
export const Footer = props => {
  const {
    okText,
    okType = 'primary',
    cancelText,
    confirmLoading,
    onOk,
    onCancel,
    okButtonProps,
    cancelButtonProps,
    footer
  } = props;
  const [locale] = useLocale('Modal', getConfirmLocale());
  // ================== Locale Text ==================
  const okTextLocale = okText || (locale === null || locale === void 0 ? void 0 : locale.okText);
  const cancelTextLocale = cancelText || (locale === null || locale === void 0 ? void 0 : locale.cancelText);
  // ================= Context Value =================
  const btnCtxValue = {
    confirmLoading,
    okButtonProps,
    cancelButtonProps,
    okTextLocale,
    cancelTextLocale,
    okType,
    onOk,
    onCancel
  };
  const btnCtxValueMemo = React.useMemo(() => btnCtxValue, _toConsumableArray(Object.values(btnCtxValue)));
  let footerNode;
  if (typeof footer === 'function' || typeof footer === 'undefined') {
    footerNode = /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement(NormalCancelBtn, null), /*#__PURE__*/React.createElement(NormalOkBtn, null));
    if (typeof footer === 'function') {
      footerNode = footer(footerNode, {
        OkBtn: NormalOkBtn,
        CancelBtn: NormalCancelBtn
      });
    }
    footerNode = /*#__PURE__*/React.createElement(ModalContextProvider, {
      value: btnCtxValueMemo
    }, footerNode);
  } else {
    footerNode = footer;
  }
  return /*#__PURE__*/React.createElement(DisabledContextProvider, {
    disabled: false
  }, footerNode);
};