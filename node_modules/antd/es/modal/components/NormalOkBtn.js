"use client";

import React, { useContext } from 'react';
import Button from '../../button';
import { convertLegacyProps } from '../../button/buttonHelpers';
import { ModalContext } from '../context';
const NormalOkBtn = () => {
  const {
    confirmLoading,
    okButtonProps,
    okType,
    okTextLocale,
    onOk
  } = useContext(ModalContext);
  return /*#__PURE__*/React.createElement(Button, Object.assign({}, convertLegacyProps(okType), {
    loading: confirmLoading,
    onClick: onOk
  }, okButtonProps), okTextLocale);
};
export default NormalOkBtn;