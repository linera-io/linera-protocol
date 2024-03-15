"use client";

import React, { useContext } from 'react';
import Button from '../../button';
import { ModalContext } from '../context';
const NormalCancelBtn = () => {
  const {
    cancelButtonProps,
    cancelTextLocale,
    onCancel
  } = useContext(ModalContext);
  return /*#__PURE__*/React.createElement(Button, Object.assign({
    onClick: onCancel
  }, cancelButtonProps), cancelTextLocale);
};
export default NormalCancelBtn;