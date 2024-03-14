"use client";

import DeleteOutlined from "@ant-design/icons/es/icons/DeleteOutlined";
import classNames from 'classnames';
import * as React from 'react';
import TransButton from '../_util/transButton';
import Checkbox from '../checkbox';
import { useLocale } from '../locale';
import defaultLocale from '../locale/en_US';
const ListItem = props => {
  const {
    renderedText,
    renderedEl,
    item,
    checked,
    disabled,
    prefixCls,
    onClick,
    onRemove,
    showRemove
  } = props;
  const className = classNames(`${prefixCls}-content-item`, {
    [`${prefixCls}-content-item-disabled`]: disabled || item.disabled,
    [`${prefixCls}-content-item-checked`]: checked
  });
  let title;
  if (typeof renderedText === 'string' || typeof renderedText === 'number') {
    title = String(renderedText);
  }
  const [contextLocale] = useLocale('Transfer', defaultLocale.Transfer);
  const liProps = {
    className,
    title
  };
  const labelNode = /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-content-item-text`
  }, renderedEl);
  if (showRemove) {
    return /*#__PURE__*/React.createElement("li", Object.assign({}, liProps), labelNode, /*#__PURE__*/React.createElement(TransButton, {
      disabled: disabled || item.disabled,
      className: `${prefixCls}-content-item-remove`,
      "aria-label": contextLocale === null || contextLocale === void 0 ? void 0 : contextLocale.remove,
      onClick: () => {
        onRemove === null || onRemove === void 0 ? void 0 : onRemove(item);
      }
    }, /*#__PURE__*/React.createElement(DeleteOutlined, null)));
  }
  // Default click to select
  liProps.onClick = disabled || item.disabled ? undefined : event => onClick(item, event);
  return /*#__PURE__*/React.createElement("li", Object.assign({}, liProps), /*#__PURE__*/React.createElement(Checkbox, {
    className: `${prefixCls}-checkbox`,
    checked: checked,
    disabled: disabled || item.disabled
  }), labelNode);
};
export default /*#__PURE__*/React.memo(ListItem);