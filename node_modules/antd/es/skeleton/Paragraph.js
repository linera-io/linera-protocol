"use client";

import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import classNames from 'classnames';
import * as React from 'react';
const Paragraph = props => {
  const getWidth = index => {
    const {
      width,
      rows = 2
    } = props;
    if (Array.isArray(width)) {
      return width[index];
    }
    // last paragraph
    if (rows - 1 === index) {
      return width;
    }
    return undefined;
  };
  const {
    prefixCls,
    className,
    style,
    rows
  } = props;
  const rowList = _toConsumableArray(Array(rows)).map((_, index) => (
  /*#__PURE__*/
  // eslint-disable-next-line react/no-array-index-key
  React.createElement("li", {
    key: index,
    style: {
      width: getWidth(index)
    }
  })));
  return /*#__PURE__*/React.createElement("ul", {
    className: classNames(prefixCls, className),
    style: style
  }, rowList);
};
export default Paragraph;