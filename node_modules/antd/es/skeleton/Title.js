"use client";

/* eslint-disable jsx-a11y/heading-has-content */
import classNames from 'classnames';
import * as React from 'react';
const Title = _ref => {
  let {
    prefixCls,
    className,
    width,
    style
  } = _ref;
  return /*#__PURE__*/React.createElement("h3", {
    className: classNames(prefixCls, className),
    style: Object.assign({
      width
    }, style)
  });
};
export default Title;