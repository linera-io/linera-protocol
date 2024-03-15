"use client";

import * as React from 'react';
import classNames from 'classnames';
import RcCollapse from 'rc-collapse';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
const CollapsePanel = /*#__PURE__*/React.forwardRef((props, ref) => {
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Collapse.Panel');
    warning.deprecated(!('disabled' in props), 'disabled', 'collapsible="disabled"');
  }
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const {
    prefixCls: customizePrefixCls,
    className,
    showArrow = true
  } = props;
  const prefixCls = getPrefixCls('collapse', customizePrefixCls);
  const collapsePanelClassName = classNames({
    [`${prefixCls}-no-arrow`]: !showArrow
  }, className);
  return /*#__PURE__*/React.createElement(RcCollapse.Panel, Object.assign({
    ref: ref
  }, props, {
    prefixCls: prefixCls,
    className: collapsePanelClassName
  }));
});
export default CollapsePanel;