"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import classNames from 'classnames';
import { getRenderPropValue } from '../_util/getRenderPropValue';
import { getTransitionName } from '../_util/motion';
import { ConfigContext } from '../config-provider';
import Tooltip from '../tooltip';
import PurePanel from './PurePanel';
// CSSINJS
import useStyle from './style';
const Overlay = _ref => {
  let {
    title,
    content,
    prefixCls
  } = _ref;
  return /*#__PURE__*/React.createElement(React.Fragment, null, title && /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-title`
  }, getRenderPropValue(title)), /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-inner-content`
  }, getRenderPropValue(content)));
};
const Popover = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      title,
      content,
      overlayClassName,
      placement = 'top',
      trigger = 'hover',
      mouseEnterDelay = 0.1,
      mouseLeaveDelay = 0.1,
      overlayStyle = {}
    } = props,
    otherProps = __rest(props, ["prefixCls", "title", "content", "overlayClassName", "placement", "trigger", "mouseEnterDelay", "mouseLeaveDelay", "overlayStyle"]);
  const {
    getPrefixCls
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('popover', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const rootPrefixCls = getPrefixCls();
  const overlayCls = classNames(overlayClassName, hashId, cssVarCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement(Tooltip, Object.assign({
    placement: placement,
    trigger: trigger,
    mouseEnterDelay: mouseEnterDelay,
    mouseLeaveDelay: mouseLeaveDelay,
    overlayStyle: overlayStyle
  }, otherProps, {
    prefixCls: prefixCls,
    overlayClassName: overlayCls,
    ref: ref,
    overlay: title || content ? /*#__PURE__*/React.createElement(Overlay, {
      prefixCls: prefixCls,
      title: title,
      content: content
    }) : null,
    transitionName: getTransitionName(rootPrefixCls, 'zoom-big', otherProps.transitionName),
    "data-popover-inject": true
  })));
});
if (process.env.NODE_ENV !== 'production') {
  Popover.displayName = 'Popover';
}
Popover._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
export default Popover;