import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _typeof from "@babel/runtime/helpers/esm/typeof";
import React from 'react';
import classNames from 'classnames';
import Portal from '@rc-component/portal';
import useId from "rc-util/es/hooks/useId";
var COVER_PROPS = {
  fill: 'transparent',
  pointerEvents: 'auto'
};
var Mask = function Mask(props) {
  var prefixCls = props.prefixCls,
    rootClassName = props.rootClassName,
    pos = props.pos,
    showMask = props.showMask,
    _props$style = props.style,
    style = _props$style === void 0 ? {} : _props$style,
    _props$fill = props.fill,
    fill = _props$fill === void 0 ? "rgba(0,0,0,0.5)" : _props$fill,
    open = props.open,
    animated = props.animated,
    zIndex = props.zIndex,
    disabledInteraction = props.disabledInteraction;
  var id = useId();
  var maskId = "".concat(prefixCls, "-mask-").concat(id);
  var mergedAnimated = _typeof(animated) === 'object' ? animated === null || animated === void 0 ? void 0 : animated.placeholder : animated;
  var isSafari = typeof navigator !== 'undefined' && /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
  var maskRectSize = isSafari ? {
    width: '100%',
    height: '100%'
  } : {
    width: '100vw',
    height: '100vh'
  };
  return /*#__PURE__*/React.createElement(Portal, {
    open: open,
    autoLock: true
  }, /*#__PURE__*/React.createElement("div", {
    className: classNames("".concat(prefixCls, "-mask"), rootClassName),
    style: _objectSpread({
      position: 'fixed',
      left: 0,
      right: 0,
      top: 0,
      bottom: 0,
      zIndex: zIndex,
      pointerEvents: pos && !disabledInteraction ? 'none' : 'auto'
    }, style)
  }, showMask ? /*#__PURE__*/React.createElement("svg", {
    style: {
      width: '100%',
      height: '100%'
    }
  }, /*#__PURE__*/React.createElement("defs", null, /*#__PURE__*/React.createElement("mask", {
    id: maskId
  }, /*#__PURE__*/React.createElement("rect", _extends({
    x: "0",
    y: "0"
  }, maskRectSize, {
    fill: "white"
  })), pos && /*#__PURE__*/React.createElement("rect", {
    x: pos.left,
    y: pos.top,
    rx: pos.radius,
    width: pos.width,
    height: pos.height,
    fill: "black",
    className: mergedAnimated ? "".concat(prefixCls, "-placeholder-animated") : ''
  }))), /*#__PURE__*/React.createElement("rect", {
    x: "0",
    y: "0",
    width: "100%",
    height: "100%",
    fill: fill,
    mask: "url(#".concat(maskId, ")")
  }), pos && /*#__PURE__*/React.createElement(React.Fragment, null, /*#__PURE__*/React.createElement("rect", _extends({}, COVER_PROPS, {
    x: "0",
    y: "0",
    width: "100%",
    height: pos.top
  })), /*#__PURE__*/React.createElement("rect", _extends({}, COVER_PROPS, {
    x: "0",
    y: "0",
    width: pos.left,
    height: "100%"
  })), /*#__PURE__*/React.createElement("rect", _extends({}, COVER_PROPS, {
    x: "0",
    y: pos.top + pos.height,
    width: "100%",
    height: "calc(100vh - ".concat(pos.top + pos.height, "px)")
  })), /*#__PURE__*/React.createElement("rect", _extends({}, COVER_PROPS, {
    x: pos.left + pos.width,
    y: "0",
    width: "calc(100vw - ".concat(pos.left + pos.width, "px)"),
    height: "100%"
  })))) : null));
};
export default Mask;