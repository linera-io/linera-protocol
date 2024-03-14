import _extends from "@babel/runtime/helpers/esm/extends";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import * as React from 'react';
import ResizeObserver from 'rc-resize-observer';
import classNames from 'classnames';
/**
 * Fill component to provided the scroll content real height.
 */
var Filler = /*#__PURE__*/React.forwardRef(function (_ref, ref) {
  var height = _ref.height,
    offsetY = _ref.offsetY,
    offsetX = _ref.offsetX,
    children = _ref.children,
    prefixCls = _ref.prefixCls,
    onInnerResize = _ref.onInnerResize,
    innerProps = _ref.innerProps,
    rtl = _ref.rtl,
    extra = _ref.extra;
  var outerStyle = {};
  var innerStyle = {
    display: 'flex',
    flexDirection: 'column'
  };
  if (offsetY !== undefined) {
    // Not set `width` since this will break `sticky: right`
    outerStyle = {
      height: height,
      position: 'relative',
      overflow: 'hidden'
    };
    innerStyle = _objectSpread(_objectSpread({}, innerStyle), {}, _defineProperty(_defineProperty(_defineProperty(_defineProperty(_defineProperty({
      transform: "translateY(".concat(offsetY, "px)")
    }, rtl ? 'marginRight' : 'marginLeft', -offsetX), "position", 'absolute'), "left", 0), "right", 0), "top", 0));
  }
  return /*#__PURE__*/React.createElement("div", {
    style: outerStyle
  }, /*#__PURE__*/React.createElement(ResizeObserver, {
    onResize: function onResize(_ref2) {
      var offsetHeight = _ref2.offsetHeight;
      if (offsetHeight && onInnerResize) {
        onInnerResize();
      }
    }
  }, /*#__PURE__*/React.createElement("div", _extends({
    style: innerStyle,
    className: classNames(_defineProperty({}, "".concat(prefixCls, "-holder-inner"), prefixCls)),
    ref: ref
  }, innerProps), children, extra)));
});
Filler.displayName = 'Filler';
export default Filler;