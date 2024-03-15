"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _ref = require("rc-util/lib/ref");
var Overlay = /*#__PURE__*/(0, _react.forwardRef)(function (props, ref) {
  var overlay = props.overlay,
    arrow = props.arrow,
    prefixCls = props.prefixCls;
  var overlayNode = (0, _react.useMemo)(function () {
    var overlayElement;
    if (typeof overlay === 'function') {
      overlayElement = overlay();
    } else {
      overlayElement = overlay;
    }
    return overlayElement;
  }, [overlay]);
  var composedRef = (0, _ref.composeRef)(ref, overlayNode === null || overlayNode === void 0 ? void 0 : overlayNode.ref);
  return /*#__PURE__*/_react.default.createElement(_react.default.Fragment, null, arrow && /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-arrow")
  }), /*#__PURE__*/_react.default.cloneElement(overlayNode, {
    ref: (0, _ref.supportRef)(overlayNode) ? composedRef : undefined
  }));
});
var _default = exports.default = Overlay;