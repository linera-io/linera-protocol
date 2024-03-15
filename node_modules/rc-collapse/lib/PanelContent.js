"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames2 = _interopRequireDefault(require("classnames"));
var _react = _interopRequireDefault(require("react"));
var PanelContent = /*#__PURE__*/_react.default.forwardRef(function (props, ref) {
  var _classnames;
  var prefixCls = props.prefixCls,
    forceRender = props.forceRender,
    className = props.className,
    style = props.style,
    children = props.children,
    isActive = props.isActive,
    role = props.role;
  var _React$useState = _react.default.useState(isActive || forceRender),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    rendered = _React$useState2[0],
    setRendered = _React$useState2[1];
  _react.default.useEffect(function () {
    if (forceRender || isActive) {
      setRendered(true);
    }
  }, [forceRender, isActive]);
  if (!rendered) {
    return null;
  }
  return /*#__PURE__*/_react.default.createElement("div", {
    ref: ref,
    className: (0, _classnames2.default)("".concat(prefixCls, "-content"), (_classnames = {}, (0, _defineProperty2.default)(_classnames, "".concat(prefixCls, "-content-active"), isActive), (0, _defineProperty2.default)(_classnames, "".concat(prefixCls, "-content-inactive"), !isActive), _classnames), className),
    style: style,
    role: role
  }, /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-content-box")
  }, children));
});
PanelContent.displayName = 'PanelContent';
var _default = exports.default = PanelContent;