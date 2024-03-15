"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireDefault(require("react"));
var ColorBlock = function ColorBlock(_ref) {
  var color = _ref.color,
    prefixCls = _ref.prefixCls,
    className = _ref.className,
    style = _ref.style,
    onClick = _ref.onClick;
  var colorBlockCls = "".concat(prefixCls, "-color-block");
  return /*#__PURE__*/_react.default.createElement("div", {
    className: (0, _classnames.default)(colorBlockCls, className),
    style: style,
    onClick: onClick
  }, /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(colorBlockCls, "-inner"),
    style: {
      background: color
    }
  }));
};
var _default = exports.default = ColorBlock;