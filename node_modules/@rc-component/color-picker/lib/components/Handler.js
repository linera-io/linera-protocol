"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireDefault(require("react"));
var Handler = function Handler(_ref) {
  var _ref$size = _ref.size,
    size = _ref$size === void 0 ? 'default' : _ref$size,
    color = _ref.color,
    prefixCls = _ref.prefixCls;
  return /*#__PURE__*/_react.default.createElement("div", {
    className: (0, _classnames.default)("".concat(prefixCls, "-handler"), (0, _defineProperty2.default)({}, "".concat(prefixCls, "-handler-sm"), size === 'small')),
    style: {
      backgroundColor: color
    }
  });
};
var _default = exports.default = Handler;