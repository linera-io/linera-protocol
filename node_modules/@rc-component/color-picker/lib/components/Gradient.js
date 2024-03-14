"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _util = require("../util");
var Gradient = function Gradient(_ref) {
  var colors = _ref.colors,
    children = _ref.children,
    _ref$direction = _ref.direction,
    direction = _ref$direction === void 0 ? 'to right' : _ref$direction,
    type = _ref.type,
    prefixCls = _ref.prefixCls;
  var gradientColors = (0, _react.useMemo)(function () {
    return colors.map(function (color, idx) {
      var result = (0, _util.generateColor)(color);
      if (type === 'alpha' && idx === colors.length - 1) {
        result.setAlpha(1);
      }
      return result.toRgbString();
    }).join(',');
  }, [colors, type]);
  return /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-gradient"),
    style: {
      position: 'absolute',
      inset: 0,
      background: "linear-gradient(".concat(direction, ", ").concat(gradientColors, ")")
    }
  }, children);
};
var _default = exports.default = Gradient;