"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getRoundNumber = exports.Color = void 0;
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));
var _createSuper2 = _interopRequireDefault(require("@babel/runtime/helpers/createSuper"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));
var _tinycolor = require("@ctrl/tinycolor");
var _excluded = ["b"],
  _excluded2 = ["v"];
var getRoundNumber = exports.getRoundNumber = function getRoundNumber(value) {
  return Math.round(Number(value || 0));
};
var convertHsb2Hsv = function convertHsb2Hsv(color) {
  if (color && (0, _typeof2.default)(color) === 'object' && 'h' in color && 'b' in color) {
    var _ref = color,
      b = _ref.b,
      resets = (0, _objectWithoutProperties2.default)(_ref, _excluded);
    return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, resets), {}, {
      v: b
    });
  }
  if (typeof color === 'string' && /hsb/.test(color)) {
    return color.replace(/hsb/, 'hsv');
  }
  return color;
};
var Color = exports.Color = /*#__PURE__*/function (_TinyColor) {
  (0, _inherits2.default)(Color, _TinyColor);
  var _super = (0, _createSuper2.default)(Color);
  function Color(color) {
    (0, _classCallCheck2.default)(this, Color);
    return _super.call(this, convertHsb2Hsv(color));
  }
  (0, _createClass2.default)(Color, [{
    key: "toHsbString",
    value: function toHsbString() {
      var hsb = this.toHsb();
      var saturation = getRoundNumber(hsb.s * 100);
      var lightness = getRoundNumber(hsb.b * 100);
      var hue = getRoundNumber(hsb.h);
      var alpha = hsb.a;
      var hsbString = "hsb(".concat(hue, ", ").concat(saturation, "%, ").concat(lightness, "%)");
      var hsbaString = "hsba(".concat(hue, ", ").concat(saturation, "%, ").concat(lightness, "%, ").concat(alpha.toFixed(alpha === 0 ? 0 : 2), ")");
      return alpha === 1 ? hsbString : hsbaString;
    }
  }, {
    key: "toHsb",
    value: function toHsb() {
      var hsv = this.toHsv();
      if ((0, _typeof2.default)(this.originalInput) === 'object' && this.originalInput) {
        if ('h' in this.originalInput) {
          hsv = this.originalInput;
        }
      }
      var _hsv = hsv,
        v = _hsv.v,
        resets = (0, _objectWithoutProperties2.default)(_hsv, _excluded2);
      return (0, _objectSpread2.default)((0, _objectSpread2.default)({}, resets), {}, {
        b: hsv.v,
        a: this.a
      });
    }
  }]);
  return Color;
}(_tinycolor.TinyColor);