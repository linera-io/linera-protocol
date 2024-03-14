"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _warning = require("rc-util/lib/warning");
var uuid = 0;

/**
 * Theme with algorithms to derive tokens from design tokens.
 * Use `createTheme` first which will help to manage the theme instance cache.
 */
var Theme = exports.default = /*#__PURE__*/function () {
  function Theme(derivatives) {
    (0, _classCallCheck2.default)(this, Theme);
    (0, _defineProperty2.default)(this, "derivatives", void 0);
    (0, _defineProperty2.default)(this, "id", void 0);
    this.derivatives = Array.isArray(derivatives) ? derivatives : [derivatives];
    this.id = uuid;
    if (derivatives.length === 0) {
      (0, _warning.warning)(derivatives.length > 0, '[Ant Design CSS-in-JS] Theme should have at least one derivative function.');
    }
    uuid += 1;
  }
  (0, _createClass2.default)(Theme, [{
    key: "getDerivativeToken",
    value: function getDerivativeToken(token) {
      return this.derivatives.reduce(function (result, derivative) {
        return derivative(token, result);
      }, undefined);
    }
  }]);
  return Theme;
}();