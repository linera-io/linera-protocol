"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));
var _createSuper2 = _interopRequireDefault(require("@babel/runtime/helpers/createSuper"));
var React = _interopRequireWildcard(require("react"));
/**
 * Fallback to findDOMNode if origin ref do not provide any dom element
 */
var DomWrapper = exports.default = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(DomWrapper, _React$Component);
  var _super = (0, _createSuper2.default)(DomWrapper);
  function DomWrapper() {
    (0, _classCallCheck2.default)(this, DomWrapper);
    return _super.apply(this, arguments);
  }
  (0, _createClass2.default)(DomWrapper, [{
    key: "render",
    value: function render() {
      return this.props.children;
    }
  }]);
  return DomWrapper;
}(React.Component);