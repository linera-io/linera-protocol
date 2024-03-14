"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "_rs", {
  enumerable: true,
  get: function get() {
    return _observerUtil._rs;
  }
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var React = _interopRequireWildcard(require("react"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _warning = require("rc-util/lib/warning");
var _SingleObserver = _interopRequireDefault(require("./SingleObserver"));
var _Collection = require("./Collection");
var _observerUtil = require("./utils/observerUtil");
var INTERNAL_PREFIX_KEY = 'rc-observer-key';
function ResizeObserver(props, ref) {
  var children = props.children;
  var childNodes = typeof children === 'function' ? [children] : (0, _toArray.default)(children);
  if (process.env.NODE_ENV !== 'production') {
    if (childNodes.length > 1) {
      (0, _warning.warning)(false, 'Find more than one child node with `children` in ResizeObserver. Please use ResizeObserver.Collection instead.');
    } else if (childNodes.length === 0) {
      (0, _warning.warning)(false, '`children` of ResizeObserver is empty. Nothing is in observe.');
    }
  }
  return childNodes.map(function (child, index) {
    var key = (child === null || child === void 0 ? void 0 : child.key) || "".concat(INTERNAL_PREFIX_KEY, "-").concat(index);
    return /*#__PURE__*/React.createElement(_SingleObserver.default, (0, _extends2.default)({}, props, {
      key: key,
      ref: index === 0 ? ref : undefined
    }), child);
  });
}
var RefResizeObserver = /*#__PURE__*/React.forwardRef(ResizeObserver);
if (process.env.NODE_ENV !== 'production') {
  RefResizeObserver.displayName = 'ResizeObserver';
}
RefResizeObserver.Collection = _Collection.Collection;
var _default = exports.default = RefResizeObserver;