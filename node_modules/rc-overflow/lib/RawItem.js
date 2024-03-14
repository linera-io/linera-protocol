"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _Item = _interopRequireDefault(require("./Item"));
var _context = require("./context");
var _excluded = ["component"],
  _excluded2 = ["className"],
  _excluded3 = ["className"];
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || _typeof(obj) !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
var InternalRawItem = function InternalRawItem(props, ref) {
  var context = React.useContext(_context.OverflowContext);
  // Render directly when context not provided
  if (!context) {
    var _props$component = props.component,
      Component = _props$component === void 0 ? 'div' : _props$component,
      _restProps = (0, _objectWithoutProperties2.default)(props, _excluded);
    return /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({}, _restProps, {
      ref: ref
    }));
  }
  var contextClassName = context.className,
    restContext = (0, _objectWithoutProperties2.default)(context, _excluded2);
  var className = props.className,
    restProps = (0, _objectWithoutProperties2.default)(props, _excluded3);
  // Do not pass context to sub item to avoid multiple measure
  return /*#__PURE__*/React.createElement(_context.OverflowContext.Provider, {
    value: null
  }, /*#__PURE__*/React.createElement(_Item.default, (0, _extends2.default)({
    ref: ref,
    className: (0, _classnames.default)(contextClassName, className)
  }, restContext, restProps)));
};
var RawItem = /*#__PURE__*/React.forwardRef(InternalRawItem);
RawItem.displayName = 'RawItem';
var _default = RawItem;
exports.default = _default;