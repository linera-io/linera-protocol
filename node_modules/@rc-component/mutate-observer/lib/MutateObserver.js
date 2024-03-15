"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = _interopRequireDefault(require("react"));
var _useLayoutEffect = _interopRequireDefault(require("rc-util/lib/hooks/useLayoutEffect"));
var _ref = require("rc-util/lib/ref");
var _findDOMNode = _interopRequireDefault(require("rc-util/lib/Dom/findDOMNode"));
var _useEvent = _interopRequireDefault(require("rc-util/lib/hooks/useEvent"));
var _wrapper = _interopRequireDefault(require("./wrapper"));
var _useMutateObserver = _interopRequireDefault(require("./useMutateObserver"));
var MutateObserver = function MutateObserver(props) {
  var children = props.children,
    options = props.options,
    _props$onMutate = props.onMutate,
    onMutate = _props$onMutate === void 0 ? function () {} : _props$onMutate;
  var callback = (0, _useEvent.default)(onMutate);
  var wrapperRef = _react.default.useRef(null);
  var elementRef = _react.default.useRef(null);
  var canRef = /*#__PURE__*/_react.default.isValidElement(children) && (0, _ref.supportRef)(children);
  var mergedRef = (0, _ref.useComposeRef)(elementRef, canRef ? children.ref : null);
  var _React$useState = _react.default.useState(null),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    target = _React$useState2[0],
    setTarget = _React$useState2[1];
  (0, _useMutateObserver.default)(target, callback, options);

  // =========================== Effect ===========================
  // Bind target
  (0, _useLayoutEffect.default)(function () {
    setTarget((0, _findDOMNode.default)(elementRef.current) || (0, _findDOMNode.default)(wrapperRef.current));
  });

  // =========================== Render ===========================
  if (!children) {
    if (process.env.NODE_ENV !== 'production') {
      console.error('MutationObserver need children props');
    }
    return null;
  }
  return /*#__PURE__*/_react.default.createElement(_wrapper.default, {
    ref: wrapperRef
  }, canRef ? /*#__PURE__*/_react.default.cloneElement(children, {
    ref: mergedRef
  }) : children);
};
var _default = MutateObserver;
exports.default = _default;