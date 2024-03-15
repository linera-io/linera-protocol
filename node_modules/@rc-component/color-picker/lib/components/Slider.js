"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _classnames = _interopRequireDefault(require("classnames"));
var _react = _interopRequireWildcard(require("react"));
var _useColorDrag3 = _interopRequireDefault(require("../hooks/useColorDrag"));
var _util = require("../util");
var _Palette = _interopRequireDefault(require("./Palette"));
var _rcUtil = require("rc-util");
var _Gradient = _interopRequireDefault(require("./Gradient"));
var _Handler = _interopRequireDefault(require("./Handler"));
var _Transform = _interopRequireDefault(require("./Transform"));
var Slider = function Slider(_ref) {
  var gradientColors = _ref.gradientColors,
    direction = _ref.direction,
    _ref$type = _ref.type,
    type = _ref$type === void 0 ? 'hue' : _ref$type,
    color = _ref.color,
    value = _ref.value,
    onChange = _ref.onChange,
    onChangeComplete = _ref.onChangeComplete,
    disabled = _ref.disabled,
    prefixCls = _ref.prefixCls;
  var sliderRef = (0, _react.useRef)();
  var transformRef = (0, _react.useRef)();
  var colorRef = (0, _react.useRef)(color);
  var onDragChange = (0, _rcUtil.useEvent)(function (offsetValue) {
    var calcColor = (0, _util.calculateColor)({
      offset: offsetValue,
      targetRef: transformRef,
      containerRef: sliderRef,
      color: color,
      type: type
    });
    colorRef.current = calcColor;
    onChange(calcColor);
  });
  var _useColorDrag = (0, _useColorDrag3.default)({
      color: color,
      targetRef: transformRef,
      containerRef: sliderRef,
      calculate: function calculate(containerRef) {
        return (0, _util.calculateOffset)(containerRef, transformRef, color, type);
      },
      onDragChange: onDragChange,
      onDragChangeComplete: function onDragChangeComplete() {
        onChangeComplete === null || onChangeComplete === void 0 || onChangeComplete(colorRef.current, type);
      },
      direction: 'x',
      disabledDrag: disabled
    }),
    _useColorDrag2 = (0, _slicedToArray2.default)(_useColorDrag, 2),
    offset = _useColorDrag2[0],
    dragStartHandle = _useColorDrag2[1];
  return /*#__PURE__*/_react.default.createElement("div", {
    ref: sliderRef,
    className: (0, _classnames.default)("".concat(prefixCls, "-slider"), "".concat(prefixCls, "-slider-").concat(type)),
    onMouseDown: dragStartHandle,
    onTouchStart: dragStartHandle
  }, /*#__PURE__*/_react.default.createElement(_Palette.default, {
    prefixCls: prefixCls
  }, /*#__PURE__*/_react.default.createElement(_Transform.default, {
    offset: offset,
    ref: transformRef
  }, /*#__PURE__*/_react.default.createElement(_Handler.default, {
    size: "small",
    color: value,
    prefixCls: prefixCls
  })), /*#__PURE__*/_react.default.createElement(_Gradient.default, {
    colors: gradientColors,
    direction: direction,
    type: type,
    prefixCls: prefixCls
  })));
};
var _default = exports.default = Slider;