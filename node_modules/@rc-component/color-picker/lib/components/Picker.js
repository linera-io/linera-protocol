"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = _interopRequireWildcard(require("react"));
var _useColorDrag3 = _interopRequireDefault(require("../hooks/useColorDrag"));
var _util = require("../util");
var _rcUtil = require("rc-util");
var _Handler = _interopRequireDefault(require("./Handler"));
var _Palette = _interopRequireDefault(require("./Palette"));
var _Transform = _interopRequireDefault(require("./Transform"));
var Picker = function Picker(_ref) {
  var color = _ref.color,
    onChange = _ref.onChange,
    prefixCls = _ref.prefixCls,
    onChangeComplete = _ref.onChangeComplete,
    disabled = _ref.disabled;
  var pickerRef = (0, _react.useRef)();
  var transformRef = (0, _react.useRef)();
  var colorRef = (0, _react.useRef)(color);
  var onDragChange = (0, _rcUtil.useEvent)(function (offsetValue) {
    var calcColor = (0, _util.calculateColor)({
      offset: offsetValue,
      targetRef: transformRef,
      containerRef: pickerRef,
      color: color
    });
    colorRef.current = calcColor;
    onChange(calcColor);
  });
  var _useColorDrag = (0, _useColorDrag3.default)({
      color: color,
      containerRef: pickerRef,
      targetRef: transformRef,
      calculate: function calculate(containerRef) {
        return (0, _util.calculateOffset)(containerRef, transformRef, color);
      },
      onDragChange: onDragChange,
      onDragChangeComplete: function onDragChangeComplete() {
        return onChangeComplete === null || onChangeComplete === void 0 ? void 0 : onChangeComplete(colorRef.current);
      },
      disabledDrag: disabled
    }),
    _useColorDrag2 = (0, _slicedToArray2.default)(_useColorDrag, 2),
    offset = _useColorDrag2[0],
    dragStartHandle = _useColorDrag2[1];
  return /*#__PURE__*/_react.default.createElement("div", {
    ref: pickerRef,
    className: "".concat(prefixCls, "-select"),
    onMouseDown: dragStartHandle,
    onTouchStart: dragStartHandle
  }, /*#__PURE__*/_react.default.createElement(_Palette.default, {
    prefixCls: prefixCls
  }, /*#__PURE__*/_react.default.createElement(_Transform.default, {
    offset: offset,
    ref: transformRef
  }, /*#__PURE__*/_react.default.createElement(_Handler.default, {
    color: color.toRgbString(),
    prefixCls: prefixCls
  })), /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-saturation"),
    style: {
      backgroundColor: "hsl(".concat(color.toHsb().h, ",100%, 50%)"),
      backgroundImage: 'linear-gradient(0deg, #000, transparent),linear-gradient(90deg, #fff, hsla(0, 0%, 100%, 0))'
    }
  })));
};
var _default = exports.default = Picker;