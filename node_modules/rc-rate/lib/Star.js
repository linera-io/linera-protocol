"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireDefault(require("react"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var _classnames = _interopRequireDefault(require("classnames"));
function Star(props, ref) {
  var disabled = props.disabled,
    prefixCls = props.prefixCls,
    character = props.character,
    characterRender = props.characterRender,
    index = props.index,
    count = props.count,
    value = props.value,
    allowHalf = props.allowHalf,
    focused = props.focused,
    onHover = props.onHover,
    onClick = props.onClick;
  // =========================== Events ===========================
  var onInternalHover = function onInternalHover(e) {
    onHover(e, index);
  };
  var onInternalClick = function onInternalClick(e) {
    onClick(e, index);
  };
  var onInternalKeyDown = function onInternalKeyDown(e) {
    if (e.keyCode === _KeyCode.default.ENTER) {
      onClick(e, index);
    }
  };
  // =========================== Render ===========================
  // >>>>> ClassName
  var starValue = index + 1;
  var classNameList = new Set([prefixCls]);
  // TODO: Current we just refactor from CC to FC. This logic seems can be optimized.
  if (value === 0 && index === 0 && focused) {
    classNameList.add("".concat(prefixCls, "-focused"));
  } else if (allowHalf && value + 0.5 >= starValue && value < starValue) {
    classNameList.add("".concat(prefixCls, "-half"));
    classNameList.add("".concat(prefixCls, "-active"));
    if (focused) {
      classNameList.add("".concat(prefixCls, "-focused"));
    }
  } else {
    if (starValue <= value) {
      classNameList.add("".concat(prefixCls, "-full"));
    } else {
      classNameList.add("".concat(prefixCls, "-zero"));
    }
    if (starValue === value && focused) {
      classNameList.add("".concat(prefixCls, "-focused"));
    }
  }
  // >>>>> Node
  var characterNode = typeof character === 'function' ? character(props) : character;
  var start = /*#__PURE__*/_react.default.createElement("li", {
    className: (0, _classnames.default)(Array.from(classNameList)),
    ref: ref
  }, /*#__PURE__*/_react.default.createElement("div", {
    onClick: disabled ? null : onInternalClick,
    onKeyDown: disabled ? null : onInternalKeyDown,
    onMouseMove: disabled ? null : onInternalHover,
    role: "radio",
    "aria-checked": value > index ? 'true' : 'false',
    "aria-posinset": index + 1,
    "aria-setsize": count,
    tabIndex: disabled ? -1 : 0
  }, /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-first")
  }, characterNode), /*#__PURE__*/_react.default.createElement("div", {
    className: "".concat(prefixCls, "-second")
  }, characterNode)));
  if (characterRender) {
    start = characterRender(start, props);
  }
  return start;
}
var _default = /*#__PURE__*/_react.default.forwardRef(Star);
exports.default = _default;