import React from 'react';
import KeyCode from "rc-util/es/KeyCode";
import classNames from 'classnames';
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
    if (e.keyCode === KeyCode.ENTER) {
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
  var start = /*#__PURE__*/React.createElement("li", {
    className: classNames(Array.from(classNameList)),
    ref: ref
  }, /*#__PURE__*/React.createElement("div", {
    onClick: disabled ? null : onInternalClick,
    onKeyDown: disabled ? null : onInternalKeyDown,
    onMouseMove: disabled ? null : onInternalHover,
    role: "radio",
    "aria-checked": value > index ? 'true' : 'false',
    "aria-posinset": index + 1,
    "aria-setsize": count,
    tabIndex: disabled ? -1 : 0
  }, /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-first")
  }, characterNode), /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-second")
  }, characterNode)));
  if (characterRender) {
    start = characterRender(start, props);
  }
  return start;
}
export default /*#__PURE__*/React.forwardRef(Star);