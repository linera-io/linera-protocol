import * as React from 'react';
import Mark from "./Mark";
export default function Marks(props) {
  var prefixCls = props.prefixCls,
    marks = props.marks,
    onClick = props.onClick;
  var markPrefixCls = "".concat(prefixCls, "-mark");

  // Not render mark if empty
  if (!marks.length) {
    return null;
  }
  return /*#__PURE__*/React.createElement("div", {
    className: markPrefixCls
  }, marks.map(function (_ref) {
    var value = _ref.value,
      style = _ref.style,
      label = _ref.label;
    return /*#__PURE__*/React.createElement(Mark, {
      key: value,
      prefixCls: markPrefixCls,
      style: style,
      value: value,
      onClick: onClick
    }, label);
  }));
}