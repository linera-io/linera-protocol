import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["icon", "type"],
  _excluded2 = ["onClear"];
import * as React from 'react';
import PickerContext from "../context";
export default function Icon(props) {
  var icon = props.icon,
    type = props.type,
    restProps = _objectWithoutProperties(props, _excluded);
  var _React$useContext = React.useContext(PickerContext),
    prefixCls = _React$useContext.prefixCls;
  return icon ? /*#__PURE__*/React.createElement("span", _extends({
    className: "".concat(prefixCls, "-").concat(type)
  }, restProps), icon) : null;
}
export function ClearIcon(_ref) {
  var onClear = _ref.onClear,
    restProps = _objectWithoutProperties(_ref, _excluded2);
  return /*#__PURE__*/React.createElement(Icon, _extends({}, restProps, {
    type: "clear",
    role: "button",
    onMouseDown: function onMouseDown(e) {
      e.preventDefault();
    },
    onClick: function onClick(e) {
      e.stopPropagation();
      onClear();
    }
  }));
}