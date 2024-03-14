import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import * as React from 'react';
import classNames from 'classnames';
import CascaderContext from "../context";
export default function Checkbox(_ref) {
  var _classNames;
  var prefixCls = _ref.prefixCls,
    checked = _ref.checked,
    halfChecked = _ref.halfChecked,
    disabled = _ref.disabled,
    onClick = _ref.onClick,
    disableCheckbox = _ref.disableCheckbox;
  var _React$useContext = React.useContext(CascaderContext),
    checkable = _React$useContext.checkable;
  var customCheckbox = typeof checkable !== 'boolean' ? checkable : null;
  return /*#__PURE__*/React.createElement("span", {
    className: classNames("".concat(prefixCls), (_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-checked"), checked), _defineProperty(_classNames, "".concat(prefixCls, "-indeterminate"), !checked && halfChecked), _defineProperty(_classNames, "".concat(prefixCls, "-disabled"), disabled || disableCheckbox), _classNames)),
    onClick: onClick
  }, customCheckbox);
}