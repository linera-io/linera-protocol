import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import * as React from 'react';
import classNames from 'classnames';
export function renderExpandIcon(_ref) {
  var _classNames;
  var prefixCls = _ref.prefixCls,
    record = _ref.record,
    onExpand = _ref.onExpand,
    expanded = _ref.expanded,
    expandable = _ref.expandable;
  var expandClassName = "".concat(prefixCls, "-row-expand-icon");
  if (!expandable) {
    return /*#__PURE__*/React.createElement("span", {
      className: classNames(expandClassName, "".concat(prefixCls, "-row-spaced"))
    });
  }
  var onClick = function onClick(event) {
    onExpand(record, event);
    event.stopPropagation();
  };
  return /*#__PURE__*/React.createElement("span", {
    className: classNames(expandClassName, (_classNames = {}, _defineProperty(_classNames, "".concat(prefixCls, "-row-expanded"), expanded), _defineProperty(_classNames, "".concat(prefixCls, "-row-collapsed"), !expanded), _classNames)),
    onClick: onClick
  });
}
export function findAllChildrenKeys(data, getRowKey, childrenColumnName) {
  var keys = [];
  function dig(list) {
    (list || []).forEach(function (item, index) {
      keys.push(getRowKey(item, index));
      dig(item[childrenColumnName]);
    });
  }
  dig(data);
  return keys;
}