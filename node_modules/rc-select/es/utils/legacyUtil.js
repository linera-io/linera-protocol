import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["children", "value"],
  _excluded2 = ["children"];
import * as React from 'react';
import toArray from "rc-util/es/Children/toArray";
function convertNodeToOption(node) {
  var _ref = node,
    key = _ref.key,
    _ref$props = _ref.props,
    children = _ref$props.children,
    value = _ref$props.value,
    restProps = _objectWithoutProperties(_ref$props, _excluded);
  return _objectSpread({
    key: key,
    value: value !== undefined ? value : key,
    children: children
  }, restProps);
}
export function convertChildrenToData(nodes) {
  var optionOnly = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  return toArray(nodes).map(function (node, index) {
    if (! /*#__PURE__*/React.isValidElement(node) || !node.type) {
      return null;
    }
    var _ref2 = node,
      isSelectOptGroup = _ref2.type.isSelectOptGroup,
      key = _ref2.key,
      _ref2$props = _ref2.props,
      children = _ref2$props.children,
      restProps = _objectWithoutProperties(_ref2$props, _excluded2);
    if (optionOnly || !isSelectOptGroup) {
      return convertNodeToOption(node);
    }
    return _objectSpread(_objectSpread({
      key: "__RC_SELECT_GRP__".concat(key === null ? index : key, "__"),
      label: key
    }, restProps), {}, {
      options: convertChildrenToData(children)
    });
  }).filter(function (data) {
    return data;
  });
}