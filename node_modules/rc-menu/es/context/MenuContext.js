import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
var _excluded = ["children", "locked"];
import * as React from 'react';
import useMemo from "rc-util/es/hooks/useMemo";
import isEqual from "rc-util/es/isEqual";
export var MenuContext = /*#__PURE__*/React.createContext(null);
function mergeProps(origin, target) {
  var clone = _objectSpread({}, origin);
  Object.keys(target).forEach(function (key) {
    var value = target[key];
    if (value !== undefined) {
      clone[key] = value;
    }
  });
  return clone;
}
export default function InheritableContextProvider(_ref) {
  var children = _ref.children,
    locked = _ref.locked,
    restProps = _objectWithoutProperties(_ref, _excluded);
  var context = React.useContext(MenuContext);
  var inheritableContext = useMemo(function () {
    return mergeProps(context, restProps);
  }, [context, restProps], function (prev, next) {
    return !locked && (prev[0] !== next[0] || !isEqual(prev[1], next[1], true));
  });
  return /*#__PURE__*/React.createElement(MenuContext.Provider, {
    value: inheritableContext
  }, children);
}