import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import * as React from 'react';
import { generateTrigger } from "./index";
var MockPortal = function MockPortal(_ref) {
  var open = _ref.open,
    autoDestroy = _ref.autoDestroy,
    children = _ref.children,
    getContainer = _ref.getContainer;
  var _React$useState = React.useState(open),
    _React$useState2 = _slicedToArray(_React$useState, 2),
    visible = _React$useState2[0],
    setVisible = _React$useState2[1];
  React.useEffect(function () {
    getContainer === null || getContainer === void 0 || getContainer();
  });
  React.useEffect(function () {
    if (open) {
      setVisible(true);
    } else if (autoDestroy) {
      setVisible(false);
    }
  }, [open, autoDestroy]);
  return visible ? children : null;
};
export default generateTrigger(MockPortal);