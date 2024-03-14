import _extends from "@babel/runtime/helpers/esm/extends";
import * as React from 'react';
import toArray from "rc-util/es/Children/toArray";
import { warning } from "rc-util/es/warning";
import SingleObserver from "./SingleObserver";
import { Collection } from "./Collection";
var INTERNAL_PREFIX_KEY = 'rc-observer-key';
import { _rs } from "./utils/observerUtil";
export { /** @private Test only for mock trigger resize event */
_rs };
function ResizeObserver(props, ref) {
  var children = props.children;
  var childNodes = typeof children === 'function' ? [children] : toArray(children);
  if (process.env.NODE_ENV !== 'production') {
    if (childNodes.length > 1) {
      warning(false, 'Find more than one child node with `children` in ResizeObserver. Please use ResizeObserver.Collection instead.');
    } else if (childNodes.length === 0) {
      warning(false, '`children` of ResizeObserver is empty. Nothing is in observe.');
    }
  }
  return childNodes.map(function (child, index) {
    var key = (child === null || child === void 0 ? void 0 : child.key) || "".concat(INTERNAL_PREFIX_KEY, "-").concat(index);
    return /*#__PURE__*/React.createElement(SingleObserver, _extends({}, props, {
      key: key,
      ref: index === 0 ? ref : undefined
    }), child);
  });
}
var RefResizeObserver = /*#__PURE__*/React.forwardRef(ResizeObserver);
if (process.env.NODE_ENV !== 'production') {
  RefResizeObserver.displayName = 'ResizeObserver';
}
RefResizeObserver.Collection = Collection;
export default RefResizeObserver;